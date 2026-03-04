from mcp.server.fastmcp import FastMCP

mcp = FastMCP(
    "OracleSQL_Agent",  # Name of the MCP server
    instructions="You are a helper who can understand user questions and answer them using Oracle SQL.",  # Instructions for the LLM on how to use this tool
    host="0.0.0.0",  # Host address (0.0.0.0 allows connections from any IP)
    port=8005,  # Port number for the server
)
 

@mcp.tool()
async def get_SQL_answer(question: str) -> str:
    """
    [KO] 사용자의 자연어 질문을 Oracle SQL(READ-ONLY)로 변환하여 실행하고,
    결과를 요약한 최종 답변과 사용한 SQL을 문자열로 반환합니다.

    동작 개요
    - LangGraph 기반 NL2SQL 에이전트를 호출해 Oracle 11g+ 문법의 SELECT 쿼리를 생성합니다.
    - 쿼리는 항상 읽기 전용(SELECT)이며 세미콜론(;)을 포함하지 않습니다.
    - 결과 행 제한은 반드시 `ROWNUM <= {max_rows}` 규칙을 사용합니다.
    - 필요한 경우 테이블/스키마 메타데이터를 조회해 컬럼/조인 정보를 안전하게 추론합니다.
    - 실행 결과를 자연어로 간결히 요약하고, 참고용으로 실제 실행한 SQL을 함께 반환합니다.

    Args:
        question (str): 한국어/영어 자연어 질문.
            예) "2000년 이후 서울 거주 직원의 사번과 성명을 5건만 보여줘"
        schema (str | None): 우선 탐색할 스키마명 (예: "SCOTT"). 지정하지 않으면 기본/현재 스키마를 추론합니다.
        max_rows (int): 결과 최대 행 수 상한(기본 100). 내부적으로 ROWNUM 제한에 사용합니다.

    Returns:
        str: 다음 형식을 따르는 단일 문자열
            "ANSWER: <요약된 답 또는 간단 표형 요약>\n"
            "SQL: <실제 실행한 SELECT 문>\n"
            "ROWS: <반환 행 수>/<max_rows>"
        - ANSWER는 사람이 읽기 쉬운 한국어 요약을 목표로 합니다.
        - SQL은 투명성을 위한 참고용입니다.

    Constraints (중요 규칙):
        - DML/DDL 금지: INSERT/UPDATE/DELETE/ALTER/DROP 등은 절대 생성/실행하지 않습니다.
        - Oracle 전용 규칙:
            * 세미콜론(;) 금지
            * 결과 제한은 ROWNUM 사용 (예: WHERE ROWNUM <= :max_rows)
            * 날짜 리터럴은 DATE 'YYYY-MM-DD' 형태 권장
        - 컬럼명이 모호하거나 조인 키가 불확실하면 먼저 스키마를 조회해 안전하게 생성합니다.

    When to use:
        - 데이터베이스 질의가 필요한 “얼마나/몇 명/상위 N건/기간별 합계” 등 정량 질문
        - “테이블/컬럼을 정확히 모르지만” 자연어로 알고 싶을 때

    Examples:
        >>> await get_SQL_answer("부서별 평균 급여 상위 3개 부서 알려줘", schema="SCOTT", max_rows=3)
        >>> await get_SQL_answer("2000년 이후 입사한 서울 거주 직원 사번/성명 5건", max_rows=5)

    [EN] Converts a natural-language question into an Oracle (read-only) SQL query,
    executes it, and returns a concise answer plus the SQL used.
    Rules: Oracle 11g+ dialect, no trailing semicolons, enforce row limits via ROWNUM,
    SELECT-only (no DML/DDL).
    """
    from urllib.parse import quote_plus
    import os, oracledb
    from langchain_community.utilities import SQLDatabase
    import ast
    from langchain_community.utilities import sql_database as lc_sql
    from typing import Any
    from langchain_core.messages import ToolMessage
    from langchain_core.runnables import RunnableLambda, RunnableWithFallbacks
    from langgraph.prebuilt import ToolNode
    from sqlalchemy import MetaData, Table, Column, text
    from sqlalchemy import String, Numeric, Date, DateTime, LargeBinary
    from langchain_community.utilities import sql_database as lc_sql
    from langchain_community.agent_toolkits import SQLDatabaseToolkit
    from langchain_openai import ChatOpenAI
    from langchain_core.tools import tool
    from langchain_core.prompts import ChatPromptTemplate

    IC_DIR = r"C:\Users\USER\Downloads\instantclient-basic-windows.x64-21.19.0.0.0dbru\instantclient_21_19"
    os.add_dll_directory(IC_DIR)  # DLL 검색 경로 보강
    oracledb.init_oracle_client(
        lib_dir=IC_DIR
    )  # ← config_dir 주지 않습니다(TNS 비사용)

    # XE는 SID이므로 sid="xe" (PDB라면 service_name="XEPDB1")
    dsn = oracledb.makedsn("localhost", 1521, sid="xe")

    uri = f"oracle+oracledb://LKDM:{quote_plus('1234')}@/?dsn={quote_plus(dsn)}"
    db = SQLDatabase.from_uri(uri)
    rows_str = db.run("SELECT table_name FROM user_tables ORDER BY table_name")
    rows = ast.literal_eval(
        rows_str
    )  # -> [('AFTER_SERVICE',), ('AREA_CD',), ...] (진짜 list of tuples)
    tables = [r[0] for r in rows]

    db2 = lc_sql.SQLDatabase(
        db._engine,
        schema="LKDM",
        sample_rows_in_table_info=0,  # ★ 핵심
    )

    # 3) 내부 속성 강제 주입(핵심 핫픽스)
    db2._all_tables = set(tables)  # 리플렉션 대신 우리가 수집한 목록 사용
    db2._include_tables = set()  # 제한 안 둘 거면 빈 집합 유지
    db2._ignore_tables = set()  # 무시 없음

    def create_tool_node_with_fallback(tools: list) -> RunnableWithFallbacks[Any, dict]:
        """
        Create a ToolNode with a fallback to handle errors and surface them to the agent.
        """
        return ToolNode(tools).with_fallbacks(
            [RunnableLambda(handle_tool_error)], exception_key="error"
        )

    def handle_tool_error(state) -> dict:
        error = state.get("error")
        tool_calls = state["messages"][-1].tool_calls
        return {
            "messages": [
                ToolMessage(
                    content=f"Error: {repr(error)}\n please fix your mistakes.",
                    tool_call_id=tc["id"],
                )
                for tc in tool_calls
            ]
        }

    OWNER = "LKDM"
    TABLES = tables  # ← 여러 개

    def oracle_to_sqla_type(dt, L, P, S):
        dt = (dt or "").upper()
        if dt in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"):
            return String(int(L) if L else None)
        if dt == "NUMBER":
            if P is not None and S is not None:
                return Numeric(int(P), int(S))
            elif P is not None:
                return Numeric(int(P))
            else:
                return Numeric()
        if dt == "DATE":
            return Date()
        if dt.startswith("TIMESTAMP"):
            return DateTime()
        if dt in ("RAW", "BLOB"):
            return LargeBinary()
        # 기타 타입은 일단 길이 기반 String으로 처리
        return String(int(L) if L else None)

    # 1) 데이터사전에서 컬럼 읽어서 메타데이터에 '여러 테이블' 등록
    md = MetaData()
    present = []  # 실제로 컬럼을 읽어온(존재하는) 테이블만 담음
    with db2._engine.connect() as conn:
        for t in TABLES:
            rows = conn.execute(
                text(
                    """
                SELECT column_id, column_name, data_type, data_length,
                    data_precision, data_scale, nullable
                FROM all_tab_columns
                WHERE owner=:o AND table_name=:t
                ORDER BY column_id
            """
                ),
                {"o": OWNER, "t": t},
            ).fetchall()
            if not rows:
                continue
            cols = []
            for _, name, dt, L, P, S, nullable in rows:
                cols.append(
                    Column(
                        str(name),
                        oracle_to_sqla_type(dt, L, P, S),
                        nullable=(nullable == "Y"),
                    )
                )
            Table(t, md, *cols, schema=OWNER)  # ★ reflect 안 쓰고 직접 정의
            present.append(t)

    # 2) 샘플 로우는 끄고(SQLDatabase가 DUAL 실수 안 하게) + 메타데이터 주입
    db_fix = lc_sql.SQLDatabase(
        db2._engine,
        schema=OWNER,
        metadata=md,
        sample_rows_in_table_info=0,  # ★ 중요: ORA-00936 방지
    )

    # 3) 내부 테이블 집합을 우리가 넣은 목록으로 설정(존재 검증 통과)
    db_fix._all_tables = set(present)
    db_fix._include_tables = set()
    db_fix._ignore_tables = set()

    # 4) 툴킷 재생성 후 호출 (여러 테이블은 콤마 구분 문자열로!)
    toolkit = SQLDatabaseToolkit(db=db_fix, llm=ChatOpenAI(model="gpt-4o-mini"))
    tools = toolkit.get_tools()
    # by = {t.name: t for t in tools}
    list_tables_tool = next(tool for tool in tools if tool.name == "sql_db_list_tables")
    get_schema_tool = next(tool for tool in tools if tool.name == "sql_db_schema")

    @tool
    def db_query_tool(query: str) -> str:
        """
        Execute a READ-ONLY SQL query (Oracle).
        - Strip trailing semicolons to avoid ORA-00911.
        - Return explicit JSON on success: {"rows":[...], "columns":[...], "rowcount":N}
        - Return explicit 'Error: ...' text on failure.
        - Return explicit 'Empty: 0 rows' on empty results (루프 방지에 중요!)
        """

        import re, json

        if not query:
            return "Error: Empty query."

        sql = query.strip()
        if sql.endswith(";"):
            sql = sql[:-1].rstrip()

        if re.search(
            r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|MERGE|CREATE)\b", sql, re.I
        ):
            return "Error: Only read-only SELECT queries are allowed."

        try:
            result = db.run_no_throw(sql)
        except Exception as e:
            return f"Error: {str(e)}"

        # ✅ None/빈 문자열 → 에러가 아니라 'Empty'
        if result is None or result == "":
            return "Empty: 0 rows"

        rows, cols = None, None
        if isinstance(result, dict):
            if result.get("error"):
                return f"Error: {result['error']}"
            rows = result.get("rows") or result.get("data") or result.get("result")
            cols = result.get("columns") or result.get("cols")
        elif isinstance(result, (list, tuple)):
            rows = result
        else:
            try:
                return json.dumps(result, ensure_ascii=False, default=str)
            except Exception:
                return str(result)

        if rows is None:
            return "Empty: 0 rows"

        if isinstance(rows, (list, tuple)) and len(rows) == 0:
            return "Empty: 0 rows"

        return json.dumps(
            {
                "rows": rows,
                "columns": cols,
                "rowcount": (len(rows) if hasattr(rows, "__len__") else None),
            },
            ensure_ascii=False,
            default=str,
        )

    query_check_system = """You are a SQL expert with a strong attention to detail.
    Double check the SQL query for common mistakes, including:
    - Using NOT IN with NULL values
    - Using UNION when UNION ALL should have been used
    - Using BETWEEN for exclusive ranges
    - Data type mismatch in predicates
    - Properly quoting identifiers
    - Using the correct number of arguments for functions
    - Casting to the correct data type
    - Using the proper columns for joins

    If there are any of the above mistakes, rewrite the query. If there are no mistakes, just reproduce the original query.

    You will call the appropriate tool to execute the query after running this check.
    """

    query_check_prompt = ChatPromptTemplate.from_messages(
        [("system", query_check_system), ("placeholder", "{messages}")]
    )
    # query_check는 LLM이 텍스트로 검수/수정만 하고, 그다음 툴을 호출하라고 지시
    # 이때 쓰는 “툴 이름”이 db_query_tool로 같은 함수를 가리키죠
    query_check = query_check_prompt | ChatOpenAI(
        model="gpt-4o-mini", temperature=0
    ).bind_tools(
        [db_query_tool],
        tool_choice="required",  # tool_choice="required"로 툴 호출을 반드시 하도록 강제
    )

    from typing import Annotated, Literal
    from langchain_core.messages import (
        BaseMessage,
        HumanMessage,
        ToolMessage,
        AIMessage,
    )
    from langchain_openai import ChatOpenAI
    from pydantic import BaseModel, Field
    from typing_extensions import TypedDict
    from langgraph.graph import END, StateGraph, START
    from langgraph.graph.message import AnyMessage, add_messages

    class State(TypedDict):
        messages: Annotated[list[AnyMessage], add_messages]

    workflow = StateGraph(State)

    def first_tool_call(state: State) -> dict[str, list[AIMessage]]:
        return {
            "messages": [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "sql_db_list_tables",
                            "args": {},
                            "id": "tool_abcd123",
                        }
                    ],
                )
            ]
        }

    def model_check_query(state: State) -> dict[str, list[AIMessage]]:
        """
        Use this tool to double-check if your query is correct before executing it.
        """
        return {"messages": [query_check.invoke({"messages": [state["messages"][-1]]})]}

    workflow.add_node("first_tool_call", first_tool_call)
    workflow.add_node(
        "list_tables_tool",
        create_tool_node_with_fallback(
            [list_tables_tool]
        ),  # ToolNode 생성해서 바로 workflow에 가가
    )
    workflow.add_node(
        "get_schema_tool",
        create_tool_node_with_fallback(
            [get_schema_tool]
        ),  # ToolNode 생성해서 바로 workflow에 가가
    )

    model_get_schema = ChatOpenAI(model="gpt-4o-mini", temperature=0).bind_tools(
        [get_schema_tool]
    )

    workflow.add_node(
        "model_get_schema",
        lambda state: {  # 람다문법 -> lambda <파라미터들>: <하나의 표현식>
            "messages": [model_get_schema.invoke(state["messages"])],
        },
    )

    # 1) 최근 사용자 질문 추출
    def get_last_user_question(messages: list[BaseMessage]) -> str:
        for m in reversed(messages):
            if isinstance(m, HumanMessage):
                return m.content or ""
            # 튜플 입력을 쓰는 경우를 대비 (("user", "..."))
            if isinstance(m, tuple) and len(m) >= 2 and m[0] in ("user", "human"):
                return str(m[1])
        return ""

    # 2) (선택) 최근 스키마 툴 결과 모으기
    def collect_schema_context(messages: list[BaseMessage]) -> str:
        parts = []
        for m in messages:
            if isinstance(m, ToolMessage) and getattr(m, "name", "") in {
                "sql_db_schema",
                "get_schema_tool",
            }:
                parts.append(m.content or "")
        return "\n\n".join(parts).strip()

    # 3) 프롬프트에 user_question / db_context 변수 추가
    query_gen_system = """
    You are a SQL expert with strong attention to detail.

    ENVIRONMENT
    - Target dialect: ORACLE (11g+). The driver rejects trailing semicolons.
    - Data is likely Korean; addresses may contain '서울' instead of 'Seoul'.

    TOOLS YOU MAY CALL
    - list_tables_tool  : list tables
    - get_schema_tool   : fetch schema(s)
    - db_query_tool     : EXECUTE a READ-ONLY SQL query

    SQL RULES
    - Produce syntactically correct **Oracle** SQL.
    - **Never** include a trailing semicolon (;) in queries sent to db_query_tool.
    - Row limiting: use `WHERE ROWNUM <= N`. If you need ORDER BY, wrap with a subquery, then apply ROWNUM.
    - Select only relevant columns (avoid SELECT *).
    - No DML/DDL.

    LOCALIZATION HINTS
    - When filtering addresses for Seoul, match both variants:
    (e.ADDRESS LIKE '%서울%' OR UPPER(e.ADDRESS) LIKE '%SEOUL%').

    ERROR HANDLING (read tool results from chat history)
    - If ORA-00911 -> remove semicolons and retry.
    - If ORA-00933 -> fix Oracle syntax (replace LIMIT with ROWNUM, avoid FETCH FIRST if driver rejects it).
    - If the tool only returns a generic "Query failed" message,
    then: (1) verify column names with get_schema_tool, (2) isolate joins by testing each table,
    (3) try a simpler SELECT first, and (4) prefer DATE literals: `e.IBSA_DATE >= DATE '2000-01-01'`.

    RETRY BUDGET
    - After **3 total db_query_tool errors in this conversation**, DO NOT call any more tools.
    Output the final answer message described below.

    FINAL ANSWER (NO TOOL CALL)
    - When you have enough information, output a single assistant message WITHOUT any tool calls:
    <FINAL>
    ...concise answer...
    (optionally include the final SQL you used and a brief rationale)
    </FINAL>
    - If, after the retry budget, you still lack information:
    <FINAL>I don't have enough information. Here is what I tried and what is missing: ...</FINAL>
    """

    query_gen_prompt = ChatPromptTemplate.from_messages(
        [
            ("system", query_gen_system),
            # 🔹 여기에 질문/스키마를 시스템 컨텍스트로 명시
            ("system", "User question:\n{user_question}"),
            ("placeholder", "{messages}"),
        ]
    )
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    query_gen = query_gen_prompt | llm.bind_tools(
        [db_query_tool, get_schema_tool, list_tables_tool],
        # 최종은 툴콜 없이 <FINAL>로 끝내야 하므로 'required'는 쓰지 않습니다.
        parallel_tool_calls=False,
    )

    # 4) 노드에서 payload를 dict로 넘기기
    def query_gen_node(state: State):
        messages = state["messages"]
        user_q = get_last_user_question(messages)
        db_ctx = state.get("db_context") or collect_schema_context(
            messages
        )  # state에 이미 있으면 그걸 우선
        msg = query_gen.invoke({"messages": messages, "user_question": user_q})
        return {"messages": [msg]}

    workflow.add_node("query_gen", query_gen_node)
    workflow.add_node(
        "execute_query", create_tool_node_with_fallback([db_query_tool])
    )  # 쿼리 실행 역할

    EXECUTE_TOOL_NAMES = {"db_query_tool"}

    def should_continue(state: State) -> Literal["execute_query", "query_gen", END]:
        last = state["messages"][-1]

        # 1) 모델이 툴콜을 냈고 그 툴이 실행 계열이면 → execute_query
        if isinstance(last, AIMessage) and getattr(last, "tool_calls", None):
            names = {tc["name"] for tc in last.tool_calls}
            if names & EXECUTE_TOOL_NAMES:
                return "execute_query"
            # 실행계열이 아닌 툴콜이면 모델로 되돌려 추가 판단
            return "query_gen"

        # 2) 모델이 텍스트만 냈다면(툴콜 없음) → 종료
        if isinstance(last, AIMessage) and not getattr(last, "tool_calls", None):
            return END

        # 3) 툴 결과면 → 모델이 읽고 마무리/추가 지시하도록
        if isinstance(last, ToolMessage):
            return "query_gen"

        # 4) 그 외 안전망
        return "query_gen"

    workflow.add_edge(START, "first_tool_call")
    workflow.add_edge("first_tool_call", "list_tables_tool")
    workflow.add_edge("list_tables_tool", "model_get_schema")
    workflow.add_edge("model_get_schema", "get_schema_tool")
    workflow.add_edge("get_schema_tool", "query_gen")
    workflow.add_conditional_edges(
        "query_gen",
        should_continue,
    )
    workflow.add_edge("execute_query", "query_gen")

    app = workflow.compile()

    inputs = {"messages": [("user", question)]}

    ## 모든 내용이 나와서 지저분함 아래 결과만 출력하게 수정함 !!!
    # result = []
    # for chunk in app.stream(inputs, stream_mode="values"):
    #     for state_key, state_value in chunk.items():
    #         if state_key == "messages":
    #             msg = state_value[-1]
    #             result.append(str(msg))

    # return "\n".join(result)

    final_text = None
    for chunk in app.stream(inputs, stream_mode="values"):
        msgs = chunk.get("messages", [])
        if msgs:
            last = msgs[-1]
            # last.content에 <FINAL>...</FINAL> 들어있음
            if isinstance(last, AIMessage) and last.content and "<FINAL>" in last.content:
                final_text = last.content

    return final_text or "Error: No FINAL answer produced."

if __name__ == "__main__":
    # Print a message indicating the server is starting
    print("mcp remote server is running...")

    # Start the MCP server with SSE transport
    # Server-Sent Events (SSE) transport allows the server to communicate with clients
    # over HTTP, making it suitable for remote/distributed deployments
    mcp.run(transport="sse")
