# test_client.py
import asyncio
import sys

from mcp import ClientSession, types
from mcp.client.sse import sse_client


def _extract_text(result: types.CallToolResult) -> str:
    """CallToolResult에서 사람이 읽을 텍스트만 뽑기"""
    texts = []
    for c in result.content:
        if isinstance(c, types.TextContent):
            texts.append(c.text)
    # 서버가 json_response=True 같은 설정이면 structuredContent도 올 수 있음
    if getattr(result, "structuredContent", None):
        texts.append(f"[structuredContent]\n{result.structuredContent}")
    return "\n".join(texts).strip()


async def main():
    # 기본: 네 서버 포트 8005, SSE 기본 경로 /sse
    server_url = sys.argv[1] if len(sys.argv) >= 2 else "http://localhost:8005/sse"

    # 1) SSE로 서버 연결 → read/write 스트림 획득
    async with sse_client(server_url) as (read, write):
        # 2) MCP 세션 생성
        async with ClientSession(read, write) as session:
            # 3) 초기화(핵심)
            await session.initialize()

            # 4) 툴 목록 확인
            tools = await session.list_tools()
            tool_names = [t.name for t in tools.tools]
            print("=== Available tools ===")
            for name in tool_names:
                print("-", name)

            # 5) 간단 REPL: 질문 입력 → get_SQL_answer 호출
            print("\nType a question. (exit to quit)\n")
            while True:
                q = input("Q> ").strip()
                if not q:
                    continue
                if q.lower() in ("exit", "quit"):
                    break

                # 네 MCP 서버의 tool 이름이 get_SQL_answer 라고 가정
                # (mcp.tool()로 등록된 함수명)
                result = await session.call_tool(
                    "get_SQL_answer",
                    arguments={"question": q},
                )
                print("\nA>")
                print(_extract_text(result))
                print()


if __name__ == "__main__":
    asyncio.run(main())


### 실향 예시:
# python mcp_server_remote.py 킨 후 다른 터미널에서
# python test_client.py 실행하면

# 서버의 tool 목록이 찍히고
# Q> 프롬프트에서 질문을 입력하면
# get_SQL_answer 결과가 출력돼.
