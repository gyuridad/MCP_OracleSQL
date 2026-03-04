- 이 파일은 자연어 질문을 하면 DB에 접속해서 조회한 후 결과를 반환하는 LangGraph기반 SQL-Agent 입니다.
- 이 파일 주요 내용은 아래의 주소에 정리하였습니다.
  - https://www.notion.so/Smithery-25f5d0bd696380d08140dc95df1ed0ac?p=3185d0bd696380c8bdffffc8db6346c4&showMoveTo=true 

사용자 질문
     │
     ▼
테이블 목록 조회
(sql_db_list_tables)
     │
     ▼
관련 테이블 스키마 조회
(sql_db_schema)
     │
     ▼
SQL 생성 (LLM)
     │
     ▼
SQL 검수 (LLM)
     │
     ▼
SQL 실행
(db_query_tool)
     │
     ▼
결과 분석 및 최종 답변
