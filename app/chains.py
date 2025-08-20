import duckdb, pandas as pd
from pathlib import Path
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate

def connect_duckdb(data_dir: str = "app/data"):
    con = duckdb.connect(database=":memory:")
    for csv in Path(data_dir).glob("*.csv"):
        table = csv.stem.lower()
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_csv_auto('{csv.as_posix()}');")
    return con

def schema_text(con: duckdb.DuckDBPyConnection) -> str:
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    lines = []
    for t in tables:
        df = con.execute(f"DESCRIBE {t}").fetchdf()[["column_name","column_type"]]
        cols = ", ".join(f"{r.column_name}:{r.column_type}" for _, r in df.iterrows())
        lines.append(f"- {t}({cols})")
    return "\n".join(lines)

def enforce_readonly(sql: str) -> str:
    low = sql.strip().lower()
    banned = ("insert","update","delete","drop","alter","create","truncate","attach","copy","pragma","call")
    if any(b in low for b in banned) or ";" in low:
        raise ValueError("Only a single SELECT is allowed.")
    return sql

llm_sql = ChatOpenAI(model="gpt-4o-mini", temperature=0)
llm_answer = ChatOpenAI(model="gpt-4o-mini", temperature=0)

SQL_PROMPT = ChatPromptTemplate.from_messages([
  ("system",
   "You are a SQL planner for DuckDB. Output ONLY one SELECT statement using ONLY these tables:\n{schema}\n"
   "No DDL/DML. Use correct column names."),
  ("human", "Question: {question}")
])

ANSWER_PROMPT = ChatPromptTemplate.from_messages([
  ("system", "Use the provided table snippet to answer concisely and precisely."),
  ("human", "Question: {question}\nData (first rows):\n{preview}")
])

def plan_sql(question: str, schema: str) -> str:
    msg = SQL_PROMPT.format_messages(schema=schema, question=question)
    sql = llm_sql.invoke(msg).content
    sql = sql.strip().strip("```").replace("sql", "").strip()
    return enforce_readonly(sql)

def answer_from_df(question: str, df: pd.DataFrame) -> str:
    preview = df.head(20).to_csv(index=False)
    msg = ANSWER_PROMPT.format_messages(question=question, preview=preview)
    return llm_answer.invoke(msg).content
