import os, asyncio, httpx
from fastapi import FastAPI, Request
from app.chains import connect_duckdb, schema_text, plan_sql, answer_from_df

app = FastAPI()

DATA_DIR = os.getenv("DATA_DIR", "app/data")
LINE_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_REPLY_URL = "https://api.line.me/v2/bot/message/reply"

con = connect_duckdb(DATA_DIR)
schema = schema_text(con)

async def line_reply(reply_token: str, text: str):
    async with httpx.AsyncClient(timeout=10) as client:
        payload = {"replyToken": reply_token, "messages": [{"type":"text","text": text[:4900]}]}
        headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
        r = await client.post(LINE_REPLY_URL, headers=headers, json=payload)
        r.raise_for_status()

@app.post("/ingest")
async def ingest(req: Request):
    body = await req.json()
    events = body.get("events", [])
    tasks = []
    for e in events:
        if e.get("type") != "message": continue
        msg = e.get("message", {})
        if msg.get("type") != "text": continue

        q = msg["text"]
        token = e["replyToken"]

        async def handle():
            try:
                sql = plan_sql(q, schema)
                df = con.execute(sql).fetchdf()
                ans = "No matching rows." if df.empty else answer_from_df(q, df)
            except Exception as ex:
                ans = f"Query error: {ex}"
            await line_reply(token, ans)

        tasks.append(asyncio.create_task(handle()))

    if tasks: await asyncio.gather(*tasks)
    return {"ok": True}
