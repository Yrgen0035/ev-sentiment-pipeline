from dotenv import load_dotenv
load_dotenv()
import re, psycopg2, os
from langdetect import detect
from psycopg2.extras import execute_values

PG = dict(
    host=os.getenv("PGHOST","localhost"), port=os.getenv("PGPORT","5432"),
    user=os.getenv("PGUSER","ds"), password=os.getenv("PGPASSWORD","ds"),
    dbname=os.getenv("PGDATABASE","ev")
)

def _pg_conn(): 
    return psycopg2.connect(**PG)

URL = re.compile(r"http\S+")
MULTI = re.compile(r"\s+")

def normalize(text):
    t = URL.sub("", text or "")
    t = re.sub(r"[@#]\w+","", t)
    t = t.lower()
    t = MULTI.sub(" ", t).strip()
    return t

def run_clean(batch=1000):
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT r.id, r.text, r.source, r.created_at, r.author
            FROM raw.events r
            LEFT JOIN dw.messages m ON m.id = r.id
            WHERE m.id IS NULL
            LIMIT %s
        """, (batch,))
        rows = cur.fetchall()
        out = []
        for id_, text, source, created_at, author in rows:
            t = normalize(text)
            if not t:
                continue
            try:
                lang = detect(t)
            except Exception:
                lang = None
            if lang and lang != "en":
                continue
            out.append((id_, created_at, author, t, lang, source))
        if out:
            execute_values(cur, """
                INSERT INTO dw.messages (id, created_at, author, text_clean, lang, source)
                VALUES %s
                ON CONFLICT (id) DO NOTHING
            """, out)

if __name__ == "__main__":
    run_clean()

