from dotenv import load_dotenv
load_dotenv()
import psycopg2, os
from psycopg2.extras import execute_values
from nltk.sentiment import SentimentIntensityAnalyzer

PG = dict(
    host=os.getenv("PGHOST","localhost"), port=os.getenv("PGPORT","5432"),
    user=os.getenv("PGUSER","ds"), password=os.getenv("PGPASSWORD","ds"),
    dbname=os.getenv("PGDATABASE","ev")
)

def _pg_conn(): 
    return psycopg2.connect(**PG)

def label_from_compound(c):
    if c >= 0.05: return "positive"
    if c <= -0.05: return "negative"
    return "neutral"

def run_sentiment(batch=1000):
    sia = SentimentIntensityAnalyzer()
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT m.id, m.text_clean
            FROM dw.messages m
            LEFT JOIN dw.sentiment s ON s.id = m.id
            WHERE s.id IS NULL
            LIMIT %s
        """, (batch,))
        rows = cur.fetchall()
        out = []
        for id_, text in rows:
            score = sia.polarity_scores(text)["compound"]
            out.append((id_, score, label_from_compound(score)))
        if out:
            execute_values(cur, """
                INSERT INTO dw.sentiment (id, compound, label)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET compound = EXCLUDED.compound, label = EXCLUDED.label
            """, out)

if __name__ == "__main__":
    run_sentiment()

