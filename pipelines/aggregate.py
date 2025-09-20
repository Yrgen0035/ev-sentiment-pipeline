from dotenv import load_dotenv
load_dotenv()
import psycopg2, os

PG = dict(
    host=os.getenv("PGHOST","localhost"), port=os.getenv("PGPORT","5432"),
    user=os.getenv("PGUSER","ds"), password=os.getenv("PGPASSWORD","ds"),
    dbname=os.getenv("PGDATABASE","ev")
)

def _pg_conn(): 
    return psycopg2.connect(**PG)

SQL = """
INSERT INTO dw.daily_metrics (day, avg_compound, pos_ratio, volume)
SELECT
  DATE(m.created_at) AS day,
  AVG(s.compound) AS avg_compound,
  AVG(CASE WHEN s.label='positive' THEN 1.0 ELSE 0.0 END) AS pos_ratio,
  COUNT(*) AS volume
FROM dw.messages m
JOIN dw.sentiment s ON s.id = m.id
GROUP BY 1
ON CONFLICT (day) DO UPDATE
  SET avg_compound = EXCLUDED.avg_compound,
      pos_ratio = EXCLUDED.pos_ratio,
      volume = EXCLUDED.volume;
"""

if __name__ == "__main__":
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(SQL)

