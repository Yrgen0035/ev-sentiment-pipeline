from dotenv import load_dotenv
load_dotenv()
import requests, hashlib, json
from datetime import datetime, timedelta, timezone
import feedparser
import psycopg2
from psycopg2.extras import execute_values
import os

PG = dict(
    host=os.getenv("PGHOST","localhost"), port=os.getenv("PGPORT","5432"),
    user=os.getenv("PGUSER","ds"), password=os.getenv("PGPASSWORD","ds"),
    dbname=os.getenv("PGDATABASE","ev")
)

def _pg_conn():
    return psycopg2.connect(**PG)

def upsert_raw(rows):
    if not rows: 
        return
    with _pg_conn() as conn, conn.cursor() as cur:
        execute_values(cur, '''
            INSERT INTO raw.events (id, source, created_at, author, text, lang, meta)
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        ''', rows)

def hn_search_ev(last_hours=24):
    # HN Algolia API - simple search for "electric vehicle"
    since = int((datetime.now(timezone.utc)-timedelta(hours=last_hours)).timestamp())
    url = f"https://hn.algolia.com/api/v1/search_by_date?query=electric%20vehicle&tags=story,comment&numericFilters=created_at_i>{since}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json().get("hits", [])
    rows = []
    for h in data:
        text = (h.get("title") or h.get("comment_text") or "").strip()
        if not text:
            continue
        created = datetime.fromtimestamp(h["created_at_i"], tz=timezone.utc)
        ev_id = f"hn_{h['objectID']}"
        author = h.get("author")
        meta = {k:h.get(k) for k in ("url","points","parent_id","story_id","story_title")}
        rows.append((ev_id, "hn", created, author, text, None, json.dumps(meta)))
    upsert_raw(rows)

def rss_fetch_ev(feeds):
    rows = []
    for feed in feeds:
        fp = feedparser.parse(feed)
        for e in fp.entries:
            title = (e.get("title") or "").strip()
            summary = (e.get("summary") or "").strip()
            text = f"{title}. {summary}".strip()
            if not text:
                continue
            created = None
            for key in ("published_parsed","updated_parsed"):
                if getattr(e, key, None):
                    created = datetime(*getattr(e, key)[:6], tzinfo=timezone.utc)
                    break
            if created is None:
                created = datetime.now(timezone.utc)
            base = (e.get("link") or title) + str(created.timestamp())
            ev_id = "rss_" + hashlib.md5(base.encode()).hexdigest()
            rows.append((ev_id, "rss", created, e.get("author"), text, None, json.dumps({"link": e.get("link")})))
    upsert_raw(rows)

if __name__ == "__main__":
    hn_search_ev(24)
    rss_fetch_ev([
        "https://www.reuters.com/subjects/autos-transportation/rss",
        "https://www.theverge.com/rss/index.xml",
    ])

