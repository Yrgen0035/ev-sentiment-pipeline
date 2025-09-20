CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dw;

-- Raw events, minimally typed
CREATE TABLE IF NOT EXISTS raw.events (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,             -- twitter, reddit, hn, rss
  created_at TIMESTAMP NOT NULL,
  author TEXT,
  text TEXT NOT NULL,
  lang TEXT,
  meta JSONB,
  ingested_at TIMESTAMP DEFAULT NOW()
);

-- Cleaned and normalized messages
CREATE TABLE IF NOT EXISTS dw.messages (
  id TEXT PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  author TEXT,
  text_clean TEXT NOT NULL,
  lang TEXT,
  source TEXT NOT NULL
);

-- Sentiment enrichment
CREATE TABLE IF NOT EXISTS dw.sentiment (
  id TEXT PRIMARY KEY,
  compound NUMERIC,
  label TEXT,                        -- positive / neutral / negative
  scored_at TIMESTAMP DEFAULT NOW()
);

-- Daily aggregations
CREATE TABLE IF NOT EXISTS dw.daily_metrics (
  day DATE PRIMARY KEY,
  avg_compound NUMERIC,
  pos_ratio NUMERIC,
  volume INTEGER
);
