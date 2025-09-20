# app/dash_app.py  — enhanced metrics view

from dotenv import load_dotenv
load_dotenv()

import os, pandas as pd, psycopg2
from dash import Dash, dcc, html
import plotly.graph_objects as go

PG = dict(
    host=os.getenv("PGHOST","localhost"), port=os.getenv("PGPORT","5432"),
    user=os.getenv("PGUSER","ds"), password=os.getenv("PGPASSWORD","ds"),
    dbname=os.getenv("PGDATABASE","ev")
)

def fetch_df(sql):
    with psycopg2.connect(**PG) as conn:
        return pd.read_sql(sql, conn)

app = Dash(__name__)
app.title = "EV Sentiment Dashboard"

def build_layout():
    # --- Core aggregates (one row per day) ---
    df = fetch_df("""
        SELECT day::timestamp AS day, avg_compound, volume
        FROM dw.daily_metrics
        ORDER BY day;
    """)

    # --- Global stats for context (N, last update) ---
    stats = fetch_df("""
        SELECT NOW() AS now_utc,
               COUNT(*) AS total_msgs,
               COALESCE(MAX(created_at), NOW()) AS last_msg_ts
        FROM dw.messages;
    """)
    total_msgs = int(stats["total_msgs"][0]) if not stats.empty else 0
    last_ts = stats["last_msg_ts"][0] if not stats.empty else None

    # --- Sentiment distribution (last 7 days) ---
    dist = fetch_df("""
        SELECT s.label, COUNT(*) AS cnt
        FROM dw.sentiment s
        JOIN dw.messages m ON m.id = s.id
        WHERE m.created_at >= NOW() - INTERVAL '7 days'
        GROUP BY s.label
        ORDER BY cnt DESC;
    """)

    # ==== Figure 1: Sentiment trend with 7-day moving average ====
    fig_sent = go.Figure()

    if not df.empty:
        # 7-day moving average (centered at end)
        df_ma = df.copy()
        df_ma["ma7"] = df_ma["avg_compound"].rolling(7, min_periods=2).mean()

        # base line (daily avg)
        fig_sent.add_trace(go.Scatter(
            x=df["day"], y=df["avg_compound"],
            mode="lines+markers+text",
            text=[f"N={n}" for n in df["volume"].fillna(0).astype(int)],
            textposition="top center",
            name="Daily avg sentiment"
        ))

        # moving average
        fig_sent.add_trace(go.Scatter(
            x=df_ma["day"], y=df_ma["ma7"],
            mode="lines",
            name="7-day MA"
        ))

        fig_sent.update_yaxes(range=[-1, 1], title_text="Avg compound (−1…1)")
        fig_sent.update_xaxes(title_text="Date")
        fig_sent.update_layout(
            margin=dict(l=30, r=20, t=30, b=30),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0)
        )

    # ==== Figure 2: Volume bar chart ====
    fig_vol = go.Figure()
    if not df.empty:
        fig_vol.add_trace(go.Bar(x=df["day"], y=df["volume"], name="Messages/day"))
        fig_vol.update_yaxes(title_text="Messages")
        fig_vol.update_xaxes(title_text="Date")
        fig_vol.update_layout(margin=dict(l=30, r=20, t=20, b=30))

    # ==== Figure 3: Sentiment distribution (last 7 days) ====
    fig_pie = go.Figure()
    if not dist.empty:
        fig_pie.add_trace(go.Pie(
            labels=dist["label"],
            values=dist["cnt"],
            textinfo="label+percent",
            hovertemplate="%{label}: %{value} msgs (%{percent})<extra></extra>"
        ))
        fig_pie.update_layout(margin=dict(l=30, r=20, t=20, b=20))

    # Header with key metrics
    header = html.Div([
        html.H2("EV Sentiment Dashboard"),
        html.P(
            f"Last update: {last_ts} · Total messages: {total_msgs}",
            style={"color": "#555", "marginBottom": "8px"}
        ),
        html.P(
            "Daily average sentiment is clamped to −1…1. Point labels show message counts per day (N).",
            style={"color": "#777", "fontSize": "0.9rem"}
        )
    ])

    return html.Div([
        header,
        html.Hr(),
        html.H3("Sentiment over time"),
        dcc.Graph(figure=fig_sent),

        html.H3("Message volume"),
        dcc.Graph(figure=fig_vol),

        html.H3("Sentiment distribution (last 7 days)"),
        dcc.Graph(figure=fig_pie),
    ], style={"maxWidth": "1100px", "margin": "0 auto"})

app.layout = build_layout

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=False)
