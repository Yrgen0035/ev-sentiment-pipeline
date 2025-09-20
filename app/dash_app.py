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
    df = fetch_df("SELECT * FROM dw.daily_metrics ORDER BY day;")
    fig_sent = go.Figure()
    if not df.empty:
        fig_sent.add_trace(go.Scatter(x=df["day"], y=df["avg_compound"], mode="lines+markers", name="Avg sentiment"))

    fig_vol = go.Figure()
    if not df.empty:
        fig_vol.add_trace(go.Bar(x=df["day"], y=df["volume"], name="Volume"))

    dist = fetch_df("""
    SELECT label, COUNT(*) cnt
    FROM dw.sentiment s
    JOIN dw.messages m ON m.id = s.id
    WHERE m.created_at >= NOW() - INTERVAL '7 days'
    GROUP BY label ORDER BY cnt DESC;
    """ )
    fig_pie = go.Figure()
    if not dist.empty:
        fig_pie = go.Figure(data=[go.Pie(labels=dist["label"], values=dist["cnt"])])

    return html.Div([
        html.H2("EV Sentiment Dashboard"),
        html.P("Daily average sentiment and message volume"),
        dcc.Graph(figure=fig_sent),
        dcc.Graph(figure=fig_vol),
        html.H3("Last 7 days sentiment distribution"),
        dcc.Graph(figure=fig_pie),
    ])

app.layout = build_layout

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=False)

