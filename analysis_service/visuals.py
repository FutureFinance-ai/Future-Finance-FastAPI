from __future__ import annotations

from typing import Literal
import pandas as pd


async def cashflow_line(df: pd.DataFrame):
    try:
        import plotly.express as px
    except Exception:
        return None
    if df.empty:
        return None
    fig = px.line(df, x="month", y=["credits", "debits", "net_cash_flow"], title="Monthly Cash Flow")
    return fig


async def category_bar(df: pd.DataFrame):
    try:
        import plotly.express as px
    except Exception:
        return None
    if df.empty:
        return None
    fig = px.bar(df, x="transaction_category", y="spend", color="transaction_category", title="Category Spend")
    return fig


async def cashflow_sankey(df: pd.DataFrame):
    try:
        import plotly.graph_objects as go
    except Exception:
        return None
    if df.empty:
        return None
    # Simple sankey: Income -> Categories
    income = float(df["credit"].sum())
    spend_by_cat = (
        df.assign(spend=df["debit"].abs())
          .groupby("transaction_category")["spend"].sum()
          .sort_values(ascending=False)
    )
    labels = ["Income"] + list(spend_by_cat.index)
    source = [0] * len(spend_by_cat)
    target = list(range(1, len(spend_by_cat) + 1))
    values = spend_by_cat.values.tolist()
    fig = go.Figure(data=[go.Sankey(
        node=dict(label=labels),
        link=dict(source=source, target=target, value=values)
    )])
    fig.update_layout(title_text="Cash Flow Sankey", font_size=12)
    return fig


async def figure_to_response(fig, fmt: Literal["json", "png"] = "json"):
    if fig is None:
        return None
    if fmt == "json":
        return fig.to_plotly_json()
    # png fallback via kaleido
    try:
        import plotly.io as pio
        import base64
        png_bytes = pio.to_image(fig, format="png")
        return png_bytes
    except Exception:
        return fig.to_plotly_json()


