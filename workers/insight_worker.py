from __future__ import annotations

from typing import Any, List
from datetime import datetime, timedelta, timezone
import pandas as pd

from arq.connections import RedisSettings
from arq import cron

from settings.config import settings
from settings.db import get_service_db
from insights.insight_repo import InsightRepo


async def _fetch_user_transactions(db, user_id: str, days: int = 90) -> List[dict]:
    since_iso = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    query = """
    SELECT id, value_date, trans_time, description, category, amount_minor
    FROM transaction
    WHERE account.owner = type::thing($user_id)
      AND value_date >= $since
    """
    res = await db.query(query, {"user_id": user_id, "since": since_iso})
    return res[0]


def _build_dataframe(rows: List[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["value_date", "trans_time", "description", "category", "debit", "credit"])
    s = pd.DataFrame(rows)
    # amounts in minor units -> major
    s["amount"] = (s.get("amount_minor") or 0) / 100.0 if "amount_minor" in s.columns else 0.0
    # derive debit/credit
    s["debit"] = s["amount"].where(s["amount"] < 0.0, 0.0).abs()
    s["credit"] = s["amount"].where(s["amount"] > 0.0, 0.0)
    # dates
    s["value_date"] = pd.to_datetime(s.get("value_date"), errors="coerce")
    s["trans_time"] = pd.to_datetime(s.get("trans_time"), errors="coerce")
    # normalize text
    s["description"] = s.get("description", "").astype("string")
    s["category"] = s.get("category", "Uncategorized").astype("string")
    return s


def _generate_messages(df: pd.DataFrame) -> List[tuple[str, dict]]:
    messages: List[tuple[str, dict]] = []
    if df.empty:
        return messages
    df["month"] = df["value_date"].dt.to_period("M").dt.to_timestamp()
    # Spend by category per month
    df["spend"] = df["debit"].abs()
    by_cat = df.groupby(["month", "category"], dropna=False)["spend"].sum().reset_index()
    if by_cat.empty:
        return messages
    # Compare last month vs previous month
    months = sorted(by_cat["month"].unique())
    if len(months) >= 2:
        last, prev = months[-1], months[-2]
        last_df = by_cat[by_cat["month"] == last]
        prev_df = by_cat[by_cat["month"] == prev]
        merged = last_df.merge(prev_df, on="category", how="left", suffixes=("_last", "_prev")).fillna(0.0)
        merged["delta_pct"] = merged.apply(
            lambda r: ((r["spend_last"] - r["spend_prev"]) / r["spend_prev"] * 100.0) if r["spend_prev"] > 0 else (100.0 if r["spend_last"] > 0 else 0.0),
            axis=1,
        )
        # Top increases
        inc = merged.sort_values("delta_pct", ascending=False).head(3)
        for _, row in inc.iterrows():
            if row["delta_pct"] >= 20.0 and row["spend_last"] >= 10.0:
                msg = f"Your '{row['category']}' spending is up {row['delta_pct']:.0f}% this month."
                meta = {
                    "month": str(last),
                    "category": row["category"],
                    "spend_last": float(row["spend_last"]),
                    "spend_prev": float(row["spend_prev"]),
                }
                messages.append((msg, meta))
    # Subscriptions heuristic: categories named 'Subscriptions' with >= 3 occurrences
    subs = df[df["category"].str.lower() == "subscriptions"]
    if not subs.empty:
        counts = subs["description"].str.upper().str.extract(r"^([A-Z0-9\.\- ]+)").fillna("UNKNOWN").value_counts()
        if len(counts.index) >= 3:
            msg = "You have multiple active subscriptions to review."
            messages.append((msg, {"top": counts.head(5).to_dict()}))
    return messages


async def generate_user_insights(ctx: dict[str, Any], user_id: str) -> dict:
    db = await get_service_db()
    repo = InsightRepo(db)
    rows = await _fetch_user_transactions(db, user_id=user_id, days=90)
    df = _build_dataframe(rows)
    messages = _generate_messages(df)
    created = 0
    for msg, meta in messages:
        await repo.create_insight(user_id=user_id, message=msg, insight_type="spend_trend", metadata=meta)
        created += 1
    await db.close()
    return {"created": created}


class WorkerSettings:
    functions = [generate_user_insights]
    redis_settings = RedisSettings.from_dsn(settings.REDIS_URL or "redis://localhost:6379")
    # Example daily schedule at 01:00
    cron_jobs = [cron(generate_user_insights, hour=1, minute=0)]


