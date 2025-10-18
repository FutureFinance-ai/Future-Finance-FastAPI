
from __future__ import annotations

from typing import Dict, Any, List

import pandas as pd
import numpy as np
from functools import lru_cache
from typing import Optional, Tuple
from datetime import datetime

class AnalysisService():

  def __init__(self) -> None:
    pass

  async def transactions_to_dataframe(self, llm_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert LLM JSON output to a strictly typed DataFrame.
    Enforces dtypes and normalizes values to avoid calculation errors.
    """
    transactions: List[Dict[str, Any]] = llm_json.get("transactions", []) or []

    df = pd.DataFrame(transactions)

    if df.empty:
      return pd.DataFrame(
        columns=[
          "trans_time",
          "value_date",
          "description",
          "debit",
          "credit",
          "balance",
          "transaction_reference",
          "counterparty",
          "transaction_category",
        ]
      ).astype(
        {
          "trans_time": "datetime64[ns]",
          "value_date": "datetime64[ns]",
          "description": "string",
          "debit": "float64",
          "credit": "float64",
          "balance": "float64",
          "transaction_reference": "string",
          "counterparty": "string",
          "transaction_category": "string",
        }
      )

    # Normalize numeric fields: remove currency symbols and commas
    for col in ["debit", "credit", "balance"]:
      if col in df.columns:
        df[col] = (
          df[col]
          .astype("string")
          .str.replace("₦", "", regex=False)
          .str.replace("N", "", regex=False)
          .str.replace(",", "", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Enforce sign convention (vectorized)
    if "debit" in df.columns:
      df["debit"] = df["debit"].fillna(0.0).astype("float64")
      df["debit"] = -df["debit"].abs()
    else:
      df["debit"] = 0.0

    if "credit" in df.columns:
      df["credit"] = df["credit"].fillna(0.0).astype("float64")
      df["credit"] = df["credit"].abs()
    else:
      df["credit"] = 0.0

    # Standardize descriptions using regex (vectorized)
    if "description" in df.columns:
      df["description"] = (
        df["description"].astype("string")
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
      )

    # Initial rule-based categorization (vectorized)
    desc = df.get("description", pd.Series([], dtype="string")).fillna("").astype("string")
    conditions = [
      desc.str.contains(r"\bUBER\b", case=False, regex=True, na=False),
    ]
    choices = [
      "Transport",
    ]
    df["transaction_category"] = np.select(conditions, choices, default="Uncategorized").astype("string")

    # Parse date/time columns
    if "trans_time" in df.columns:
      df["trans_time"] = pd.to_datetime(df["trans_time"], errors="coerce")
    else:
      df["trans_time"] = pd.NaT

    if "value_date" in df.columns:
      df["value_date"] = pd.to_datetime(df["value_date"], errors="coerce")
    else:
      df["value_date"] = pd.NaT

    # Coerce text columns to pandas StringDtype
    for col in ["description", "transaction_reference", "counterparty"]:
      if col in df.columns:
        df[col] = df[col].astype("string")
      else:
        df[col] = pd.Series([pd.NA] * len(df), dtype="string")

    # Final dtype enforcement
    df = df.astype(
      {
        "trans_time": "datetime64[ns]",
        "value_date": "datetime64[ns]",
        "description": "string",
        "debit": "float64",
        "credit": "float64",
        "balance": "float64",
        "transaction_reference": "string",
        "counterparty": "string",
        "transaction_category": "string",
      }
    )

    # Sort chronologically by trans_time
    df = df.sort_values(by=["trans_time", "value_date"], kind="mergesort", na_position="last").reset_index(drop=True)

    return df

  async def calculate_monthly_cash_flow(self, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
      return pd.DataFrame(columns=["month", "credits", "debits", "net_cash_flow"]).astype({
        "month": "datetime64[ns]",
        "credits": "float64",
        "debits": "float64",
        "net_cash_flow": "float64",
      })
    s = df.copy()
    s["month"] = s["trans_time"].dt.to_period("M").dt.to_timestamp()
    agg = s.groupby("month").agg(
      credits=("credit", "sum"),
      debits=("debit", "sum"),
    ).reset_index()
    agg["debits"] = agg["debits"].abs()
    agg["net_cash_flow"] = agg["credits"] - agg["debits"]
    return agg.astype({
      "month": "datetime64[ns]",
      "credits": "float64",
      "debits": "float64",
      "net_cash_flow": "float64",
    })

  async def categorical_spend(self, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
      return pd.DataFrame(columns=["month", "transaction_category", "spend", "pct_of_total"]).astype({
        "month": "datetime64[ns]", "transaction_category": "string", "spend": "float64", "pct_of_total": "float64"
      })
    s = df.copy()
    s["month"] = s["trans_time"].dt.to_period("M").dt.to_timestamp()
    s["spend"] = s["debit"].abs()
    grp = s.groupby(["month", "transaction_category"], dropna=False)["spend"].sum().reset_index()
    total = grp.groupby("month")["spend"].transform("sum")
    grp["pct_of_total"] = (grp["spend"] / total).fillna(0.0) * 100.0
    return grp.astype({
      "month": "datetime64[ns]", "transaction_category": "string", "spend": "float64", "pct_of_total": "float64"
    })

  async def detect_recurring(self, df: pd.DataFrame, amount_tol: float = 1.0) -> pd.DataFrame:
    if df.empty:
      return pd.DataFrame(columns=["keyword", "months", "count", "avg_amount", "is_fee"]).astype({
        "keyword": "string", "months": "string", "count": "int64", "avg_amount": "float64", "is_fee": "bool"
      })
    s = df.copy()
    s["desc_norm"] = s["description"].str.replace(r"\s+", " ", regex=True).str.upper().str.strip()
    # simple keyword extraction: first word token (placeholder heuristic)
    s["keyword"] = s["desc_norm"].str.extract(r"^([A-Z0-9]+)")
    s["month"] = s["trans_time"].dt.to_period("M").dt.to_timestamp()
    # consider debits only for subscriptions/fees
    s["amount_abs"] = s["debit"].abs()
    grp = s.groupby(["keyword", "month"]).agg(count=("amount_abs", "size"), avg_amount=("amount_abs", "mean")).reset_index()
    cadence = grp.groupby("keyword").agg(
      count=("month", "nunique"),
      months=("month", lambda x: ",".join(sorted({str(m) for m in x}))),
      avg_amount=("avg_amount", "mean"),
    ).reset_index()
    cadence["is_fee"] = cadence["keyword"].str.contains(r"FEE|CHARGE|MAINT|VAT|SMS", case=False, regex=True).fillna(False)
    return cadence.fillna({"keyword": "", "months": ""}).astype({
      "keyword": "string", "months": "string", "count": "int64", "avg_amount": "float64", "is_fee": "bool"
    })

  async def compare_to_budget(self, df: pd.DataFrame, budgets: pd.DataFrame) -> pd.DataFrame:
    if df.empty or budgets is None or budgets.empty:
      return pd.DataFrame(columns=["month", "transaction_category", "actual", "budget", "variance", "pct_variance"]).astype({
        "month": "datetime64[ns]", "transaction_category": "string", "actual": "float64", "budget": "float64", "variance": "float64", "pct_variance": "float64"
      })
    actual = self.categorical_spend(df).rename(columns={"spend": "actual"})[["month", "transaction_category", "actual"]]
    merged = actual.merge(budgets, on=["month", "transaction_category"], how="left")
    merged["budget"] = merged["budget"].fillna(0.0)
    merged["variance"] = merged["actual"] - merged["budget"]
    merged["pct_variance"] = np.where(merged["budget"] != 0, merged["variance"] / merged["budget"] * 100.0, np.nan)
    return merged.astype({
      "month": "datetime64[ns]", "transaction_category": "string", "actual": "float64", "budget": "float64", "variance": "float64", "pct_variance": "float64"
    })

  async def forecast_cash_flow(self, df: pd.DataFrame, periods: int = 6) -> pd.DataFrame:
    try:
      from prophet import Prophet
    except Exception:
      # Return empty frame if Prophet is not available
      return pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"]).astype({
        "ds": "datetime64[ns]", "yhat": "float64", "yhat_lower": "float64", "yhat_upper": "float64"
      })
    monthly = self.calculate_monthly_cash_flow(df)
    if monthly.empty:
      return pd.DataFrame(columns=["ds", "yhat", "yhat_lower", "yhat_upper"]).astype({
        "ds": "datetime64[ns]", "yhat": "float64", "yhat_lower": "float64", "yhat_upper": "float64"
      })
    train = monthly.rename(columns={"month": "ds", "net_cash_flow": "y"})[["ds", "y"]]
    model = Prophet(seasonality_mode="additive", yearly_seasonality=True)
    model.fit(train)
    future = model.make_future_dataframe(periods=periods, freq="MS")
    forecast = model.predict(future)
    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].astype({
      "ds": "datetime64[ns]", "yhat": "float64", "yhat_lower": "float64", "yhat_upper": "float64"
    })



@lru_cache(maxsize=1)
def get_analysis_service() -> "AnalysisService":
  return AnalysisService()

  async def generate_monthly_narrative(self, df: pd.DataFrame) -> str:
    monthly = await self.calculate_monthly_cash_flow(df)
    categories = await self.categorical_spend(df)
    recurring = await self.detect_recurring(df)

    if monthly.empty:
      return "# Monthly Financial Summary\n\nNo transactions available for this period."

    last = monthly.sort_values("month").iloc[-1]
    net = last["net_cash_flow"]

    top_cat = None
    if not categories.empty:
      last_month = categories[categories["month"] == last["month"]]
      if not last_month.empty:
        top_cat_row = last_month.sort_values("spend", ascending=False).iloc[0]
        top_cat = (top_cat_row["transaction_category"], float(top_cat_row["spend"]))

    new_recurring = None
    if not recurring.empty:
      r = recurring.sort_values(["count", "avg_amount"], ascending=[False, False]).iloc[0]
      new_recurring = (str(r["keyword"]), float(r["avg_amount"]))

    lines = [
      "# Monthly Financial Summary",
      f"This month, your net cash flow was {net:+.2f}.",
    ]

    if top_cat is not None:
      lines.append(f"Top spending category: {top_cat[0]} at {top_cat[1]:.2f}.")

    if new_recurring is not None and new_recurring[0]:
      lines.append(f"Alert: A recurring pattern detected for '{new_recurring[0]}' at approximately {new_recurring[1]:.2f}.")

    lines.append("\nTips: Consider reviewing recurring subscriptions and refining budgets for high-variance categories.")

    return "\n\n".join(lines)