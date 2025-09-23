

import pandas as pd
from collections import defaultdict
from typing import List, Dict
from datetime import datetime

class SpendingAnalyzer:
    def __init__(self, transactions: List[CategorizedTransaction]):
        # Convert to DataFrame for easy analysis
        self.df = pd.DataFrame([t.__dict__ for t in transactions])
        if not self.df.empty:
            self.df['date'] = pd.to_datetime(self.df['date'], errors="coerce")

    def monthly_summary(self) -> pd.DataFrame:
        """Monthly totals for income, expenses, and net savings."""
        if self.df.empty:
            return pd.DataFrame()
        monthly = self.df.groupby([self.df['date'].dt.to_period("M"), "type"])["amount"].sum().unstack(fill_value=0)
        monthly["net_savings"] = monthly.get("credit", 0) - monthly.get("debit", 0)
        return monthly.reset_index().rename(columns={"date": "month"})

    def category_breakdown(self) -> pd.DataFrame:
        """Total spend by category/subcategory."""
        if self.df.empty:
            return pd.DataFrame()
        cat_summary = self.df.groupby(["category", "subcategory"])["amount"].sum().reset_index()
        return cat_summary.sort_values("amount", ascending=False)

    def top_merchants(self, n: int = 10) -> pd.DataFrame:
        """Finds top merchants/descriptions where money was spent."""
        if self.df.empty:
            return pd.DataFrame()
        desc_summary = self.df[self.df["type"] == "debit"].groupby("description")["amount"].sum().reset_index()
        return desc_summary.sort_values("amount", ascending=False).head(n)

    def spending_trends(self) -> pd.DataFrame:
        """Daily spend trends."""
        if self.df.empty:
            return pd.DataFrame()
        return self.df.groupby(self.df["date"].dt.to_period("D"))["amount"].sum().reset_index()

    def summary_stats(self) -> Dict[str, float]:
        """Quick overview of spending habits."""
        if self.df.empty:
            return {}
        total_income = self.df[self.df["type"] == "credit"]["amount"].sum()
        total_expenses = self.df[self.df["type"] == "debit"]["amount"].sum()
        avg_daily_expense = self.df[self.df["type"] == "debit"].groupby(self.df["date"].dt.date)["amount"].sum().mean()
        return {
            "total_income": round(total_income, 2),
            "total_expenses": round(total_expenses, 2),
            "net_savings": round(total_income - total_expenses, 2),
            "avg_daily_expense": round(avg_daily_expense, 2),
        }
