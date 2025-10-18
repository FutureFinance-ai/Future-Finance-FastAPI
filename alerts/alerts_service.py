from __future__ import annotations

from typing import List, Dict, Any
import pandas as pd


class AlertsService:
    async def generate_alerts(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        alerts: List[Dict[str, Any]] = []
        if df.empty:
            return alerts

        # New recurring charges
        desc = df.get("description", pd.Series([], dtype="string")).fillna("").astype("string")
        recurring_mask = desc.str.contains(r"SUBSCRIPTION|RECURRING|MONTHLY|AUTO-RENEW", case=False, regex=True, na=False)
        recurring_rows = df[recurring_mask]
        for _, row in recurring_rows.iterrows():
            alerts.append({
                "type": "recurring",
                "message": f"New recurring pattern detected: '{row.get('description', '')}'",
                "severity": "info",
            })

        # Large transaction alert (simple rule)
        large_threshold = float(df.get("credit", pd.Series([0.0])).abs().max()) * 0.5
        large_debits = df[df["debit"].abs() > max(large_threshold, 1.0)]
        for _, row in large_debits.iterrows():
            alerts.append({
                "type": "large_debit",
                "message": f"Large debit detected: {float(abs(row['debit'])):.2f} - {row.get('description', '')}",
                "severity": "warning",
            })

        return alerts

