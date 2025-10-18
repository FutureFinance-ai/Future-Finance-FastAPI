from __future__ import annotations

from typing import List, Dict
import pandas as pd


class NetWorthService:
    def aggregate(self, assets: pd.DataFrame, liabilities: pd.DataFrame) -> pd.DataFrame:
        if assets is None:
            assets = pd.DataFrame(columns=["date", "value"]).astype({"date": "datetime64[ns]", "value": "float64"})
        if liabilities is None:
            liabilities = pd.DataFrame(columns=["date", "value"]).astype({"date": "datetime64[ns]", "value": "float64"})
        a = assets.groupby("date")["value"].sum().rename("assets")
        l = liabilities.groupby("date")["value"].sum().rename("liabilities")
        df = pd.concat([a, l], axis=1).fillna(0.0).reset_index()
        df["net_worth"] = df["assets"] - df["liabilities"]
        return df.astype({"date": "datetime64[ns]", "assets": "float64", "liabilities": "float64", "net_worth": "float64"})


