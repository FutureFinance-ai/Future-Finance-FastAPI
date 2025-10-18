from __future__ import annotations

from fastapi import APIRouter
from typing import List, Dict, Any
import pandas as pd

from alerts.alerts_service import AlertsService


router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("/")
async def list_alerts() -> List[Dict[str, Any]]:
    df = pd.DataFrame([])
    alerts = await AlertsService().generate_alerts(df)
    return alerts


@router.post("/ack/{alert_id}")
async def acknowledge_alert(alert_id: str) -> Dict[str, str]:
    # Placeholder acknowledge
    return {"status": "acknowledged", "id": alert_id}


