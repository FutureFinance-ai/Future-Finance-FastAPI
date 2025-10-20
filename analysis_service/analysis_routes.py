from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from typing import Optional

from analysis_service.analysis_service import get_analysis_service, AnalysisService
from analysis_service.visuals import cashflow_line, category_bar, cashflow_sankey, figure_to_response
import pandas as pd



router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.get("/monthly-cashflow")
async def monthly_cashflow(service: AnalysisService = Depends(get_analysis_service)):
    # In a real system, df would come from persisted normalized data; using placeholder
    # Here we return empty structure if not available
    df = pd.DataFrame([])
    result = await service.calculate_monthly_cash_flow(df)
    return result.to_dict(orient="records")


@router.get("/category-spend")
async def category_spend(service: AnalysisService = Depends(get_analysis_service)):
    df = pd.DataFrame([])
    result = await service.categorical_spend(df)
    return result.to_dict(orient="records")


@router.get("/recurring")
async def recurring(service: AnalysisService = Depends(get_analysis_service)):
    df = pd.DataFrame([])
    result = await service.detect_recurring(df)
    return result.to_dict(orient="records")


@router.get("/forecast")
async def forecast(periods: int = Query(default=6, ge=1, le=24), service: AnalysisService = Depends(get_analysis_service)):
    df = pd.DataFrame([])
    result = await service.forecast_cash_flow(df, periods=periods)
    return result.to_dict(orient="records")


@router.get("/narrative")
async def narrative(service: AnalysisService = Depends(get_analysis_service)):
    df = pd.DataFrame([])
    text = await service.generate_monthly_narrative(df)
    return {"markdown": text}


@router.get("/cashflow-figure")
async def cashflow_figure(format: str = Query(default="json", pattern="^(json|png)$"), service: AnalysisService = Depends(get_analysis_service)):
    # assemble monthly first
    m = await service.calculate_monthly_cash_flow(pd.DataFrame([]))
    fig = await cashflow_line(m)
    payload = await figure_to_response(fig, fmt=format)  # bytes for png, dict for json
    return payload


@router.get("/sankey")
async def sankey(format: str = Query(default="json", pattern="^(json|png)$")):
    df = pd.DataFrame([])
    fig = await cashflow_sankey(df)
    payload = await figure_to_response(fig, fmt=format)
    return payload


