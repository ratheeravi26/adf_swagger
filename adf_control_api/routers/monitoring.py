"""Monitoring and summary routes."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from azure.core.exceptions import AzureError
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

from azure.mgmt.datafactory.models import RunFilterParameters, RunQueryFilter, RunQueryOrder, RunQueryOrderBy

from adf_control_api.core.azure_client import ADFClientFactory, get_adf_factory
from adf_control_api.core.config import Settings, get_settings
from adf_control_api.models.run_models import FailedRunsResponseModel, RunSummaryResponseModel
from adf_control_api.utils import adf_helpers

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/runs", tags=["Monitoring"])


@router.get("/failed", response_model=FailedRunsResponseModel)
async def list_failed_runs(
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info("Listing failed pipeline runs", extra={"user": getattr(request.state, "user", None)})

    if settings.use_mock_mode:
        return FailedRunsResponseModel(runs=adf_helpers.mock_failed_runs())

    def _list() -> FailedRunsResponseModel:
        try:
            client = factory.client
            now = datetime.now(timezone.utc)
            window_start = now - timedelta(hours=settings.default_run_lookback_hours)

            parameters = RunFilterParameters(
                last_updated_after=window_start,
                last_updated_before=now,
                filters=[
                    RunQueryFilter(operand="Status", operator="Equals", values=["Failed"]),
                ],
                order_by=[RunQueryOrderBy(order_by="RunStart", order=RunQueryOrder.descending)],
            )
            result = client.pipeline_runs.query_by_factory(
                settings.azure_resource_group,
                settings.adf_factory_name,
                parameters,
            )
            runs = getattr(result, "value", []) or []
            models = [adf_helpers.serialize_pipeline_run(run) for run in runs]
            return FailedRunsResponseModel(runs=models)
        except AzureError as exc:
            raise HTTPException(
                status_code=503,
                detail="Unable to retrieve failed runs. Verify Azure configuration.",
            ) from exc

    return await run_in_threadpool(_list)


@router.get("/summary", response_model=RunSummaryResponseModel)
async def get_run_summary(
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info("Generating run summary", extra={"user": getattr(request.state, "user", None)})

    if settings.use_mock_mode:
        runs = adf_helpers.mock_failed_runs()
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(hours=settings.default_run_lookback_hours)
        return adf_helpers.compute_run_summary(runs, window_start, now)

    def _summary() -> RunSummaryResponseModel:
        try:
            client = factory.client
            now = datetime.now(timezone.utc)
            window_start = now - timedelta(hours=settings.default_run_lookback_hours)

            parameters = RunFilterParameters(
                last_updated_after=window_start,
                last_updated_before=now,
                order_by=[RunQueryOrderBy(order_by="RunStart", order=RunQueryOrder.descending)],
            )
            result = client.pipeline_runs.query_by_factory(
                settings.azure_resource_group,
                settings.adf_factory_name,
                parameters,
            )
            runs = getattr(result, "value", []) or []
            run_models = [adf_helpers.serialize_pipeline_run(run) for run in runs]
            return adf_helpers.compute_run_summary(run_models, window_start, now)
        except AzureError as exc:
            raise HTTPException(
                status_code=503,
                detail="Unable to generate run summary. Verify Azure configuration.",
            ) from exc

    return await run_in_threadpool(_summary)
