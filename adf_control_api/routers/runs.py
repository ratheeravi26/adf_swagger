"""Pipeline run management routes."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import List

from azure.core.exceptions import AzureError
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from adf_control_api.core.azure_client import ADFClientFactory, get_adf_factory
from adf_control_api.core.config import Settings, get_settings
from adf_control_api.models.pipeline_models import PipelineRunModel
from adf_control_api.models.root_pipeline_model import RootPipelineRunModel
from adf_control_api.models.run_models import ActivityRunsResponseModel, RunQueryRequestModel
from adf_control_api.utils import adf_helpers

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/runs", tags=["Runs"])


@router.get("/{runId}", response_model=PipelineRunModel)
async def get_run_status(
    runId: str,
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info(
        "Fetching pipeline run status",
        extra={"runId": runId, "user": getattr(request.state, "user", None)},
    )

    if settings.use_mock_mode:
        return adf_helpers.mock_pipeline_run()

    def _get():
        try:
            client = factory.client
            run = client.pipeline_runs.get(
                settings.azure_resource_group,
                settings.adf_factory_name,
                runId,
            )
            return adf_helpers.serialize_pipeline_run(run)
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Unable to fetch run {runId}. Verify Azure credentials.",
            ) from exc

    return await run_in_threadpool(_get)


@router.post("/{runId}/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel_pipeline_run(
    runId: str,
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info(
        "Canceling pipeline run",
        extra={"runId": runId, "user": getattr(request.state, "user", None)},
    )

    if settings.use_mock_mode:
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"runId": runId, "status": "CancelRequested"},
        )

    def _cancel():
        try:
            client = factory.client
            client.pipeline_runs.cancel(
                settings.azure_resource_group,
                settings.adf_factory_name,
                runId,
            )
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Unable to cancel run {runId}. Verify Azure configuration.",
            ) from exc

    await run_in_threadpool(_cancel)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"runId": runId, "status": "CancelRequested"},
    )


@router.post("/query", response_model=List[PipelineRunModel])
async def query_pipeline_runs(
    payload: RunQueryRequestModel,
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info(
        "Querying pipeline runs",
        extra={
            "filters": [f.dict() for f in payload.filters],
            "user": getattr(request.state, "user", None),
        },
    )

    if settings.use_mock_mode:
        return [adf_helpers.mock_pipeline_run()]

    def _query():
        try:
            client = factory.client
            return adf_helpers.query_pipeline_runs(client, settings, payload)
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to query pipeline runs. Verify Azure credentials.",
            ) from exc

    return await run_in_threadpool(_query)


@router.post("/{runId}/activities", response_model=ActivityRunsResponseModel)
async def list_activity_runs(
    runId: str,
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info(
        "Listing activity runs",
        extra={"runId": runId, "user": getattr(request.state, "user", None)},
    )

    if settings.use_mock_mode:
        return adf_helpers.mock_activity_runs()

    def _list() -> ActivityRunsResponseModel:
        try:
            client = factory.client
            run = client.pipeline_runs.get(
                settings.azure_resource_group,
                settings.adf_factory_name,
                runId,
            )

            now = datetime.now(timezone.utc)
            start = getattr(run, "run_start", None) or now - timedelta(hours=settings.default_run_lookback_hours)
            end = getattr(run, "run_end", None) or now + timedelta(hours=1)
            if end <= start:
                end = start + timedelta(hours=1)

            return adf_helpers.list_activity_runs(client, settings, runId, start, end)
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Unable to list activity runs for {runId}. Verify Azure credentials.",
            ) from exc

    return await run_in_threadpool(_list)


@router.get("/{runId}/root", response_model=RootPipelineRunModel)
async def get_root_pipeline_run(
    runId: str,
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info(
        "Resolving root pipeline run",
        extra={"runId": runId, "user": getattr(request.state, "user", None)},
    )

    if settings.use_mock_mode:
        return adf_helpers.mock_root_pipeline_run()

    def _resolve():
        try:
            client = factory.client
            return adf_helpers.resolve_root_pipeline_run(client, settings, runId)
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Unable to resolve root run for {runId}. Verify Azure configuration.",
            ) from exc

    return await run_in_threadpool(_resolve)
