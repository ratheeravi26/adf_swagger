"""Pipeline-related API routes."""

from __future__ import annotations

import logging
from typing import List

from azure.core.exceptions import AzureError
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool

from adf_control_api.core.azure_client import ADFClientFactory, get_adf_factory
from adf_control_api.core.config import Settings, get_settings
from adf_control_api.models.pipeline_models import PipelineRunModel, PipelineRunParameters, PipelineSummaryModel
from adf_control_api.models.root_pipeline_model import RootPipelineModel
from adf_control_api.utils import adf_helpers

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pipelines", tags=["Pipelines"])


@router.get("", response_model=List[PipelineSummaryModel])
async def list_pipelines(
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info("Listing pipelines", extra={"user": getattr(request.state, "user", None)})

    if settings.use_mock_mode:
        return adf_helpers.mock_pipeline_summaries()

    def _list():
        try:
            client = factory.client
            pipelines = list(
                client.pipelines.list_by_factory(settings.azure_resource_group, settings.adf_factory_name)
            )
            return [adf_helpers.serialize_pipeline_resource(p) for p in pipelines]
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to contact Azure Data Factory. Verify credentials and network connectivity.",
            ) from exc

    return await run_in_threadpool(_list)


@router.get("/{pipelineName}")
async def get_pipeline_definition(
    pipelineName: str,
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info(
        "Fetching pipeline definition",
        extra={"pipelineName": pipelineName, "user": getattr(request.state, "user", None)},
    )

    if settings.use_mock_mode:
        return adf_helpers.mock_pipeline_definition(pipelineName)

    def _get():
        try:
            client = factory.client
            pipeline = client.pipelines.get(
                settings.azure_resource_group,
                settings.adf_factory_name,
                pipelineName,
            )
            return pipeline.as_dict()
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to contact Azure Data Factory. Verify credentials and network connectivity.",
            ) from exc

    return await run_in_threadpool(_get)


@router.post("/{pipelineName}/run", response_model=PipelineRunModel, status_code=status.HTTP_202_ACCEPTED)
async def trigger_pipeline_run(
    pipelineName: str,
    request: Request,
    parameters: PipelineRunParameters | None = None,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info(
        "Triggering pipeline run",
        extra={
            "pipelineName": pipelineName,
            "user": getattr(request.state, "user", None) if request else None,
        },
    )

    if settings.use_mock_mode:
        return adf_helpers.mock_pipeline_run(pipelineName)

    parameter_dict = parameters.to_parameters() if parameters else {}

    def _trigger():
        try:
            client = factory.client
            response = client.pipelines.create_run(
                settings.azure_resource_group,
                settings.adf_factory_name,
                pipelineName,
                parameters=parameter_dict or None,
            )
            # Return minimal run info; detailed status can be queried separately
            return PipelineRunModel(
                runId=response.run_id,
                pipelineName=pipelineName,
                status="Queued",
                startTime=None,
                endTime=None,
            )
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to trigger pipeline run. Verify Azure credentials and permissions.",
            ) from exc

    return await run_in_threadpool(_trigger)


@router.get("/root", response_model=List[RootPipelineModel])
async def list_root_pipelines(
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info("Listing root pipelines", extra={"user": getattr(request.state, "user", None)})

    if settings.use_mock_mode:
        return adf_helpers.mock_root_pipelines()

    def _collect():
        try:
            client = factory.client
            return adf_helpers.get_root_pipelines(
                client, settings, lookback_hours=settings.default_run_lookback_hours
            )
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to retrieve root pipelines. Verify Azure configuration.",
            ) from exc

    root_pipelines = await run_in_threadpool(_collect)
    return root_pipelines
