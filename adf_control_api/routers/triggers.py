"""Trigger management routes."""

from __future__ import annotations

import logging

from azure.core.exceptions import AzureError
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from adf_control_api.core.azure_client import ADFClientFactory, get_adf_factory
from adf_control_api.core.config import Settings, get_settings
from adf_control_api.utils import adf_helpers

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/triggers", tags=["Triggers"])


@router.get("")
async def list_triggers(
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info("Listing triggers", extra={"user": getattr(request.state, "user", None)})

    if settings.use_mock_mode:
        return adf_helpers.mock_triggers()

    def _list():
        try:
            client = factory.client
            triggers = list(
                client.triggers.list_by_factory(settings.azure_resource_group, settings.adf_factory_name)
            )
            return [trigger.as_dict() for trigger in triggers]
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Unable to list triggers. Verify Azure credentials.",
            ) from exc

    return await run_in_threadpool(_list)


@router.post("/{triggerName}/start", status_code=status.HTTP_202_ACCEPTED)
async def start_trigger(
    triggerName: str,
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info(
        "Starting trigger",
        extra={"triggerName": triggerName, "user": getattr(request.state, "user", None)},
    )

    if settings.use_mock_mode:
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"triggerName": triggerName, "status": "StartRequested"},
        )

    def _start():
        try:
            client = factory.client
            client.triggers.start(
                settings.azure_resource_group,
                settings.adf_factory_name,
                triggerName,
            )
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Unable to start trigger {triggerName}. Verify Azure configuration.",
            ) from exc

    await run_in_threadpool(_start)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"triggerName": triggerName, "status": "StartRequested"},
    )


@router.post("/{triggerName}/stop", status_code=status.HTTP_202_ACCEPTED)
async def stop_trigger(
    triggerName: str,
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info(
        "Stopping trigger",
        extra={"triggerName": triggerName, "user": getattr(request.state, "user", None)},
    )

    if settings.use_mock_mode:
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"triggerName": triggerName, "status": "StopRequested"},
        )

    def _stop():
        try:
            client = factory.client
            client.triggers.stop(
                settings.azure_resource_group,
                settings.adf_factory_name,
                triggerName,
            )
        except AzureError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Unable to stop trigger {triggerName}. Verify Azure configuration.",
            ) from exc

    await run_in_threadpool(_stop)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"triggerName": triggerName, "status": "StopRequested"},
    )
