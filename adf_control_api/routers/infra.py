"""Integration runtime and linked services routes."""

from __future__ import annotations

import logging

from azure.core.exceptions import AzureError
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

from adf_control_api.core.azure_client import ADFClientFactory, get_adf_factory
from adf_control_api.core.config import Settings, get_settings
from adf_control_api.utils import adf_helpers

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Infrastructure"])


@router.get("/integrationRuntimes")
async def list_integration_runtimes(
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info("Listing integration runtimes", extra={"user": getattr(request.state, "user", None)})

    if settings.use_mock_mode:
        return adf_helpers.mock_integration_runtimes()

    def _list():
        try:
            client = factory.client
            runtimes = list(
                client.integration_runtimes.list_by_factory(
                    settings.azure_resource_group, settings.adf_factory_name
                )
            )
            return [runtime.as_dict() for runtime in runtimes]
        except AzureError as exc:
            raise HTTPException(
                status_code=503,
                detail="Unable to list integration runtimes. Verify Azure credentials.",
            ) from exc

    return await run_in_threadpool(_list)


@router.get("/integrationRuntimes/{irName}/status")
async def get_integration_runtime_status(
    irName: str,
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info(
        "Fetching integration runtime status",
        extra={"integrationRuntime": irName, "user": getattr(request.state, "user", None)},
    )

    if settings.use_mock_mode:
        return adf_helpers.mock_integration_runtime_status(irName)

    def _status():
        try:
            client = factory.client
            status = client.integration_runtimes.get_status(
                settings.azure_resource_group,
                settings.adf_factory_name,
                irName,
            )
            return status.as_dict()
        except AzureError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Unable to retrieve status for integration runtime {irName}.",
            ) from exc

    return await run_in_threadpool(_status)


@router.get("/linkedServices")
async def list_linked_services(
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info("Listing linked services", extra={"user": getattr(request.state, "user", None)})

    if settings.use_mock_mode:
        return adf_helpers.mock_linked_services()

    def _list():
        try:
            client = factory.client
            services = list(
                client.linked_services.list_by_factory(settings.azure_resource_group, settings.adf_factory_name)
            )
            return [service.as_dict() for service in services]
        except AzureError as exc:
            raise HTTPException(
                status_code=503,
                detail="Unable to list linked services. Verify Azure credentials.",
            ) from exc

    return await run_in_threadpool(_list)


@router.get("/datasets")
async def list_datasets(
    request: Request,
    factory: ADFClientFactory = Depends(get_adf_factory),
    settings: Settings = Depends(get_settings),
):
    logger.info("Listing datasets", extra={"user": getattr(request.state, "user", None)})

    if settings.use_mock_mode:
        return adf_helpers.mock_datasets()

    def _list():
        try:
            client = factory.client
            datasets = list(
                client.datasets.list_by_factory(settings.azure_resource_group, settings.adf_factory_name)
            )
            return [dataset.as_dict() for dataset in datasets]
        except AzureError as exc:
            raise HTTPException(
                status_code=503,
                detail="Unable to list datasets. Verify Azure credentials.",
            ) from exc

    return await run_in_threadpool(_list)
