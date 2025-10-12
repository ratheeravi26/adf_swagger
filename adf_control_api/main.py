"""FastAPI application entrypoint for the ADF Control API."""

from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from adf_control_api.core.auth import AzureADAuthMiddleware
from adf_control_api.core.config import get_settings
from adf_control_api.routers import infra, monitoring, pipelines, runs, triggers
from adf_control_api.utils.logging_config import setup_logging

settings = get_settings()
setup_logging(settings)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Azure Data Factory Control API",
    description="Management endpoints for controlling Azure Data Factory pipelines.",
    version="1.0.0",
    swagger_ui_init_oauth={
        "clientId": settings.azure_client_id,
        "usePkceWithAuthorizationCodeGrant": True,
    },
    swagger_ui_oauth2_redirect_url="/docs/oauth2-redirect",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)
app.add_middleware(AzureADAuthMiddleware, settings=settings)

app.include_router(pipelines.router)
app.include_router(runs.router)
app.include_router(triggers.router)
app.include_router(infra.router)
app.include_router(monitoring.router)


@app.on_event("startup")
async def startup_event() -> None:
    logger.info(
        "ADF Control API initialized and connected to Azure Data Factory: %s",
        settings.adf_factory_name,
    )


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "name": "Azure Data Factory Control API",
        "status": "ok",
        "docs": "/docs",
    }


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        routes=app.routes,
        description=app.description,
    )
    components = openapi_schema.setdefault("components", {})
    security_schemes = components.setdefault("securitySchemes", {})

    if settings.use_mock_mode:
        security_schemes["bearerAuth"] = {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "Paste any bearer token for mock mode (e.g., 'mock-token').",
        }
        openapi_schema["security"] = [{"bearerAuth": []}]
    else:
        token_url = f"https://login.microsoftonline.com/{settings.azure_tenant_id}/oauth2/v2.0/token"
        security_schemes["azureAD"] = {
            "type": "oauth2",
            "description": "Azure AD OAuth2 client credentials",
            "flows": {
                "clientCredentials": {
                    "tokenUrl": token_url,
                    "scopes": {
                        "api.readwrite": "Full API access for authorized clients.",
                    },
                }
            },
        }
        openapi_schema["security"] = [{"azureAD": ["api.readwrite"]}]

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi  # type: ignore[assignment]


if __name__ == "__main__":
    import os

    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("adf_control_api.main:app", host="0.0.0.0", port=port, reload=False)
