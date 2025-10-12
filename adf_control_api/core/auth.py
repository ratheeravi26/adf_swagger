"""Azure AD authentication middleware."""

from __future__ import annotations

import base64
import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, Request, status
from fastapi.security.utils import get_authorization_scheme_param
from fastapi.concurrency import run_in_threadpool
from msal import ConfidentialClientApplication
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from adf_control_api.core.config import Settings, get_settings

logger = logging.getLogger(__name__)


def _decode_token_claims(token: str) -> Dict[str, Any]:
    """Decode JWT payload without verifying signature (validation already performed)."""
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    decoded = base64.urlsafe_b64decode(payload + padding)
    return json.loads(decoded.decode("utf-8"))


class AzureADTokenValidator:
    """Validate bearer tokens using MSAL on-behalf-of flow."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._app: Optional[ConfidentialClientApplication] = None
        self._scopes: List[str] = []

        if not self._settings.auth_use_mock:
            authority = f"https://login.microsoftonline.com/{self._settings.azure_tenant_id}"
            self._app = ConfidentialClientApplication(
                client_id=self._settings.azure_client_id,
                authority=authority,
                client_credential=self._settings.azure_client_secret,
            )

            scope = self._settings.auth_scope or f"api://{self._settings.azure_client_id}/.default"
            self._scopes = [scope]

    def validate(self, token: str) -> Dict[str, Any]:
        if self._settings.auth_use_mock:
            logger.debug("Auth in mock mode; bypassing Azure AD validation.")
            return {"sub": "mock-user", "scp": "mock", "aud": "mock"}

        if self._app is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Authentication service is not configured.",
            )

        result = self._app.acquire_token_on_behalf_of(token, scopes=self._scopes)
        if "access_token" not in result:
            logger.error(
                "Azure AD token validation failed",
                extra={"error": result.get("error"), "error_description": result.get("error_description")},
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired access token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        claims = _decode_token_claims(token)

        expected_audience = self._settings.auth_audience
        if expected_audience and claims.get("aud") != expected_audience:
            logger.warning(
                "Token audience mismatch",
                extra={"expected_audience": expected_audience, "token_audience": claims.get("aud")},
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token audience",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return claims


class AzureADAuthMiddleware(BaseHTTPMiddleware):
    """Protect routes by requiring a valid Azure AD bearer token."""

    def __init__(
        self,
        app,
        settings: Settings | None = None,
        validator: Optional[AzureADTokenValidator] = None,
        exempt_paths: Optional[List[str]] = None,
    ) -> None:
        super().__init__(app)
        self._settings = settings or get_settings()
        self._validator = validator or AzureADTokenValidator(self._settings)
        default_exempt = set(
            exempt_paths
            or [
                "/",
                "/openapi.json",
                "/docs",
                "/docs/index.html",
                "/docs/oauth2-redirect",
            ]
        )

        if self._settings.use_mock_mode:
            # In mock mode we allow the built-in swagger to function without auth prompts.
            default_exempt.add("swagger_ui")

        self._exempt_paths = default_exempt

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path
        if path in self._exempt_paths:
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        scheme, token = get_authorization_scheme_param(auth_header)
        if scheme.lower() != "bearer" or not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing or invalid Authorization header",
                headers={"WWW-Authenticate": "Bearer"},
            )

        claims = await run_in_threadpool(self._validator.validate, token)
        request.state.user = claims

        return await call_next(request)


_middleware_instance: Optional[AzureADAuthMiddleware] = None


def get_auth_middleware(app, settings: Settings | None = None) -> AzureADAuthMiddleware:
    """Return a singleton middleware instance for reuse (especially during reload)."""
    global _middleware_instance
    if _middleware_instance is None:
        _middleware_instance = AzureADAuthMiddleware(app, settings=settings)
    return _middleware_instance
