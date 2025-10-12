"""Azure Data Factory client helpers."""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Dict, Optional

import requests
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

from adf_control_api.core.config import Settings, get_settings


class ADFClientFactory:
    """Factory for Azure Data Factory SDK and REST clients."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
        self._client: Optional[DataFactoryManagementClient] = None

    @property
    def credential(self) -> DefaultAzureCredential:
        return self._credential

    @property
    def client(self) -> DataFactoryManagementClient:
        if self._client is None:
            self._client = DataFactoryManagementClient(
                credential=self._credential,
                subscription_id=self._settings.azure_subscription_id,
                base_url=self._settings.management_endpoint,
                credential_scopes=[self._settings.credential_scope],
            )
        return self._client

    def get_rest_headers(self) -> Dict[str, str]:
        """Return authorization headers for direct REST calls."""
        token = self._credential.get_token(self._settings.credential_scope)
        return {
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json",
        }

    def rest_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Perform a direct REST GET call against the management endpoint."""
        base_url = self._settings.management_endpoint.rstrip("/")
        url = f"{base_url}{path}"
        response = requests.get(url, headers=self.get_rest_headers(), params=params, timeout=30)
        response.raise_for_status()
        return response.json()


@lru_cache()
def get_adf_factory() -> ADFClientFactory:
    """Return cached client factory for dependency injection."""
    return ADFClientFactory()

