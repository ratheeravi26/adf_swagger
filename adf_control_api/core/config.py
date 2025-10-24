"""Application configuration management."""

from __future__ import annotations

from functools import lru_cache
from typing import Dict, List, Optional

from dotenv import load_dotenv
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load values from .env if present
load_dotenv()


class Settings(BaseSettings):
    """Central configuration object backed by environment variables."""

    azure_subscription_id: str = Field(..., env="AZURE_SUBSCRIPTION_ID")
    azure_resource_group: str = Field(..., env="AZURE_RESOURCE_GROUP")
    adf_factory_name: str = Field(..., env="ADF_FACTORY_NAME")

    azure_tenant_id: str = Field(..., env="AZURE_TENANT_ID")
    azure_client_id: str = Field(..., env="AZURE_CLIENT_ID")
    azure_client_secret: str = Field(..., env="AZURE_CLIENT_SECRET")

    azure_environment: str = Field("public", env="AZURE_ENVIRONMENT")
    adf_base_url: Optional[str] = Field(None, env="ADF_BASE_URL")

    log_level: str = Field("INFO", env="LOG_LEVEL")
    allow_origins: str = Field("*", env="ALLOW_ORIGINS")

    auth_audience: Optional[str] = Field(None, env="AUTH_AUDIENCE")
    auth_scope: Optional[str] = Field(None, env="AUTH_SCOPE")
    auth_use_mock: bool = Field(False, env="AUTH_USE_MOCK")
    auth_auto_client: bool = Field(False, env="AUTH_AUTO_CLIENT")

    default_run_lookback_hours: int = Field(24, env="DEFAULT_RUN_LOOKBACK_HOURS")
    azure_mock_mode: bool = Field(False, env="ADF_MOCK_MODE")

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")
    _mock_mode_override: Optional[bool] = None

    @property
    def management_endpoint(self) -> str:
        """Return the Azure Resource Manager endpoint for the selected cloud."""
        if self.adf_base_url:
            return self.adf_base_url.rstrip("/")

        cloud = self.azure_environment.lower()
        endpoints: Dict[str, str] = {
            "public": "https://management.azure.com",
            "gov": "https://management.usgovcloudapi.net",
            "usgov": "https://management.usgovcloudapi.net",
            "govcloud": "https://management.usgovcloudapi.net",
        }
        return endpoints.get(cloud, endpoints["public"])

    @property
    def credential_scope(self) -> str:
        """Scope used when fetching ARM access tokens."""
        return f"{self.management_endpoint}/.default"

    @property
    def allowed_origins(self) -> List[str]:
        """Return allowed CORS origins."""
        if not self.allow_origins:
            return ["*"]
        # Support comma-separated origin list
        return [origin.strip() for origin in self.allow_origins.split(",") if origin.strip()]

    @property
    def effective_auth_scope(self) -> str:
        """Scope used for Azure AD client credential flows."""
        if self.auth_scope:
            return self.auth_scope
        return f"{self.management_endpoint}/.default"

    @field_validator("azure_environment")
    def _normalize_env(cls, value: str) -> str:
        return value.lower().strip()

    @property
    def use_mock_mode(self) -> bool:
        """Return True when mock responses should be served."""
        if self._mock_mode_override is not None:
            return self._mock_mode_override
        return bool(self.azure_mock_mode or self.auth_use_mock)

    def override_mock_mode(self, value: bool) -> None:
        """Allow tests to override mock mode behavior."""
        self._mock_mode_override = value


@lru_cache()
def get_settings() -> Settings:
    """Return cached Settings instance."""
    return Settings()
