"""Pydantic models for pipeline run operations."""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from adf_control_api.models.pipeline_models import PipelineRunModel


class RunQueryFilterModel(BaseModel):
    operand: str
    operator: str
    values: List[str] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)


class RunQueryRequestModel(BaseModel):
    last_updated_after: datetime = Field(..., alias="lastUpdatedAfter")
    last_updated_before: datetime = Field(..., alias="lastUpdatedBefore")
    filters: List[RunQueryFilterModel] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)


class ActivityRunModel(BaseModel):
    activity_run_id: Optional[str] = Field(None, alias="activityRunId")
    activity_name: Optional[str] = Field(None, alias="activityName")
    activity_type: Optional[str] = Field(None, alias="activityType")
    status: Optional[str] = None
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    input: Optional[dict] = None  # noqa: A003 - align with ADF naming
    output: Optional[dict] = None
    error: Optional[dict] = None

    model_config = ConfigDict(populate_by_name=True)


class ActivityRunsResponseModel(BaseModel):
    items: List[ActivityRunModel]


class RunSummaryItemModel(BaseModel):
    status: str
    count: int


class RunSummaryResponseModel(BaseModel):
    window_start: datetime = Field(..., alias="windowStart")
    window_end: datetime = Field(..., alias="windowEnd")
    totals: List[RunSummaryItemModel]

    model_config = ConfigDict(populate_by_name=True)


class FailedRunsResponseModel(BaseModel):
    runs: List[PipelineRunModel]

    model_config = ConfigDict(populate_by_name=True)
