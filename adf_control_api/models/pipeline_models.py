"""Pydantic models for pipeline operations."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, RootModel


class PipelineRunModel(BaseModel):
    run_id: str = Field(..., alias="runId")
    pipeline_name: str = Field(..., alias="pipelineName")
    status: Optional[str] = None
    start_time: Optional[datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime] = Field(None, alias="endTime")

    model_config = ConfigDict(populate_by_name=True)


class PipelineRunParameters(RootModel[Dict[str, Any]]):
    """Request body wrapper for pipeline parameters."""

    root: Dict[str, Any] = Field(default_factory=dict)

    def to_parameters(self) -> Dict[str, Any]:
        return self.root


class PipelineSummaryModel(BaseModel):
    name: str
    description: Optional[str] = None
    annotations: List[str] = Field(default_factory=list)
    etag: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)
