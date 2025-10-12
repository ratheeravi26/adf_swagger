"""Models for root pipeline responses."""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

from adf_control_api.models.pipeline_models import PipelineRunModel


class RootPipelineModel(BaseModel):
    pipeline_name: str = Field(..., alias="pipelineName")
    description: Optional[str] = None
    trigger_names: List[str] = Field(default_factory=list, alias="triggerNames")
    last_run: Optional[PipelineRunModel] = Field(None, alias="lastRun")

    model_config = ConfigDict(populate_by_name=True)


class RootPipelineRunModel(BaseModel):
    root_run_id: str = Field(..., alias="rootRunId")
    root_pipeline_name: str = Field(..., alias="rootPipelineName")
    trigger_type: Optional[str] = Field(None, alias="triggerType")
    trigger_name: Optional[str] = Field(None, alias="triggerName")

    model_config = ConfigDict(populate_by_name=True)
