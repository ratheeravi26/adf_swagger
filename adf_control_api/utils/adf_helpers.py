"""Utility helpers for Azure Data Factory interactions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import ActivityRun, PipelineRun, RunFilterParameters, RunQueryFilter, RunQueryOrder, RunQueryOrderBy, TriggerResource

from adf_control_api.core.config import Settings
from adf_control_api.models.pipeline_models import PipelineRunModel, PipelineSummaryModel
from adf_control_api.models.root_pipeline_model import RootPipelineModel, RootPipelineRunModel
from adf_control_api.models.run_models import ActivityRunModel, ActivityRunsResponseModel, RunQueryRequestModel, RunSummaryItemModel, RunSummaryResponseModel


def _ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def serialize_pipeline_run(run: PipelineRun) -> PipelineRunModel:
    return PipelineRunModel(
        runId=run.run_id,
        pipelineName=run.pipeline_name,
        status=getattr(run, "status", None),
        startTime=_ensure_utc(getattr(run, "run_start", None)),
        endTime=_ensure_utc(getattr(run, "run_end", None)),
    )


def serialize_pipeline_resource(resource) -> PipelineSummaryModel:
    description = getattr(resource, "description", None)
    if description is None and getattr(resource, "properties", None):
        description = getattr(resource.properties, "description", None)

    annotations: List[str] = []
    if getattr(resource, "annotations", None):
        annotations = list(resource.annotations)  # type: ignore[arg-type]
    elif getattr(resource, "properties", None) and getattr(resource.properties, "annotations", None):
        annotations = list(resource.properties.annotations)  # type: ignore[arg-type]

    return PipelineSummaryModel(
        name=getattr(resource, "name", ""),
        description=description,
        annotations=annotations,
        etag=getattr(resource, "etag", None),
    )


def _pipeline_trigger_names(trigger: TriggerResource) -> List[str]:
    trigger_names: List[str] = []
    properties = getattr(trigger, "properties", None)
    pipelines = getattr(properties, "pipelines", None) if properties else None
    if pipelines:
        for pipeline_ref in pipelines:
            reference = getattr(pipeline_ref, "pipeline_reference", None)
            if reference and getattr(reference, "reference_name", None):
                trigger_names.append(reference.reference_name)
    return trigger_names


def get_root_pipelines(
    client: DataFactoryManagementClient,
    settings: Settings,
    lookback_hours: int,
) -> List[RootPipelineModel]:
    triggers: Sequence[TriggerResource] = list(
        client.triggers.list_by_factory(settings.azure_resource_group, settings.adf_factory_name)
    )

    trigger_map: dict[str, List[str]] = {}
    for trigger in triggers:
        pipelines = _pipeline_trigger_names(trigger)
        if not pipelines:
            continue
        trigger_name = getattr(trigger, "name", "")
        for pipeline in pipelines:
            trigger_map.setdefault(pipeline, []).append(trigger_name)

    pipelines = list(client.pipelines.list_by_factory(settings.azure_resource_group, settings.adf_factory_name))
    now_utc = datetime.now(timezone.utc)

    root_pipelines: List[RootPipelineModel] = []
    for pipeline in pipelines:
        pipeline_name = getattr(pipeline, "name", None)
        if not pipeline_name:
            continue
        if pipeline_name not in trigger_map:
            continue

        last_run = get_latest_pipeline_run(
            client,
            settings,
            pipeline_name,
            since=now_utc - timedelta(hours=lookback_hours),
            until=now_utc + timedelta(minutes=5),
        )

        root_pipelines.append(
            RootPipelineModel(
                pipelineName=pipeline_name,
                description=getattr(getattr(pipeline, "properties", None), "description", None),
                triggerNames=trigger_map[pipeline_name],
                lastRun=serialize_pipeline_run(last_run) if last_run else None,
            )
        )

    return root_pipelines


def get_latest_pipeline_run(
    client: DataFactoryManagementClient,
    settings: Settings,
    pipeline_name: str,
    since: datetime,
    until: datetime,
) -> Optional[PipelineRun]:
    parameters = RunFilterParameters(
        last_updated_after=since,
        last_updated_before=until,
        filters=[
            RunQueryFilter(operand="PipelineName", operator="Equals", values=[pipeline_name]),
        ],
        order_by=[RunQueryOrderBy(order_by="RunStart", order=RunQueryOrder.descending)],
    )
    result = client.pipeline_runs.query_by_factory(
        settings.azure_resource_group,
        settings.adf_factory_name,
        parameters,
    )
    runs: Iterable[PipelineRun] = getattr(result, "value", []) or []
    for run in runs:
        return run
    return None


def query_pipeline_runs(
    client: DataFactoryManagementClient,
    settings: Settings,
    request: RunQueryRequestModel,
) -> List[PipelineRunModel]:
    filters = [
        RunQueryFilter(operand=f.operand, operator=f.operator, values=f.values) for f in request.filters
    ]
    parameters = RunFilterParameters(
        last_updated_after=_ensure_utc(request.last_updated_after),
        last_updated_before=_ensure_utc(request.last_updated_before),
        filters=filters,
    )
    result = client.pipeline_runs.query_by_factory(
        settings.azure_resource_group,
        settings.adf_factory_name,
        parameters,
    )
    runs = getattr(result, "value", []) or []
    return [serialize_pipeline_run(run) for run in runs]


def list_activity_runs(
    client: DataFactoryManagementClient,
    settings: Settings,
    run_id: str,
    window_start: datetime,
    window_end: datetime,
) -> ActivityRunsResponseModel:
    parameters = RunFilterParameters(
        last_updated_after=_ensure_utc(window_start),
        last_updated_before=_ensure_utc(window_end),
    )
    response = client.activity_runs.list_by_pipeline_run(
        settings.azure_resource_group,
        settings.adf_factory_name,
        run_id,
        parameters,
    )
    items: List[ActivityRun] = getattr(response, "value", []) or []
    return ActivityRunsResponseModel(items=[serialize_activity_run(i) for i in items])


def serialize_activity_run(activity_run: ActivityRun) -> ActivityRunModel:
    return ActivityRunModel(
        activityRunId=getattr(activity_run, "activity_run_id", None),
        activityName=getattr(activity_run, "activity_name", None),
        activityType=getattr(activity_run, "activity_type", None),
        status=getattr(activity_run, "status", None),
        start=_ensure_utc(getattr(activity_run, "activity_run_start", None)),
        end=_ensure_utc(getattr(activity_run, "activity_run_end", None)),
        input=getattr(activity_run, "input", None),
        output=getattr(activity_run, "output", None),
        error=getattr(activity_run, "error", None),
    )


def resolve_root_pipeline_run(
    client: DataFactoryManagementClient,
    settings: Settings,
    run_id: str,
) -> RootPipelineRunModel:
    seen: set[str] = set()
    current_id = run_id
    last_run: Optional[PipelineRun] = None
    while current_id:
        if current_id in seen:
            # Prevent infinite loops
            break
        seen.add(current_id)
        run = client.pipeline_runs.get(
            settings.azure_resource_group,
            settings.adf_factory_name,
            current_id,
        )
        last_run = run
        invoked_by = getattr(run, "invoked_by", None)
        parent_run_id = getattr(invoked_by, "pipeline_run_id", None) if invoked_by else None
        if not parent_run_id:
            trigger_type = getattr(invoked_by, "invoked_by_type", None) if invoked_by else None
            trigger_name = getattr(invoked_by, "name", None) if invoked_by else None
            return RootPipelineRunModel(
                rootRunId=run.run_id,
                rootPipelineName=run.pipeline_name,
                triggerType=trigger_type,
                triggerName=trigger_name,
            )
        current_id = parent_run_id

    if last_run is None:
        raise ValueError(f"Pipeline run {run_id} was not found.")

    invoked_by = getattr(last_run, "invoked_by", None)
    return RootPipelineRunModel(
        rootRunId=last_run.run_id,
        rootPipelineName=last_run.pipeline_name,
        triggerType=getattr(invoked_by, "invoked_by_type", None) if invoked_by else None,
        triggerName=getattr(invoked_by, "name", None) if invoked_by else None,
    )


def compute_run_summary(
    runs: Iterable[PipelineRunModel],
    window_start: datetime,
    window_end: datetime,
) -> RunSummaryResponseModel:
    counts: dict[str, int] = {}
    for run in runs:
        status = run.status or "Unknown"
        counts[status] = counts.get(status, 0) + 1

    totals = [RunSummaryItemModel(status=status, count=count) for status, count in counts.items()]
    return RunSummaryResponseModel(
        windowStart=_ensure_utc(window_start) or datetime.now(timezone.utc),
        windowEnd=_ensure_utc(window_end) or datetime.now(timezone.utc),
        totals=totals,
    )


# -------------------------
# Mock helpers for dev mode
# -------------------------

def mock_pipeline_summaries() -> List[PipelineSummaryModel]:
    return [
        PipelineSummaryModel(
            name="MockPipeline",
            description="Local mock pipeline",
            annotations=["mock"],
            etag="mock-etag",
        )
    ]


def mock_pipeline_definition(pipeline_name: str) -> dict:
    return {
        "name": pipeline_name,
        "properties": {
            "activities": [],
            "parameters": {},
            "annotations": ["mock"],
            "description": "Mock pipeline definition",
        },
    }


def mock_pipeline_run(pipeline_name: str = "MockPipeline") -> PipelineRunModel:
    now = datetime.now(timezone.utc)
    return PipelineRunModel(
        runId="mock-run-id",
        pipelineName=pipeline_name,
        status="Succeeded",
        startTime=now - timedelta(minutes=5),
        endTime=now,
    )


def mock_root_pipelines() -> List[RootPipelineModel]:
    return [
        RootPipelineModel(
            pipelineName="MockPipeline",
            description="Mock root pipeline",
            triggerNames=["MockTrigger"],
            lastRun=mock_pipeline_run(),
        )
    ]


def mock_activity_runs() -> ActivityRunsResponseModel:
    now = datetime.now(timezone.utc)
    return ActivityRunsResponseModel(
        items=[
            ActivityRunModel(
                activityRunId="mock-activity",
                activityName="MockActivity",
                activityType="ExecutePipeline",
                status="Succeeded",
                start=now - timedelta(minutes=4),
                end=now - timedelta(minutes=3),
                input={"mock": True},
                output={"result": "ok"},
            )
        ]
    )


def mock_root_pipeline_run() -> RootPipelineRunModel:
    return RootPipelineRunModel(
        rootRunId="mock-root-run",
        rootPipelineName="MockPipeline",
        triggerType="Manual",
        triggerName="MockTrigger",
    )


def mock_triggers() -> List[dict]:
    return [
        {
            "name": "MockTrigger",
            "properties": {
                "type": "ScheduleTrigger",
                "pipelines": [{"pipelineReference": {"referenceName": "MockPipeline"}}],
                "annotations": ["mock"],
            },
        }
    ]


def mock_integration_runtimes() -> List[dict]:
    return [{"name": "MockIR", "properties": {"type": "Managed", "description": "Mock IR"}}]


def mock_integration_runtime_status(ir_name: str) -> dict:
    return {"name": ir_name, "state": "Online", "nodes": []}


def mock_linked_services() -> List[dict]:
    return [{"name": "MockLinkedService", "properties": {"type": "AzureBlobStorage"}}]


def mock_datasets() -> List[dict]:
    return [{"name": "MockDataset", "properties": {"linkedServiceName": {"referenceName": "MockLinkedService"}}}]


def mock_failed_runs() -> List[PipelineRunModel]:
    run = mock_pipeline_run()
    run.status = "Failed"
    return [run]
