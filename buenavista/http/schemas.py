from typing import Any, Dict, List, Optional


from pydantic import BaseModel, HttpUrl


def camel_case(s: str) -> str:
    temp = s.split("_")
    return temp[0] + "".join(ele.title() for ele in temp[1:])


class CamelModel(BaseModel):
    class Config:
        alias_generator = camel_case
        allow_population_by_field_name = True


class ClientTypeSignatureParameter(CamelModel):
    kind: str
    value: Any


class ClientTypeSignature(CamelModel):
    raw_type: str
    arguments: List[ClientTypeSignatureParameter]


class Column(CamelModel):
    name: str
    type: str
    type_signature: Optional[ClientTypeSignature] = None


class StatementStats(CamelModel):
    state: str
    waiting_for_prerequisites: bool = False
    queued: bool = False
    scheduled: bool = False
    nodes: int = 1
    total_splits: int = 1
    queued_splits: int = 0
    running_splits: int = 0
    completed_splits: int = 1
    cpu_time_millis: int = 0
    wall_time_millis: int = 0
    waiting_for_prerequisites_time_millis: int = 0
    queued_time_millis: int = 0
    elapsed_time_millis: int
    processed_rows: int = 0
    processed_bytes: int = 0
    peak_memory_bytes: int = 0
    peak_total_memory_bytes: int = 0
    peak_task_total_memory_bytes: int = 0
    spilled_bytes: int = 0
    root_stage: Optional[CamelModel] = None
    runtime_stats: Optional[CamelModel] = None


class QueryError(CamelModel):
    message: Optional[str]
    sql_state: Optional[str]
    error_code: int
    error_name: Optional[str]
    error_type: Optional[str]
    retriable: bool
    error_location: Optional[CamelModel] = None
    failure_info: Optional[CamelModel] = None


class WarningCode(CamelModel):
    code: int  # todo: check non-negative
    name: str


class PrestoWarning(CamelModel):
    warning_code: WarningCode
    message: str


class BaseResult(CamelModel):
    id: str
    info_uri: HttpUrl
    stats: StatementStats
    warnings: Optional[List[PrestoWarning]]


class QueryResult(BaseResult):
    partial_cancel_uri: Optional[HttpUrl]
    columns: Optional[List[Column]]
    data: Optional[List[List[Any]]]
    update_type: Optional[str]
    update_count: Optional[int]


class ErrorResult(BaseResult):
    error: QueryError
