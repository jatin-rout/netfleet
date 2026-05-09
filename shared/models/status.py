from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum
from datetime import datetime


class ComponentStatus(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class DeviceOperationStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    UNREACHABLE = "UNREACHABLE"
    AUTH_FAILED = "AUTH_FAILED"
    SKIPPED = "SKIPPED"


class DeviceOperationResult(BaseModel):
    execution_id: str
    job_id: str
    device_id: str
    ip_address: str
    vendor: str
    segment: str
    operation: str
    status: DeviceOperationStatus
    raw_output: Optional[str] = None
    normalized_data: Optional[dict] = None
    error_message: Optional[str] = None
    executed_at: datetime = Field(
        default_factory=datetime.utcnow
    )
    duration_ms: Optional[int] = None

    class Config:
        use_enum_values = True


class ComponentHealth(BaseModel):
    component_name: str
    instance_id: str
    status: ComponentStatus
    error_count: int = 0
    processed_count: int = 0
    error_threshold: int = 100
    last_heartbeat: datetime = Field(
        default_factory=datetime.utcnow
    )
    error_message: Optional[str] = None

    @property
    def is_threshold_reached(self) -> bool:
        return self.error_count >= self.error_threshold

    class Config:
        use_enum_values = True


class RawOutputMessage(BaseModel):
    execution_id: str
    job_id: str
    device_id: str
    ip_address: str
    vendor: str
    segment: str
    operation: str
    raw_output: str
    produced_at: datetime = Field(
        default_factory=datetime.utcnow
    )

    class Config:
        use_enum_values = True


class NormalizedOutputMessage(BaseModel):
    execution_id: str
    job_id: str
    device_id: str
    ip_address: str
    vendor: str
    segment: str
    operation: str
    normalized_data: dict
    produced_at: datetime = Field(
        default_factory=datetime.utcnow
    )

    class Config:
        use_enum_values = True


class RawDeviceOutput(BaseModel):
    """LLD inter-component contract: Preprocessor → queue_raw_results and queue_rag_raw."""
    execution_id: str
    job_id: str
    device_id: str
    vendor: str
    region: str
    segment: str
    operation: str
    raw_output: str
    collected_at: datetime = Field(
        default_factory=datetime.utcnow
    )
    success: bool = True

    class Config:
        use_enum_values = True


class NormalizedRecord(BaseModel):
    """LLD inter-component contract: Postprocessor → DB Insert."""
    execution_id: str
    device_id: str
    vendor: str
    region: str
    segment: str
    operation: str
    stats: dict
    collected_at: datetime = Field(
        default_factory=datetime.utcnow
    )

    class Config:
        use_enum_values = True


class FailureReason(str, Enum):
    AUTH = "auth"
    CONNECTION = "connection"
    TIMEOUT = "timeout"
    ROLLBACK = "rollback"
    DIRTY_STATE = "dirty_state"
    UNKNOWN = "unknown"


class JobCounters(BaseModel):
    """Per-execution counter snapshot published to Redis by the Collector."""
    execution_id: str
    job_id: str
    job_name: str
    operation: str
    scheduled: int = 0
    successful: int = 0
    failed_total: int = 0
    failed_auth: int = 0
    failed_connection: int = 0
    failed_timeout: int = 0
    failed_rollback: int = 0
    failed_dirty_state: int = 0
    failed_unknown: int = 0
    last_updated: datetime = Field(
        default_factory=datetime.utcnow
    )
