from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum
from datetime import datetime


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"


class JobType(str, Enum):
    PERIODIC = "PERIODIC"
    ADHOC = "ADHOC"


class JobOperation(str, Enum):
    OPTIC_POWER = "OPTIC_POWER"
    INTERFACE_STATS = "INTERFACE_STATS"
    DISCOVERY = "DISCOVERY"
    CONFIG_PUSH = "CONFIG_PUSH"
    PASSWORD_ROTATION = "PASSWORD_ROTATION"


class JobPriority(str, Enum):
    HIGH = "HIGH"
    STANDARD = "STANDARD"


class Job(BaseModel):
    job_id: str
    job_name: str
    job_type: JobType = JobType.PERIODIC
    operation: JobOperation
    segments: list[str]
    cron: Optional[str] = None
    timeout_minutes: int = 120
    error_threshold: int = 100
    retry_count: int = 3
    is_active: bool = True
    created_at: datetime = Field(
        default_factory=datetime.utcnow
    )

    class Config:
        use_enum_values = True


class JobExecution(BaseModel):
    execution_id: str
    job_id: str
    job_name: str
    operation: JobOperation
    segments: list[str]
    status: JobStatus = JobStatus.PENDING
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    triggered_at: datetime = Field(
        default_factory=datetime.utcnow
    )
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    triggered_by: str = "scheduler"

    class Config:
        use_enum_values = True


class JobProgress(BaseModel):
    execution_id: str
    job_id: str
    total_records: int
    inserted_records: int = 0
    failed_records: int = 0
    last_updated: datetime = Field(
        default_factory=datetime.utcnow
    )

    @property
    def completion_percentage(self) -> float:
        if self.total_records == 0:
            return 0.0
        return round(
            self.inserted_records / self.total_records * 100,
            2
        )

    @property
    def is_complete(self) -> bool:
        return self.inserted_records >= self.total_records


class JobEventMessage(BaseModel):
    """Scheduler → Interim contract: triggers device resolution."""
    execution_id: str
    job_id: str
    job_name: str
    operation: JobOperation
    segments: list[str]
    protocol: Optional[str] = None
    priority: JobPriority
    total_records: int
    triggered_at: datetime = Field(
        default_factory=datetime.utcnow
    )

    class Config:
        use_enum_values = True
