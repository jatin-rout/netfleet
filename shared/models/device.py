from pydantic import BaseModel, Field
from typing import Optional
from enum import Enum
from datetime import datetime


class SegmentType(str, Enum):
    TIER1 = "Tier1"
    TIER2 = "Tier2"
    TIER3 = "Tier3"
    EDGE = "Edge"
    FIELD = "Field"


class SegmentPriority(str, Enum):
    HIGH = "HIGH"
    STANDARD = "STANDARD"


class Protocol(str, Enum):
    SNMP = "SNMP"
    SSH = "SSH"
    TELNET = "TELNET"
    REST = "REST"


class DeviceStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    UNREACHABLE = "UNREACHABLE"


class IdentityType(str, Enum):
    MAC_ADDRESS = "mac_address"
    SERIAL_NUMBER = "serial_number"


class VendorType(str, Enum):
    CISCO_IOS = "cisco_ios"
    HUAWEI_VRP = "huawei_vrp"
    JUNIPER_JUNOS = "juniper_junos"
    BDCOM = "bdcom"
    ZTE_ZXROS = "zte_zxros"
    UTSTARCOM = "utstarcom"


class Device(BaseModel):
    device_id: str
    ip_address: str
    mac_address: Optional[str] = None
    serial_number: Optional[str] = None
    vendor: VendorType
    segment: SegmentType
    priority: SegmentPriority
    protocol: Protocol
    identity_type: IdentityType
    region: str
    status: DeviceStatus = DeviceStatus.ACTIVE
    discovered_at: datetime = Field(
        default_factory=datetime.utcnow
    )
    last_seen: Optional[datetime] = None
    last_ip_change: Optional[datetime] = None

    class Config:
        use_enum_values = True


class DeviceInventoryRecord(BaseModel):
    region: str
    segment: SegmentType
    devices: list[Device]
    total_count: int
    source: str
    discovered_at: datetime = Field(
        default_factory=datetime.utcnow
    )


class DeviceDeltaResult(BaseModel):
    region: str
    segment: SegmentType
    existing_count: int
    incoming_count: int
    delta_count: int
    delta_percentage: float
    is_valid: bool
    reason: Optional[str] = None


class DeviceQueueMessage(BaseModel):
    execution_id: str
    job_id: str
    job_name: str
    operation: str
    device: Device
    priority: SegmentPriority
    job_protocol: Optional[str] = None
    params: dict = Field(default_factory=dict)
    queued_at: datetime = Field(
        default_factory=datetime.utcnow
    )

    class Config:
        use_enum_values = True
