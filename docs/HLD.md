# NetFleet — High Level Design

## Problem Statement

Managing large scale network device fleets is hard.
Most solutions either do not scale or do not handle
failure gracefully.

When you have millions of devices spread across
multiple regions and segments you need a platform that:

- Automatically discovers devices as they join or leave
- Schedules configuration jobs across specific segments
- Connects to hundreds of devices concurrently
- Handles failures without losing the entire job
- Normalizes output across vendors and protocols
- Scales elastically based on workload

Without such a platform:
- Operations teams manually push configs device by device
- No visibility into which devices succeeded or failed
- A single device timeout blocks entire operations
- No audit trail of what changed and when
- No way to scale beyond a few thousand devices

---

## Market Gap
```
Ansible Network    → Good for small fleets
                     Sequential — too slow at scale
                     No built in fleet discovery

Cisco NSO          → Enterprise grade but expensive
                     Vendor specific
                     Not open source

Netmiko Scripts    → What most engineers use today
                     Manual, not scalable
                     No failure handling

Open Source Gap    → Nothing handles millions of devices
                     Multi vendor
                     Intelligent failure handling
                     Built in security
                     Real time observability
```

NetFleet fills this gap.

---

## System Overview

NetFleet is a distributed network automation platform
built on a five component pipeline. It supports three
deployment profiles to match any scale requirement.
```
Standalone   → Docker Compose, Redis queues
               Cron based discovery
               Up to 100K devices

Distributed  → Kubernetes, Kafka event bus
               Event driven discovery
               Auto scaling workers
               100K to 10M devices

Enterprise   → Kubernetes cluster
               Kafka cluster, MongoDB cluster
               Redis cluster, multi tenant
               10M+ devices
```

---

## Architecture

### Core Pipeline
```mermaid
graph TB
    subgraph InputSources
        PDB[(Primary DB)]
        SF[Secondary Files]
        NC[Network Controller]
    end

    subgraph Pipeline
        D[Component 1\nDevice Discovery]
        S[Component 2\nJob Scheduler]
        I[Component 3\nInterim Orchestrator]
        P[Component 4a\nPreprocessor Pool]
        PP[Component 4b\nPostprocessor Pool]
        M[Component 5\nMongoDB Insert]
    end

    subgraph TransportLayer
        RQ[Redis Queues\nStandalone]
        KQ[Kafka Topics\nDistributed]
    end

    subgraph Storage
        DB[(MongoDB)]
        RC[(Redis Cache)]
    end

    subgraph NetworkDevices
        C[Cisco IOS]
        H[Huawei VRP]
        J[Juniper JunOS]
        B[BDCOM]
        Z[ZTE ZXROS]
        U[UTStarcom]
    end

    PDB --> D
    SF --> D
    NC --> D
    D --> DB
    S --> I
    I --> TransportLayer
    TransportLayer --> P
    P --> C
    P --> H
    P --> J
    P --> B
    P --> Z
    P --> U
    C --> P
    H --> P
    J --> P
    B --> P
    Z --> P
    U --> P
    P --> TransportLayer
    TransportLayer --> PP
    PP --> TransportLayer
    TransportLayer --> M
    M --> DB
    M --> RC
    RC --> S
```

---

### Hybrid Transport Architecture
```mermaid
graph LR
    subgraph StandaloneMode
        R[Redis Lists\nDocker Compose\nUp to 100K devices]
    end

    subgraph DistributedMode
        K[Kafka Topics\nKubernetes\n100K to 10M devices]
    end

    subgraph BusinessLogic
        BL[All Components\nSame code\nDifferent transport]
    end

    ENV[TRANSPORT_MODE\nenv variable] --> BL
    BL --> R
    BL --> K
```

One environment variable switches the entire
transport layer. Business logic never changes.

---

### Deployment Profiles
```mermaid
graph TB
    subgraph Standalone
        DC[Docker Compose]
        RS[Redis Single]
        MS[MongoDB Single]
        CD[Cron Discovery]
    end

    subgraph Distributed
        K8S[Kubernetes]
        KF[Kafka]
        HPA[Auto Scaling]
        ED[Event Discovery]
    end

    subgraph Enterprise
        K8SE[Kubernetes Cluster]
        KFC[Kafka Cluster]
        MDC[MongoDB Cluster]
        RDC[Redis Cluster]
        MT[Multi Tenant]
    end
```

---

## Network Segments

| Segment | Type | Priority | Identity | Vendors |
|---|---|---|---|---|
| Tier1 | Core Switch | HIGH | Serial Number | Cisco, Huawei, Juniper |
| Tier2 | Distribution Switch | HIGH | Serial Number | Cisco, Huawei |
| Tier3 | Data Center Switch | HIGH | Serial Number | Cisco, Huawei, Juniper |
| Edge | Edge Switch | STANDARD | MAC Address | BDCOM, ZTE, UTStarcom |
| Field | Field Switch | STANDARD | MAC Address | BDCOM, UTStarcom |

---

## Component Responsibilities

### Component 1 — Device Discovery

Maintains live fleet inventory across all segments
and regions.

**Two modes:**
```
Cron Mode — Standalone:
    Runs daily in idle hours
    Queries primary DB for higher segments
    Reads secondary files for lower segments
    Region basis delta validation
    Blue green refresh pattern

Event Driven Mode — Distributed:
    Listens to network controller events
    Device joins network — discovered immediately
    Device leaves network — marked inactive
    Real time fleet accuracy
```

**Region basis delta validation:**
```
For each region:
    existing_count = current DB count
    incoming_count = new data count
    delta_pct = abs(existing - incoming) / existing * 100

    if delta_pct <= threshold:
        region VALID
    else:
        region INVALID — abort, keep existing data
```

Prevents silent data loss when secondary files
are partially copied or primary DB is unavailable.

**Blue Green Refresh:**
```
Validate all regions
    → Backup current DB
    → Truncate
    → Bulk insert new data
    → Verify counts
    → Rollback to backup if insert fails
```

---

### Component 2 — Job Scheduler

Monitors all configured jobs and triggers based
on cron schedules. Owns the job lifecycle.

**Job lifecycle:**
```
PENDING → RUNNING → COMPLETE
                  → FAILED
                  → TIMEOUT
```

**Count based completion tracking:**
```
At trigger:
    store total_records = device count for segment

During execution:
    MongoDB Insert reports inserted_records
    via Redis cache

Completion:
    if inserted_records >= total_records:
        mark job COMPLETE
```

**Timeout safety net:**

If job does not complete within timeout window
mark FAILED. Prevents zombie jobs running forever.

---

### Component 3 — Interim Orchestrator

Resolves devices for given segment from Discovery DB
and distributes to priority queues via transport.

**Priority isolation:**
```
Tier1, Tier2, Tier3 → HIGH priority queue
Edge, Field         → STANDARD priority queue
```

Critical operations never blocked by high volume
lower segment jobs.

---

### Component 4a — Preprocessor Pool

Connects to devices and executes operations.
Performance heart of the system.

**Why ThreadPool not AsyncIO:**
```
Network devices have unpredictable latency:
    Fast device   → 200ms response
    Slow device   → 30000ms response

AsyncIO event loop:
    One slow device blocks entire loop
    Cascading failures

ThreadPool:
    Each device gets its own thread
    Slow device only blocks itself
    Predictable and isolated
```

**Plugin based vendor support:**
```python
handler = PluginRegistry.get_handler(
    vendor=device.vendor,
    protocol=device.protocol
)
result = handler.execute(operation)
```

**Error threshold circuit breaker:**
```
connection_errors >= ERROR_THRESHOLD:
    component marks itself FAILED
    signals Scheduler immediately
```

**Retry logic:**
```
Timeout      → retry once
Auth failure → retry once
Unreachable  → no retry
```

---

### Component 4b — Postprocessor Pool

Normalizes raw device output using TextFSM templates.

**Why TextFSM:**
```
Same command — different vendor output:

Cisco:
    GigabitEthernet0/0 is up, line protocol is up
    Hardware address is aabb.ccdd.eeff

Huawei:
    GigabitEthernet0/0/0 current state: UP
    Hardware address is AABB-CCDD-EEFF

TextFSM normalizes both to:
    {
        "interface": "GigabitEthernet0/0",
        "status": "up",
        "mac_address": "aa:bb:cc:dd:ee:ff"
    }
```

Template library covers all supported vendors.
Community can contribute templates for any vendor.

---

### Component 5 — MongoDB Insert

Independent microservice for bulk database writes.
Separated from Postprocessor for independent
scaling and clean failure isolation.

**Aggregator pattern with Redis cache:**
```
Records arrive in batches

Cache per job:
    {
        total_records: N,
        inserted_records: 0,
        last_updated: timestamp
    }

Each batch:
    inserted_records += batch_size
    if inserted >= total:
        signal Scheduler COMPLETE

Cache timeout:
    No new records within window
    Signal Scheduler FAILED
```

---

## Plugin Architecture

Anyone can add vendor support by implementing
the base plugin interface:
```python
class BaseVendorPlugin:
    vendor: str
    supported_protocols: list[str]
    supported_operations: list[str]
    textfsm_templates_path: str

    def connect(self, device: Device): pass
    def execute(self, operation: str,
                params: dict) -> str: pass
    def disconnect(self): pass
    def health_check(self) -> bool: pass

# Register
PluginRegistry.register(CiscoIOSPlugin)
```

**Supported vendors:**

| Vendor | Segments | Protocols |
|---|---|---|
| Cisco IOS | Tier1, Tier2, Tier3 | SSH, SNMP |
| Huawei VRP | Tier1, Tier2, Tier3 | SSH, SNMP, Telnet |
| Juniper JunOS | Tier1, Tier3 | SSH, SNMP |
| BDCOM | Edge, Field | Telnet, SSH |
| ZTE ZXROS | Edge, Field | Telnet, SSH |
| UTStarcom | Edge, Field | Telnet, SSH |

---

## Data Flow

### Happy Path
```
1.  Scheduler detects job cron matches current time
2.  Scheduler calls Interim with job details
3.  Interim queries Discovery DB for segment devices
4.  Interim publishes device records to transport
5.  Scheduler marks job RUNNING stores total_records
6.  Preprocessor workers consume from transport
7.  Plugin handler connects to device
8.  Raw output published to transport
9.  Postprocessor consumes raw output
10. TextFSM template normalizes vendor output
11. Normalized records published to transport
12. MongoDB Insert consumes normalized records
13. Bulk insert to MongoDB
14. Cache updated with inserted count
15. Scheduler detects inserted equals total
16. Job marked COMPLETE
```

### Failure Paths
```
Device unreachable:
    Skip, no retry, continue

Device timeout or auth failure:
    Retry once, then skip

Component error threshold reached:
    Component FAILED
    Signal Scheduler
    Job FAILED
    Alert raised

Instance crash:
    Thread pool records lost
    Other instances absorb remaining work

Cache timeout:
    MongoDB Insert detects silence
    Signals Scheduler
    Job FAILED
```

---

## Key Design Decisions

### 1. Pluggable Transport Layer
Single environment variable switches between Redis
and Kafka. Business logic never changes. Same code
deploys at any scale.

### 2. ThreadPool over AsyncIO
Network device latency is unpredictable. AsyncIO
blocked by one slow device cascades failures.
ThreadPool isolates each connection completely.

### 3. Plugin Based Vendor Support
Hardcoded vendor support limits adoption. Plugin
interface lets anyone add any vendor. Community
grows the platform organically.

### 4. Region Basis Delta Validation
Global threshold misses partial file failures.
Region basis catches cases where one region fails
while others succeed.

### 5. MongoDB as Independent Microservice
Separated from Postprocessor for independent
scaling and single responsibility per component.

### 6. Blue Green Discovery Refresh
Atomic all-or-nothing refresh. No partial state.
Instant rollback on failure.

### 7. TextFSM Template Library
Vendor normalization solved once in templates.
New vendor requires only new templates and plugin.
No code changes needed.

---

## Failure Handling Summary

| Scenario | Detection | Response |
|---|---|---|
| Device unreachable | Connection error | Skip no retry |
| Device timeout | Socket timeout | Retry once |
| Auth failure | Auth exception | Retry once |
| Error threshold | Error counter | Component FAILED |
| Instance crash | Thread pool lost | Others continue |
| Cache timeout | No new records | Job FAILED |
| Discovery delta invalid | Region mismatch | Abort keep existing |
| Transport failure | Connection error | Circuit breaker |

---

## Observability

Every component exposes Prometheus metrics:
```
netfleet_devices_processed_total
netfleet_job_duration_seconds
netfleet_connection_errors_total
netfleet_queue_depth
netfleet_component_health
netfleet_transport_publish_total
netfleet_transport_consume_total
```

Pre-built Grafana dashboard included in
`metrics/grafana/netfleet_dashboard.json`

---

## Technology Stack

| Layer | Standalone | Distributed | Enterprise |
|---|---|---|---|
| Queue | Redis Lists | Kafka Topics | Kafka Cluster |
| Cache | Redis | Redis | Redis Cluster |
| Database | MongoDB | MongoDB | MongoDB Cluster |
| Deployment | Docker Compose | Kubernetes | Kubernetes |
| Discovery | Cron | Event Driven | Event Driven |
| Scaling | Manual | HPA Auto | HPA Auto |

**Common across all profiles:**

| Component | Technology | Reason |
|---|---|---|
| API | FastAPI | Async, Pydantic, auto Swagger |
| Concurrency | ThreadPool | Device latency variance |
| Normalization | TextFSM | Industry standard |
| Protocols | Netmiko | Battle tested network IO |
| Monitoring | Prometheus and Grafana | Industry standard |

---

## Future Roadmap

- Anomaly detection on collected stats
- Predictive alerting before device failure
- RESTCONF and YANG model support
- Web based job management dashboard
- Dead letter queue for failed operations
- Distributed tracing with OpenTelemetry
- Support for gNMI streaming telemetry
