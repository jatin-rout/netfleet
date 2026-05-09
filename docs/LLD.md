# NetFleet — Low Level Design

## Purpose

This document covers the internal design of each
NetFleet component: data schemas, module
responsibilities, inter-component contracts, and
the full implementation detail for Phase 2 (Natural
Language Query) and Phase 3 (RAG on Device Output).

---

## Module Map

```
netfleet/
├── shared/
│   ├── config/
│   │   ├── settings.py        Config dataclasses
│   │   ├── segments.yaml      Segment definitions
│   │   └── jobs.yaml          Job definitions
│   ├── models/
│   │   ├── device.py          Device, Segment enums
│   │   ├── job.py             Job, JobExecution, JobProgress
│   │   └── status.py          Component health model
│   ├── transport/
│   │   ├── base.py            EventTransport ABC
│   │   ├── factory.py         Transport factory
│   │   ├── redis_transport.py Redis Lists impl
│   │   └── kafka_transport.py Kafka Topics impl
│   └── utils/
│       ├── logger.py          Structured JSON logger
│       ├── redis_client.py    Singleton Redis client
│       └── mongo_client.py    Async MongoDB client
│
├── components/
│   ├── inventory/             Component 1
│   ├── orchestrator/          Component 2
│   ├── interim/               Component 3
│   ├── collector/             Component 4a
│   ├── normalizer/            Component 4b
│   ├── db_insert/             Component 5
│   └── rag_indexer/           Component 6 (Phase 3)
│       ├── main.py            Consumer loop
│       ├── embedder.py        Sentence-transformers wrapper
│       └── qdrant_writer.py   Qdrant upsert
│
├── api/
│   ├── main.py                FastAPI app, router registration
│   ├── routers/
│   │   ├── jobs.py            Job management endpoints
│   │   ├── devices.py         Device query endpoints
│   │   ├── inventory.py       Inventory trigger endpoints
│   │   ├── health.py          Health check endpoints
│   │   ├── query.py           Phase 2 NL query endpoint
│   │   └── search.py          Phase 3 RAG search endpoint
│   └── services/
│       ├── nl_query.py        Phase 2 NL query service
│       └── rag_search.py      Phase 3 RAG search service
│
└── plugins/
    ├── base.py                BaseVendorPlugin interface
    ├── registry.py            Plugin registry
    └── vendors/               One module per vendor
```

---

## Queue Message Schemas

All inter-component messages are JSON. The schemas
below are the contracts between pipeline stages.

### DeviceQueueMessage (Interim → Collector)
```json
{
  "execution_id": "uuid",
  "job_id":       "string",
  "job_name":     "string",
  "operation":    "OPTIC_POWER | INTERFACE_STATS | ...",
  "device": {
    "device_id":    "string",
    "ip_address":   "string",
    "vendor":       "cisco_ios | huawei_vrp | ...",
    "segment":      "Tier1 | Edge | ...",
    "region":       "string",
    "protocol":     "SSH | SNMP | ...",
    "status":       "ACTIVE | UNREACHABLE | ..."
  },
  "priority":     "HIGH | STANDARD",
  "queued_at":    "ISO8601"
}
```

### RawDeviceOutput (Collector → queue_raw_results
                    Collector → queue_rag_raw)
```json
{
  "execution_id": "uuid",
  "job_id":       "string",
  "device_id":    "string",
  "vendor":       "string",
  "region":       "string",
  "segment":      "string",
  "operation":    "string",
  "raw_output":   "string (full CLI output)",
  "collected_at": "ISO8601",
  "success":      true
}
```

The same message type is published to both queues.
Normalizer consumes queue_raw_results.
RAG Indexer consumes queue_rag_raw.

### NormalizedRecord (Normalizer → DB Insert)
```json
{
  "execution_id": "uuid",
  "device_id":    "string",
  "vendor":       "string",
  "region":       "string",
  "segment":      "string",
  "operation":    "string",
  "stats":        { "field": "value" },
  "collected_at": "ISO8601"
}
```

The `stats` object schema varies by operation and
vendor but is defined by the TextFSM template for
that vendor+operation pair.

---

## MongoDB Schema

### Collection: devices
```
device_id       string  (unique index)
ip_address      string
mac_address     string  (optional)
serial_number   string  (optional)
vendor          string
segment         string
priority        string
protocol        string
identity_type   string
region          string
status          string
discovered_at   datetime
last_seen       datetime
last_ip_change  datetime
```

Indexes:
- `{ device_id: 1 }` unique
- `{ region: 1, segment: 1 }` compound (inventory delta validation)
- `{ vendor: 1, status: 1 }` compound (NL query hot path)
- `{ region: 1, vendor: 1, status: 1 }` compound (NL query with region filter)

### Collection: device_stats
```
device_id       string
vendor          string
region          string
segment         string
operation       string
stats           object
collected_at    datetime
execution_id    string
```

Indexes:
- `{ device_id: 1, collected_at: -1 }` compound
- `{ region: 1, operation: 1, collected_at: -1 }` compound (NL query hot path)
- `{ execution_id: 1 }` (job-level aggregation)

### Collection: job_executions
```
execution_id    string  (unique index)
job_id          string
job_name        string
operation       string
segments        array[string]
status          string
total_records   int
processed_records int
failed_records  int
triggered_at    datetime
completed_at    datetime
error_message   string
triggered_by    string
```

### Collection: operation_results
```
execution_id    string
device_id       string
operation       string
success         bool
error_message   string
duration_ms     int
collected_at    datetime
```

---

## Qdrant Schema (Phase 3)

### Collection: netfleet_device_output

**Vector config:**
```
size:     384       (all-MiniLM-L6-v2 output dim)
distance: Cosine
```

**Payload schema per point:**
```
device_id:    string   (keyword index)
vendor:       string   (keyword index)
region:       string   (keyword index)
segment:      string   (keyword index)
operation:    string   (keyword index)
execution_id: string
collected_at: datetime (range index)
raw_chunk:    string   (the text that was embedded)
chunk_index:  int      (position within original output)
```

Keyword indexes on device_id, vendor, region, segment,
and operation allow Qdrant payload filtering before
vector comparison. This is more efficient than
post-filtering large result sets.

---

## Component 6 — RAG Indexer (Phase 3)

### embedder.py

Loads sentence-transformers `all-MiniLM-L6-v2` once
at startup. Exposes a single function:

```
chunk_and_embed(
    text: str,
    chunk_tokens: int = 512
) -> list[tuple[str, list[float]]]

Returns: list of (chunk_text, embedding_vector) pairs
```

**Chunking strategy:**
```
1. Split raw_output by newline
2. Accumulate lines until token count approaches limit
3. Token count estimated as len(line.split()) * 1.3
4. Each chunk is a coherent block of CLI output
   (not split mid-line)
5. Minimum chunk size: 10 tokens
   (avoids embedding whitespace-only blocks)
```

Why line-boundary chunking over fixed-character
splits: network CLI output is structured by line
(one interface per line, one log entry per line).
Splitting at character boundaries destroys that
structure and degrades search quality.

### qdrant_writer.py

```
upsert_points(
    points: list[QdrantPoint]
) -> bool

QdrantPoint:
    id:          uuid (deterministic from
                       execution_id + device_id
                       + chunk_index)
    vector:      list[float]
    payload:     dict (matches payload schema above)
```

Deterministic IDs allow safe re-runs. If the
same execution is re-indexed, existing points are
overwritten rather than duplicated.

Batch size: 100 points per Qdrant upsert call.

### main.py — consumer loop

```
On startup:
    Load embedding model
    Connect to Qdrant, create collection if absent
    Connect to transport (Redis or Kafka)

Loop:
    messages = transport.consume_batch(
        topic=QUEUES["rag_raw"],
        batch_size=20,
        timeout=5
    )
    for msg in messages:
        pairs = embedder.chunk_and_embed(
            msg["raw_output"]
        )
        points = [
            QdrantPoint(
                id=deterministic_uuid(
                    msg["execution_id"],
                    msg["device_id"],
                    i
                ),
                vector=vec,
                payload={
                    "device_id":    msg["device_id"],
                    "vendor":       msg["vendor"],
                    "region":       msg["region"],
                    "segment":      msg["segment"],
                    "operation":    msg["operation"],
                    "execution_id": msg["execution_id"],
                    "collected_at": msg["collected_at"],
                    "raw_chunk":    chunk,
                    "chunk_index":  i
                }
            )
            for i, (chunk, vec) in enumerate(pairs)
        ]
        qdrant_writer.upsert_points(points)

Error handling:
    Qdrant unavailable → exponential backoff,
                         message stays in queue
    Embedding fails    → log and skip that message
```

---

## Phase 2 — NL Query Service

### api/routers/query.py

```
POST /api/v1/query

Request:
{
  "question":   string (required)
  "collection": string (optional, default: auto-detect)
  "limit":      int    (optional, default: 50, max: 200)
}

Response 200:
{
  "question":         string,
  "collection_used":  string,
  "mongo_filter":     object,
  "result_count":     int,
  "results":          list[object],
  "summary":          string
}

Response 422:
{
  "error":   "invalid_filter",
  "detail":  string
}

Response 503:
{
  "error":   "llm_unavailable",
  "detail":  string
}
```

### api/services/nl_query.py

**Collection routing (before Claude call):**
```
Keywords → Collection mapping:
    "cpu", "memory", "crc", "error", "power",
    "optical", "rx", "tx", "interface", "route",
    "bgp", "ospf", "stats"
        → device_stats

    "unreachable", "inactive", "discovered",
    "vendor", "segment", "region", "status",
    "device", "BX", "DX", "switch"
        → devices

    "job", "execution", "running", "failed",
    "complete", "timeout"
        → job_executions

If question matches multiple collections:
    device_stats takes priority over devices
    (most ops queries are about stats, not inventory)
```

**System prompt template:**
```
You are a MongoDB query generator for a network
device management platform.

Convert the user question to a MongoDB filter JSON
object. Return ONLY valid JSON. No explanation,
no markdown, no code block.

Target collection: {collection}

Schema for collection '{collection}':
{schema_context}

Rules:
- Use only fields listed in the schema above
- Vendor values are lowercase with underscore:
  cisco_ios, huawei_vrp, juniper_junos,
  bdcom, zte_zxros, utstarcom
- Segment values are: Tier1 Tier2 Tier3 Edge Field
- Status values are: ACTIVE INACTIVE UNREACHABLE
- For numeric comparisons use $gt $lt $gte $lte
- For string substring match use $regex with
  case insensitive option i
- Return {} (empty filter) if the question cannot
  be mapped to a valid filter

Example output: {"region": "bangalore",
                  "stats.crc_errors": {"$gt": 100}}
```

**Schema context strings:**

devices schema context:
```
device_id (string), ip_address (string),
vendor (string: cisco_ios huawei_vrp juniper_junos
               bdcom zte_zxros utstarcom),
segment (string: Tier1 Tier2 Tier3 Edge Field),
region (string), protocol (string: SSH SNMP TELNET REST),
status (string: ACTIVE INACTIVE UNREACHABLE),
identity_type (string: serial_number mac_address),
discovered_at (datetime), last_seen (datetime)
```

device_stats schema context:
```
device_id (string), vendor (string), region (string),
segment (string), operation (string),
collected_at (datetime), execution_id (string),
stats.crc_errors (int), stats.input_errors (int),
stats.output_errors (int), stats.cpu_percent (float),
stats.memory_percent (float), stats.rx_power_dbm (float),
stats.tx_power_dbm (float), stats.interface (string),
stats.admin_status (string), stats.oper_status (string),
stats.in_octets (int), stats.out_octets (int)
```

**Claude call parameters:**
```python
client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=512,
    system=system_prompt,
    messages=[{"role": "user", "content": question}]
)
```

max_tokens=512 is sufficient for a filter JSON and
prevents runaway generation.

**Safety validator:**
```
FORBIDDEN_OPERATORS = {
    "$where", "$eval", "$function",
    "$accumulator", "$addFields"
}

def validate_filter(filter_dict: dict) -> bool:
    Recursively walk all keys in the dict.
    If any key is in FORBIDDEN_OPERATORS → reject.
    If any value is callable → reject.
    Return True if safe.
```

**Retry on parse failure:**
```
Round 1: call Claude with standard prompt
Round 2 (if JSON parse fails): call Claude again
         with stricter prompt appending:
         "Your previous response was not valid JSON.
          Return ONLY the JSON object, no other text."
Round 3: raise LLMFilterError → return 422 to caller
```

**Response formatting:**
```
results = list of MongoDB documents (raw dicts)
summary = f"Found {len(results)} {collection} records
           matching your query."

If results empty:
    summary = "No records found matching your query."
```

---

## Phase 3 — RAG Search Service

### api/routers/search.py

```
POST /api/v1/search

Request:
{
  "question":  string (required)
  "top_k":     int    (optional, default: 10, max: 50)
  "filters":   object (optional, Qdrant payload filter)
}

Response 200:
{
  "question":    string,
  "top_k":       int,
  "results": [
    {
      "score":        float,
      "device_id":    string,
      "vendor":       string,
      "region":       string,
      "segment":      string,
      "operation":    string,
      "collected_at": string,
      "raw_chunk":    string
    }
  ]
}

Response 503:
{
  "error":  "qdrant_unavailable",
  "detail": string
}
```

### api/services/rag_search.py

```
On startup:
    Load sentence-transformers model once
    (shared with RAG Indexer if co-deployed,
     or loaded independently in API container)

search(
    question: str,
    top_k: int = 10,
    filters: dict = None
) -> list[SearchResult]

Steps:
1. Embed question using same model as indexer
   (all-MiniLM-L6-v2) to ensure vector space match
2. Build Qdrant Filter from filters dict if provided
3. Call qdrant_client.search(
       collection_name=QDRANT_COLLECTION,
       query_vector=question_vector,
       limit=top_k,
       query_filter=qdrant_filter,
       with_payload=True
   )
4. Map ScoredPoint results to SearchResult dicts
5. Return sorted by score descending (Qdrant does this)
```

**Payload filter mapping:**

The optional `filters` field in the request accepts
key-value pairs that map directly to Qdrant FieldConditions:
```json
{ "region": "bangalore", "vendor": "cisco_ios" }
```
Maps to:
```python
Filter(must=[
    FieldCondition(key="region",
                   match=MatchValue(value="bangalore")),
    FieldCondition(key="vendor",
                   match=MatchValue(value="cisco_ios"))
])
```

This allows the caller to narrow vector search to
a specific region or vendor before scoring —
equivalent to WHERE clause on the embedding search.

---

## Configuration Reference

### New environment variables (Phase 2 and Phase 3)

```
ANTHROPIC_API_KEY          required for Phase 2
                           Claude API authentication

QDRANT_HOST                default: localhost
QDRANT_PORT                default: 6333
QDRANT_COLLECTION          default: netfleet_device_output

EMBEDDING_MODEL            default: all-MiniLM-L6-v2
EMBEDDING_CHUNK_TOKENS     default: 512
RAG_BATCH_SIZE             default: 20 (messages per consume)
NL_QUERY_MAX_RESULTS       default: 50
NL_QUERY_TIMEOUT_SECONDS   default: 30
```

### New queue names

```
Redis:  queue_rag_raw             (new, consumed by Component 6)
Kafka:  netfleet.rag.raw          (new, consumed by Component 6)
```

Added to RedisConfig.QUEUES and KafkaConfig.TOPICS in
shared/config/settings.py.

---

## Collector Change (Phase 3 Integration)

The only change to the core pipeline is in
components/collector/main.py.

After the existing publish to queue_raw_results,
add one additional publish call:

```python
transport.publish(
    topic=QUEUES["raw_results"],
    message=raw_output_message
)

transport.publish(
    topic=QUEUES["rag_raw"],
    message=raw_output_message
)
```

The message payload is identical. No other collector
logic changes.

---

## Observability — New Metrics

Phase 2 and Phase 3 expose Prometheus metrics
alongside existing pipeline metrics:

```
netfleet_nl_query_total
    Counter, labels: status (success|error)
    Incremented per POST /api/v1/query call

netfleet_nl_query_duration_seconds
    Histogram, end-to-end latency of NL query
    including Claude API call and MongoDB execution

netfleet_claude_api_calls_total
    Counter, labels: status (success|retry|failed)

netfleet_rag_indexed_chunks_total
    Counter, total Qdrant points written by indexer

netfleet_rag_index_duration_seconds
    Histogram, time to embed and upsert one message

netfleet_rag_search_total
    Counter, labels: status (success|error)

netfleet_rag_search_duration_seconds
    Histogram, end-to-end search latency

netfleet_qdrant_queue_depth
    Gauge, len(queue_rag_raw), polled every 30s
    Rising value indicates indexer is falling behind
```

---

## Dependency Additions

Add to setup.py install_requires:
```
anthropic>=0.40.0          Phase 2
qdrant-client>=1.7.0       Phase 3
sentence-transformers>=2.3.0  Phase 3
```

Add to components/rag_indexer/requirements.txt:
```
sentence-transformers>=2.3.0
qdrant-client>=1.7.0
```

Add to api/requirements.txt:
```
anthropic>=0.40.0
sentence-transformers>=2.3.0
qdrant-client>=1.7.0
```

---

## Docker Compose Addition (Standalone)

Add to deploy/standalone/docker-compose.yml:
```yaml
qdrant:
  image: qdrant/qdrant:v1.7.4
  ports:
    - "6333:6333"
  volumes:
    - qdrant_data:/qdrant/storage
  environment:
    QDRANT__SERVICE__HTTP_PORT: 6333
```

---

## API File Registration

api/main.py registers all routers:
```python
from api.routers import jobs, devices, inventory,
                        health, query, search

app.include_router(jobs.router,      prefix="/api/v1")
app.include_router(devices.router,   prefix="/api/v1")
app.include_router(inventory.router, prefix="/api/v1")
app.include_router(health.router,    prefix="/api/v1")
app.include_router(query.router,     prefix="/api/v1")
app.include_router(search.router,    prefix="/api/v1")
```
