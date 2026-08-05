"""
=============================================================================
Module 1 — Production Log Retrieval Server
=============================================================================

Queries already-populated production OpenSearch indices directly and fetches
correlated traces. No ingestion pipeline — logs are already in OpenSearch
via the client's existing Fluentbit/Logstash pipeline.

Core retrieval logic:
  1. Text-search the project's configured field (default "msg") for the
     entity (order ID) to find anchor log line(s).
  2. Expand each anchor's loggingId (the request/trace correlation field)
     into a term-filter query to pull every log line for that transaction.

OpenSearch connection is fully configurable at startup via environment
variables: host, port, username, password, SSL, and CA cert path.

REST API:
  GET  /api/v1/logs?order_id=ORD-00042&project_id=app_launchpad
                   [&time_from=ISO8601&time_to=ISO8601]
  POST /api/v1/logs/trace   (fetch full trace by loggingId)
  GET  /healthz
  GET  /projects

Run:
    uvicorn server:app --port 8001 --reload
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from opensearchpy import AsyncOpenSearch

from project_config import get_project_config, list_projects


# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mcp-server")


# =============================================================================
# OPENSEARCH CONFIGURATION (all configurable via env vars at startup)
# =============================================================================
OPENSEARCH_HOST     = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT     = int(os.getenv("OPENSEARCH_PORT", "9200"))
OPENSEARCH_USER     = os.getenv("OPENSEARCH_USER", "")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "")
OPENSEARCH_USE_SSL  = os.getenv("OPENSEARCH_USE_SSL", "false").lower() in ("true", "1", "yes")
OPENSEARCH_CA_CERTS = os.getenv("OPENSEARCH_CA_CERTS", "")         # path to CA cert file
OPENSEARCH_VERIFY   = os.getenv("OPENSEARCH_VERIFY_CERTS", "true").lower() in ("true", "1", "yes")

# The field in each project's index that holds the request/trace correlation
# ID ("loggingId is the traceid" per client). Dynamic string mapping in
# OpenSearch auto-creates a `.keyword` sub-field for exact-match term
# filtering — verify this against the real mapping before go-live.
LOGGING_ID_FIELD = os.getenv("LOGGING_ID_FIELD", "loggingId.keyword")

# Cap on how many distinct loggingId traces one order/entity search expands
# into, so one query can't pull an unbounded number of transactions.
MAX_TRACES = int(os.getenv("MAX_TRACES", "3"))

# Default look-back window when no explicit time range is provided.
DEFAULT_LOOKBACK_HOURS = int(os.getenv("DEFAULT_LOOKBACK_HOURS", "24"))


# =============================================================================
# PYDANTIC MODELS
# =============================================================================
class LogEntry(BaseModel):
    """A single log line from OpenSearch, formatted for consumption."""
    timestamp: str
    level: str
    service: str
    message: str
    logging_id: Optional[str] = None
    exception: Optional[str] = None
    raw: dict = Field(default_factory=dict, description="Full raw document from OpenSearch")


class TraceGroup(BaseModel):
    """A group of log entries correlated by the same loggingId (trace)."""
    logging_id: str
    earliest_ts: str
    latest_ts: str
    services: List[str]
    log_levels: List[str]
    has_error: bool
    log_count: int
    logs: List[LogEntry]


class LogsResponse(BaseModel):
    """Response for the /api/v1/logs endpoint."""
    order_id: str
    project_id: str
    time_from: str
    time_to: Optional[str]
    total_traces: int
    total_logs: int
    services_involved: List[str]
    has_errors: bool
    traces: List[TraceGroup]


class TraceRequest(BaseModel):
    project_id: str = Field(..., description="Which project's index to search")
    logging_id: str = Field(..., description="The loggingId to fetch the full trace for")
    time_window_hours: int = Field(DEFAULT_LOOKBACK_HOURS, description="Look back N hours")


class TraceResponse(BaseModel):
    logging_id: str
    found: bool
    trace: Optional[TraceGroup] = None


# =============================================================================
# LIFESPAN: shared OpenSearch client (configured dynamically at startup)
# =============================================================================
def _build_os_client() -> AsyncOpenSearch:
    """Build the AsyncOpenSearch client from environment configuration."""
    kwargs = {
        "hosts": [{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        "use_ssl": OPENSEARCH_USE_SSL,
        "verify_certs": OPENSEARCH_VERIFY,
    }
    if OPENSEARCH_USER and OPENSEARCH_PASSWORD:
        kwargs["http_auth"] = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)
    if OPENSEARCH_CA_CERTS:
        kwargs["ca_certs"] = OPENSEARCH_CA_CERTS

    log.info(
        f"Connecting to OpenSearch at {OPENSEARCH_HOST}:{OPENSEARCH_PORT} "
        f"(SSL={OPENSEARCH_USE_SSL}, auth={'yes' if OPENSEARCH_USER else 'no'})"
    )
    return AsyncOpenSearch(**kwargs)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.os = _build_os_client()
    # Verify connectivity at startup
    try:
        info = await app.state.os.info()
        log.info(f"OpenSearch connected: cluster={info.get('cluster_name', 'unknown')}, "
                 f"version={info.get('version', {}).get('number', 'unknown')}")
    except Exception as e:
        log.error(f"OpenSearch connection failed at startup: {e}")
        log.error("Server will start but requests will fail until OpenSearch is reachable.")
    yield
    await app.state.os.close()


app = FastAPI(
    title="GenAI RCA — Module 1: Log Retrieval Server",
    description="Production log retrieval from OpenSearch with loggingId-based trace correlation.",
    version="1.0.0",
    lifespan=lifespan,
)


# =============================================================================
# HEALTH
# =============================================================================
@app.get("/healthz")
async def healthz():
    """Health check — also verifies OpenSearch connectivity."""
    try:
        health = await app.state.os.cluster.health()
        return {
            "status": "ok",
            "opensearch": {
                "cluster": health.get("cluster_name"),
                "status": health.get("status"),
                "nodes": health.get("number_of_nodes"),
            },
        }
    except Exception as e:
        return {"status": "degraded", "opensearch": {"error": str(e)}}


# =============================================================================
# PROJECTS
# =============================================================================
@app.get("/projects")
async def get_projects():
    """List all configured projects and their OpenSearch indices."""
    return {"projects": list_projects()}


# =============================================================================
# INTERNAL HELPERS
# =============================================================================
def _parse_log_entry(doc: dict, search_field: str) -> LogEntry:
    """Convert a raw OpenSearch document into a structured LogEntry."""
    return LogEntry(
        timestamp=(doc.get("@timestamp") or "")[:23],
        level=(doc.get("level") or "INFO").upper(),
        service=doc.get("logger") or doc.get("instance") or "unknown-service",
        message=doc.get(search_field) or doc.get("msg") or doc.get("log_message") or "",
        logging_id=doc.get("loggingId"),
        exception=doc.get("exception"),
        raw=doc,
    )


async def _fetch_trace_docs(index: str, logging_id: str, since: str,
                            until: Optional[str] = None) -> Optional[List[dict]]:
    """Pull every raw log document correlated by loggingId, sorted by time."""
    ts_range: dict = {"gte": since}
    if until:
        ts_range["lte"] = until
    body = {
        "size": 500,
        "query": {
            "bool": {
                "filter": [
                    {"term": {LOGGING_ID_FIELD: logging_id}},
                    {"range": {"@timestamp": ts_range}},
                ]
            }
        },
        "sort": [{"@timestamp": {"order": "asc"}}],
    }
    resp = await app.state.os.search(index=index, body=body)
    hits = resp["hits"]["hits"]
    if not hits:
        return None
    return [h["_source"] for h in hits]


def _build_trace_group(logging_id: str, docs: List[dict], search_field: str) -> TraceGroup:
    """Fold a set of correlated raw log documents into one TraceGroup."""
    entries = [_parse_log_entry(d, search_field) for d in docs]
    timestamps = [d.get("@timestamp") for d in docs if d.get("@timestamp")]
    services = sorted({e.service for e in entries})
    levels = sorted({e.level for e in entries})
    has_error = any(e.level == "ERROR" for e in entries)

    return TraceGroup(
        logging_id=logging_id,
        earliest_ts=min(timestamps) if timestamps else "",
        latest_ts=max(timestamps) if timestamps else "",
        services=services,
        log_levels=levels,
        has_error=has_error,
        log_count=len(entries),
        logs=entries,
    )


# =============================================================================
# REST API: GET /api/v1/logs — Fetch logs by order ID
# =============================================================================
@app.get("/api/v1/logs", response_model=LogsResponse)
async def get_logs(
    order_id: str = Query(..., description="The order/entity number, e.g. ORD-00042"),
    project_id: str = Query(..., description="Which project's index to search, e.g. 'app_launchpad'"),
    time_from: Optional[str] = Query(None, description="ISO8601 start time (default: 24 hours ago)"),
    time_to: Optional[str] = Query(None, description="ISO8601 end time (default: now)"),
    max_traces: int = Query(MAX_TRACES, description="Max number of correlated traces to return"),
):
    """
    Fetch logs from OpenSearch for a given order ID.

    Two-step correlation:
      1. Text-search the project's log field for the order ID to find anchor logs.
      2. Expand each anchor's loggingId into a full correlated trace.

    If time_from is not provided, defaults to last 24 hours.
    If time_to is not provided, defaults to now (open-ended).
    """
    # Resolve project config
    try:
        project = get_project_config(project_id)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))

    index = project["index"]
    search_field = project["search_field"]

    # Resolve time window — default to last 24 hours
    since = time_from or (datetime.now(timezone.utc) - timedelta(hours=DEFAULT_LOOKBACK_HOURS)).isoformat()
    until = time_to

    ts_range: dict = {"gte": since}
    if until:
        ts_range["lte"] = until

    # Step 1: Anchor search — find log lines mentioning this order ID
    anchor_body = {
        "size": 50,
        "query": {
            "bool": {
                "must": [{"match_phrase": {search_field: order_id}}],
                "filter": [{"range": {"@timestamp": ts_range}}],
                "should": [{"match": {"level": {"query": "ERROR", "boost": 1.5}}}],
            }
        },
        "sort": [{"@timestamp": {"order": "desc"}}],
    }

    try:
        anchor_resp = await app.state.os.search(index=index, body=anchor_body)
    except Exception as e:
        log.error(f"OpenSearch anchor search failed: {e}")
        raise HTTPException(status_code=500, detail=f"OpenSearch error: {e}")

    anchor_hits = anchor_resp["hits"]["hits"]
    if not anchor_hits:
        return LogsResponse(
            order_id=order_id, project_id=project_id,
            time_from=since, time_to=until,
            total_traces=0, total_logs=0,
            services_involved=[], has_errors=False, traces=[],
        )

    # Step 2: Collect distinct loggingIds from anchor hits
    logging_ids: List[str] = []
    for hit in anchor_hits:
        lid = hit["_source"].get("loggingId")
        if lid and lid not in logging_ids:
            logging_ids.append(lid)
        if len(logging_ids) >= max_traces:
            break

    # Step 3: Expand each loggingId into a full correlated trace
    traces: List[TraceGroup] = []
    all_services: set = set()
    has_errors = False
    total_logs = 0

    for lid in logging_ids:
        try:
            docs = await _fetch_trace_docs(index, lid, since, until)
        except Exception as e:
            log.error(f"OpenSearch trace fetch failed for {lid}: {e}")
            raise HTTPException(status_code=500, detail=f"OpenSearch error: {e}")
        if not docs:
            continue

        trace = _build_trace_group(lid, docs, search_field)
        traces.append(trace)
        all_services.update(trace.services)
        total_logs += trace.log_count
        if trace.has_error:
            has_errors = True

    traces.sort(key=lambda t: t.earliest_ts)

    return LogsResponse(
        order_id=order_id,
        project_id=project_id,
        time_from=since,
        time_to=until,
        total_traces=len(traces),
        total_logs=total_logs,
        services_involved=sorted(all_services),
        has_errors=has_errors,
        traces=traces,
    )


# =============================================================================
# REST API: POST /api/v1/logs/trace — Fetch full trace by loggingId
# =============================================================================
@app.post("/api/v1/logs/trace", response_model=TraceResponse)
async def get_trace_by_logging_id(req: TraceRequest):
    """Fetch a full correlated trace directly by loggingId.
    Useful when you already know the trace/correlation ID."""
    try:
        project = get_project_config(req.project_id)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=str(e))

    index = project["index"]
    search_field = project["search_field"]
    since = (datetime.now(timezone.utc) - timedelta(hours=req.time_window_hours)).isoformat()

    try:
        docs = await _fetch_trace_docs(index, req.logging_id, since)
    except Exception as e:
        log.error(f"OpenSearch trace fetch failed: {e}")
        raise HTTPException(status_code=500, detail=f"OpenSearch error: {e}")

    if not docs:
        return TraceResponse(logging_id=req.logging_id, found=False)

    trace = _build_trace_group(req.logging_id, docs, search_field)
    return TraceResponse(logging_id=req.logging_id, found=True, trace=trace)