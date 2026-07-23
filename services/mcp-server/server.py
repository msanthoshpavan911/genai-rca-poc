"""
=============================================================================
MCP Server -- 6 Tools for the GenAI RCA Agent
=============================================================================

For the local POC we expose the tools as HTTP endpoints via FastAPI. This is
functionally equivalent to MCP for local use and trivially convertible to
the official MCP stdio/SSE transport later -- the tool implementations are
unchanged.

Logs are read directly from existing, already-populated production OpenSearch
indices (one per client project, see project_config.py) -- there is no
GenAI-owned ingestion pipeline in production. On-demand retrieval (the chat
flow) is a two-step correlation:
  1. Text-search the project's configured field (default "msg") for the
     entity the user asked about, to find anchor log line(s).
  2. Expand each anchor's `loggingId` (the request/trace correlation field)
     into a term-filter query to pull every log line for that transaction.

Proactive retrieval (Module 3's poller) skips step 1 -- it already knows the
loggingId from a fresh ERROR log it just scanned via find_new_errors, and
goes straight to get_trace_by_logging_id for step 2.

Tools:
  1. analyze_order_logs       -- two-step loggingId correlation search (chat flow)
  2. get_order_status         -- SQL JOIN on orders/payments/shipments
  3. find_similar_incidents   -- DQL multi_match on incidents-historical
  4. save_incident             -- write a completed RCA back to incidents-historical
  5. get_trace_by_logging_id  -- step-2-only trace fetch (proactive poller)
  6. find_new_errors          -- checkpointed ERROR scan, deduped by loggingId (proactive poller)

Run:
    uvicorn server:app --port 8001 --reload
"""

import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from opensearchpy import AsyncOpenSearch

from project_config import get_project_config, list_projects


# =============================================================================
# CONFIGURATION
# =============================================================================
OPENSEARCH_HOST   = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT   = int(os.getenv("OPENSEARCH_PORT", "9200"))
INCIDENTS_INDEX   = os.getenv("INCIDENTS_INDEX", "incidents-historical")

# The field in each project's index that holds the request/trace correlation
# ID ("loggingId is the traceid" per client). Dynamic string mapping in
# OpenSearch auto-creates a `.keyword` sub-field for exact-match term
# filtering -- verify this against the real mapping before go-live.
LOGGING_ID_FIELD  = os.getenv("LOGGING_ID_FIELD", "loggingId.keyword")

# Cap on how many distinct loggingId traces one order/entity search expands
# into, so one query can't pull an unbounded number of transactions.
MAX_TRACES        = int(os.getenv("MAX_TRACES", "3"))


# =============================================================================
# REQUEST / RESPONSE MODELS
# =============================================================================
class AnalyzeLogsRequest(BaseModel):
    project_id: str = Field(..., description="Which project's index to search, e.g. 'app_launchpad'")
    order_no: str = Field(..., description="The order/entity number, e.g. ORD-00042")
    additional_context: str = Field("", description="Extra hint, e.g. 'payment timeout'")
    time_window_hours: int = Field(24, description="Default look-back when no explicit window given")
    top_k: int = Field(3, description="Max log chunks (traces) to return")
    time_window_start: Optional[str] = Field(None, description="ISO datetime lower bound (overrides time_window_hours)")
    time_window_end:   Optional[str] = Field(None, description="ISO datetime upper bound (open-ended if omitted)")


class LogChunk(BaseModel):
    chunk_id: str
    trace_id: str
    earliest_ts: str
    latest_ts: str
    services: List[str]
    log_levels: List[str]
    has_error: bool
    message: str
    score: float


class AnalyzeLogsResponse(BaseModel):
    order_no: str
    total_chunks_found: int
    services_involved: List[str]
    has_errors: bool
    log_chunks: List[LogChunk]


class TraceRequest(BaseModel):
    project_id: str = Field(..., description="Which project's index to search")
    logging_id: str = Field(..., description="The already-known loggingId to expand into a full trace")
    time_window_hours: int = Field(24, description="Look back N hours")


class TraceResponse(BaseModel):
    logging_id: str
    found: bool
    chunk: Optional[LogChunk] = None


class NewErrorsRequest(BaseModel):
    project_id: str = Field(..., description="Which project's index to scan")
    since: str = Field(..., description="ISO8601 checkpoint; only errors strictly after this are returned")
    max_results: int = Field(200, description="Cap on raw error docs scanned per poll")


class NewErrorIncident(BaseModel):
    logging_id: str
    first_seen: str
    sample_message: str


class NewErrorsResponse(BaseModel):
    checked_until: str
    incidents: List[NewErrorIncident]


class SimilarIncidentsRequest(BaseModel):
    description: str = Field(..., description="Free-text description of the issue")
    top_k: int = Field(3, description="Number of similar incidents to return")


class SimilarIncident(BaseModel):
    incident_id: str
    summary: str
    root_cause: str
    resolution: str
    score: float


class SimilarIncidentsResponse(BaseModel):
    incidents: List[SimilarIncident]


class SaveIncidentRequest(BaseModel):
    order_no: Optional[str] = Field(None, description="The order/entity this RCA was about")
    project_id: Optional[str] = Field(None, description="Which project's index this RCA came from")
    summary: str
    root_cause: str
    resolution: str = ""
    keywords: List[str] = Field(default_factory=list)
    source: str = Field(
        "auto-generated",
        description="'curated' (hand-seeded), 'auto-generated' (chat-triggered RCA), or 'proactive-alert' (poller-triggered RCA)",
    )


class SaveIncidentResponse(BaseModel):
    saved: bool
    incident_id: str


# =============================================================================
# LIFESPAN: shared OS client
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.os = AsyncOpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        use_ssl=False, verify_certs=False,
    )
    yield
    await app.state.os.close()


app = FastAPI(title="MCP Server (POC)", lifespan=lifespan)


# =============================================================================
# HEALTH
# =============================================================================
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


# =============================================================================
# TOOL 1: analyze_order_logs
# =============================================================================
def _compose_line(doc: dict, search_field: str) -> str:
    """Render one raw log document as a single evidence line, in the same
    `[timestamp] [service] LEVEL: text` shape the orchestrator's
    _format_evidence() already knows how to scan for ERROR/WARN lines."""
    ts = (doc.get("@timestamp") or "")[:19]
    service = doc.get("logger") or doc.get("instance") or "unknown-service"
    level = (doc.get("level") or "INFO").upper()
    text = doc.get(search_field) or doc.get("msg") or doc.get("log_message") or ""
    line = f"[{ts}] [{service}] {level}: {text}"
    if doc.get("exception"):
        line += f" | exception: {doc['exception']}"
    return line


async def _fetch_trace(index: str, logging_id: str, since: str,
                       until: Optional[str] = None) -> Optional[List[dict]]:
    """Pull every raw log document correlated by loggingId, sorted by time."""
    ts_range = {"gte": since}
    if until:
        ts_range["lte"] = until
    body = {
        "size": 200,
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


def _build_chunk(logging_id: str, docs: List[dict], search_field: str, score: float) -> LogChunk:
    """Fold a set of correlated raw log documents into one LogChunk, the
    shape both analyze_order_logs and get_trace_by_logging_id return, and
    the shape the orchestrator's _format_evidence() already knows how to
    render (including the [timestamp] [service] LEVEL: text convention)."""
    timestamps = [d.get("@timestamp") for d in docs if d.get("@timestamp")]
    services = sorted({d.get("logger") or d.get("instance") or "unknown-service" for d in docs})
    levels = sorted({(d.get("level") or "INFO").upper() for d in docs})
    has_error = any((d.get("level") or "").upper() == "ERROR" for d in docs)
    message = "\n".join(_compose_line(d, search_field) for d in docs)

    return LogChunk(
        chunk_id=logging_id,
        trace_id=logging_id,
        earliest_ts=min(timestamps) if timestamps else "",
        latest_ts=max(timestamps) if timestamps else "",
        services=services,
        log_levels=levels,
        has_error=has_error,
        message=message,
        score=score,
    )


@app.post("/tools/analyze_order_logs", response_model=AnalyzeLogsResponse)
async def analyze_order_logs(req: AnalyzeLogsRequest):
    """
    Two-step correlation search against an already-populated production index:
      1. Text-match the order/entity number against the project's configured
         search field (default "msg") to find anchor log line(s).
      2. Expand each anchor's loggingId into a full trace via a term filter,
         so the LLM sees every correlated log line for that transaction.
    Returns log evidence, NOT analysis. The LLM does the reasoning.
    """
    try:
        project = get_project_config(req.project_id)
    except KeyError as e:
        raise HTTPException(400, str(e))

    index = project["index"]
    search_field = project["search_field"]

    if req.time_window_start:
        since = req.time_window_start
        until = req.time_window_end or None
    else:
        since = (datetime.now(timezone.utc) - timedelta(hours=req.time_window_hours)).isoformat()
        until = None

    ts_range = {"gte": since}
    if until:
        ts_range["lte"] = until

    should_clauses = [{"match": {"level": {"query": "ERROR", "boost": 1.5}}}]
    if req.additional_context:
        should_clauses.append(
            {"match": {search_field: {"query": req.additional_context, "boost": 1.2}}}
        )

    anchor_body = {
        "size": 20,
        "query": {
            "bool": {
                "must": [{"match_phrase": {search_field: req.order_no}}],
                "filter": [{"range": {"@timestamp": ts_range}}],
                "should": should_clauses,
            }
        },
        "sort": [{"@timestamp": {"order": "desc"}}],
    }

    try:
        anchor_resp = await app.state.os.search(index=index, body=anchor_body)
    except Exception as e:
        raise HTTPException(500, f"OpenSearch error: {e}")

    anchor_hits = anchor_resp["hits"]["hits"]
    if not anchor_hits:
        return AnalyzeLogsResponse(
            order_no=req.order_no, total_chunks_found=0,
            services_involved=[], has_errors=False, log_chunks=[],
        )

    # Collect distinct loggingIds in relevance/recency order (an order can
    # span multiple attempts/transactions, each with its own loggingId).
    logging_ids: List[str] = []
    for hit in anchor_hits:
        lid = hit["_source"].get("loggingId")
        if lid and lid not in logging_ids:
            logging_ids.append(lid)
        if len(logging_ids) >= MAX_TRACES:
            break

    anchor_score = anchor_hits[0]["_score"] or 1.0

    chunks: List[LogChunk] = []
    services_set = set()
    has_errors = False

    for lid in logging_ids:
        try:
            docs = await _fetch_trace(index, lid, since, until)
        except Exception as e:
            raise HTTPException(500, f"OpenSearch error: {e}")
        if not docs:
            continue

        chunk = _build_chunk(lid, docs, search_field, anchor_score)
        chunks.append(chunk)
        services_set.update(chunk.services)
        if chunk.has_error:
            has_errors = True

    chunks.sort(key=lambda c: c.earliest_ts)
    chunks = chunks[: req.top_k]

    return AnalyzeLogsResponse(
        order_no=req.order_no,
        total_chunks_found=len(chunks),
        services_involved=sorted(services_set),
        has_errors=has_errors,
        log_chunks=chunks,
    )


# =============================================================================
# TOOL: get_trace_by_logging_id (used by Module 3's proactive poller)
# =============================================================================
@app.post("/tools/get_trace_by_logging_id", response_model=TraceResponse)
async def get_trace_by_logging_id(req: TraceRequest):
    """Fetch a single already-known trace directly by loggingId -- no anchor
    text search. Used by the proactive error poller, which already knows the
    loggingId from a fresh ERROR log it just saw and just needs the full
    correlated trace around it."""
    try:
        project = get_project_config(req.project_id)
    except KeyError as e:
        raise HTTPException(400, str(e))

    index = project["index"]
    search_field = project["search_field"]
    since = (datetime.now(timezone.utc) - timedelta(hours=req.time_window_hours)).isoformat()

    try:
        docs = await _fetch_trace(index, req.logging_id, since)
    except Exception as e:
        raise HTTPException(500, f"OpenSearch error: {e}")

    if not docs:
        return TraceResponse(logging_id=req.logging_id, found=False)

    chunk = _build_chunk(req.logging_id, docs, search_field, score=1.0)
    return TraceResponse(logging_id=req.logging_id, found=True, chunk=chunk)


# =============================================================================
# TOOL: find_new_errors (the "listener" query Module 3's poller runs on schedule)
# =============================================================================
@app.post("/tools/find_new_errors", response_model=NewErrorsResponse)
async def find_new_errors(req: NewErrorsRequest):
    """Scan a project's index for new ERROR-level logs strictly after the
    given checkpoint, deduped to one entry per distinct loggingId. The caller
    (the poller) should save `checked_until` as its next checkpoint."""
    try:
        project = get_project_config(req.project_id)
    except KeyError as e:
        raise HTTPException(400, str(e))

    index = project["index"]
    search_field = project["search_field"]
    checked_until = datetime.now(timezone.utc).isoformat()

    body = {
        "size": req.max_results,
        "query": {
            "bool": {
                "must": [{"match": {"level": "ERROR"}}],
                "filter": [{"range": {"@timestamp": {"gt": req.since}}}],
            }
        },
        "sort": [{"@timestamp": {"order": "asc"}}],
    }

    try:
        resp = await app.state.os.search(index=index, body=body)
    except Exception as e:
        raise HTTPException(500, f"OpenSearch error: {e}")

    seen: Dict[str, NewErrorIncident] = {}
    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        lid = src.get("loggingId")
        if not lid or lid in seen:
            continue
        text = src.get(search_field) or src.get("msg") or src.get("log_message") or ""
        seen[lid] = NewErrorIncident(
            logging_id=lid,
            first_seen=src.get("@timestamp", ""),
            sample_message=text[:300],
        )

    return NewErrorsResponse(checked_until=checked_until, incidents=list(seen.values()))


# =============================================================================
# TOOL 2: find_similar_incidents
# =============================================================================
@app.post("/tools/find_similar_incidents", response_model=SimilarIncidentsResponse)
async def find_similar_incidents(req: SimilarIncidentsRequest):
    """Full-text DQL search on historical RCAs using multi_match + keyword boost."""
    try:
        await app.state.os.indices.get(index=INCIDENTS_INDEX)
    except Exception:
        return SimilarIncidentsResponse(incidents=[])

    body = {
        "size": req.top_k,
        "query": {
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "query": req.description,
                            "fields": ["summary^3", "root_cause^2", "resolution^1"],
                            "type": "best_fields",
                            "fuzziness": "AUTO",
                            "minimum_should_match": "30%",
                        }
                    },
                    {
                        "match": {
                            "keywords": {
                                "query": req.description,
                                "boost": 1.5,
                            }
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        },
    }
    resp = await app.state.os.search(index=INCIDENTS_INDEX, body=body)
    incidents = [
        SimilarIncident(
            incident_id=hit["_source"]["incident_id"],
            summary=hit["_source"]["summary"],
            root_cause=hit["_source"]["root_cause"],
            resolution=hit["_source"]["resolution"],
            score=hit["_score"],
        )
        for hit in resp["hits"]["hits"]
    ]
    return SimilarIncidentsResponse(incidents=incidents)


# =============================================================================
# TOOL 4: save_incident
# =============================================================================
@app.post("/tools/save_incident", response_model=SaveIncidentResponse)
async def save_incident(req: SaveIncidentRequest):
    """Write a completed RCA back into incidents-historical so future
    find_similar_incidents searches can surface it. Entries are tagged with
    `source` ('auto-generated' vs 'curated') so the knowledge base can
    distinguish AI-written history from vetted seed incidents."""
    incident_id = f"AUTO-{req.order_no or 'unknown'}-{uuid.uuid4().hex[:8]}"
    doc = {
        "incident_id": incident_id,
        "summary": req.summary,
        "root_cause": req.root_cause,
        "resolution": req.resolution,
        "keywords": req.keywords,
        "order_no": req.order_no,
        "project_id": req.project_id,
        "source": req.source,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        await app.state.os.index(index=INCIDENTS_INDEX, id=incident_id, body=doc, refresh=True)
    except Exception as e:
        raise HTTPException(500, f"OpenSearch error: {e}")
    return SaveIncidentResponse(saved=True, incident_id=incident_id)


# =============================================================================
# PROJECTS (for the UI's index/project selector)
# =============================================================================
@app.get("/projects")
async def get_projects():
    return {"projects": list_projects()}


# =============================================================================
# TOOL CATALOG (for the agent to discover)
# =============================================================================
@app.get("/tools")
async def list_tools():
    return {
        "tools": [
            {
                "name": "analyze_order_logs",
                "path": "/tools/analyze_order_logs",
                "description": "Retrieve relevant log evidence for an order via loggingId-correlated search against a project's production index.",
                "input_schema": AnalyzeLogsRequest.model_json_schema(),
            },
            {
                "name": "find_similar_incidents",
                "path": "/tools/find_similar_incidents",
                "description": "Find historically similar resolved incidents via full-text search.",
                "input_schema": SimilarIncidentsRequest.model_json_schema(),
            },
            {
                "name": "save_incident",
                "path": "/tools/save_incident",
                "description": "Write a completed RCA back into incidents-historical for future retrieval.",
                "input_schema": SaveIncidentRequest.model_json_schema(),
            },
            {
                "name": "get_trace_by_logging_id",
                "path": "/tools/get_trace_by_logging_id",
                "description": "Fetch a full correlated trace directly by loggingId, no anchor search.",
                "input_schema": TraceRequest.model_json_schema(),
            },
            {
                "name": "find_new_errors",
                "path": "/tools/find_new_errors",
                "description": "Scan a project's index for new ERROR logs since a checkpoint, deduped by loggingId.",
                "input_schema": NewErrorsRequest.model_json_schema(),
            },
        ]
    }