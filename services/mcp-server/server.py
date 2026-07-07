"""
=============================================================================
MCP Server -- 3 Tools for the GenAI RCA Agent
=============================================================================

For the local POC we expose the tools as HTTP endpoints via FastAPI. This is
functionally equivalent to MCP for local use and trivially convertible to
the official MCP stdio/SSE transport later -- the tool implementations are
unchanged.

Tools:
  1. analyze_order_logs    -- hard filter on order_no (POC-simple, reliable)
  2. get_order_status      -- SQL JOIN on orders/payments/shipments
  3. find_similar_incidents -- DQL multi_match on incidents-historical

Run:
    uvicorn server:app --port 8001 --reload
"""

import os
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any

import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from opensearchpy import AsyncOpenSearch


# =============================================================================
# CONFIGURATION
# =============================================================================
OPENSEARCH_HOST   = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT   = int(os.getenv("OPENSEARCH_PORT", "9200"))
LOGS_INDEX        = os.getenv("LOGS_INDEX", "logs-vectors-current")
INCIDENTS_INDEX   = os.getenv("INCIDENTS_INDEX", "incidents-historical")

POSTGRES_DSN      = os.getenv(
    "POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/orders"
)


# =============================================================================
# REQUEST / RESPONSE MODELS
# =============================================================================
class AnalyzeLogsRequest(BaseModel):
    order_no: str = Field(..., description="The order number, e.g. ORD-00042")
    additional_context: str = Field("", description="Extra hint, e.g. 'payment timeout'")
    time_window_hours: int = Field(24, description="Look back N hours")
    top_k: int = Field(20, description="Max log chunks to return")


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


class OrderStatusRequest(BaseModel):
    order_no: str


class OrderStatusResponse(BaseModel):
    found: bool
    order_no: Optional[str] = None
    location_no: Optional[str] = None
    location_name: Optional[str] = None
    region: Optional[str] = None
    status: Optional[str] = None
    failed_step: Optional[str] = None
    amount: Optional[float] = None
    currency: Optional[str] = None
    created_at: Optional[str] = None
    payment_status: Optional[str] = None
    payment_method: Optional[str] = None
    payment_failure_reason: Optional[str] = None
    shipment_status: Optional[str] = None
    tracking_id: Optional[str] = None
    carrier: Optional[str] = None


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


# =============================================================================
# LIFESPAN: shared OS client and DB pool
# =============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.os = AsyncOpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        use_ssl=False, verify_certs=False,
    )
    app.state.pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=10)
    yield
    await app.state.os.close()
    await app.state.pool.close()


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
@app.post("/tools/analyze_order_logs", response_model=AnalyzeLogsResponse)
async def analyze_order_logs(req: AnalyzeLogsRequest):
    """
    DQL search: hard filter on order_no ensures we only see that order's logs.
    When additional_context is provided, should clauses boost chunks whose
    message text matches the query terms and chunks that contain errors —
    so the most relevant evidence surfaces first within the top_k window.
    Returns log evidence, NOT analysis. The LLM does the reasoning.
    """
    should_clauses = [
        # Always prefer error chunks
        {"term": {"has_error": {"value": True, "boost": 1.5}}},
    ]
    if req.additional_context:
        should_clauses.append(
            {"match": {"message": {"query": req.additional_context, "boost": 2.0}}}
        )

    body = {
        "size": req.top_k,
        "query": {
            "bool": {
                "filter": [{"term": {"order_no": req.order_no}}],
                "should": should_clauses,
            }
        },
        "sort": [{"earliest_ts": "asc"}],
    }

    try:
        resp = await app.state.os.search(index=LOGS_INDEX, body=body)
    except Exception as e:
        raise HTTPException(500, f"OpenSearch error: {e}")

    hits = resp["hits"]["hits"]
    chunks = []
    services_set = set()
    has_errors = False

    for hit in hits:
        src = hit["_source"]
        chunks.append(LogChunk(
            chunk_id=src["chunk_id"],
            trace_id=src["trace_id"],
            earliest_ts=src["earliest_ts"],
            latest_ts=src["latest_ts"],
            services=src.get("services", []),
            log_levels=src.get("log_levels", []),
            has_error=src.get("has_error", False),
            message=src["message"],
            score=hit["_score"] if hit["_score"] is not None else 1.0,
        ))
        services_set.update(src.get("services", []))
        if src.get("has_error"):
            has_errors = True

    return AnalyzeLogsResponse(
        order_no=req.order_no,
        total_chunks_found=resp["hits"]["total"]["value"],
        services_involved=sorted(services_set),
        has_errors=has_errors,
        log_chunks=chunks,
    )


# =============================================================================
# TOOL 2: get_order_status
# =============================================================================
@app.post("/tools/get_order_status", response_model=OrderStatusResponse)
async def get_order_status(req: OrderStatusRequest):
    """
    Authoritative current state from Postgres.
    Parameterized SQL -- never string concatenation.
    """
    query = """
        SELECT
            o.order_no, o.location_no, o.status, o.failed_step,
            o.amount::float as amount, o.currency, o.created_at::text,
            l.location_name, l.region,
            p.status         as payment_status,
            p.method         as payment_method,
            p.failure_reason as payment_failure_reason,
            s.status         as shipment_status,
            s.tracking_id, s.carrier
        FROM orders o
        LEFT JOIN locations l ON l.location_no = o.location_no
        LEFT JOIN payments  p ON p.order_no    = o.order_no
        LEFT JOIN shipments s ON s.order_no    = o.order_no
        WHERE o.order_no = $1
        LIMIT 1
    """
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(query, req.order_no)

    if not row:
        return OrderStatusResponse(found=False)

    return OrderStatusResponse(found=True, **dict(row))


# =============================================================================
# TOOL 3: find_similar_incidents
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
# TOOL CATALOG (for the agent to discover)
# =============================================================================
@app.get("/tools")
async def list_tools():
    return {
        "tools": [
            {
                "name": "analyze_order_logs",
                "path": "/tools/analyze_order_logs",
                "description": "Retrieve relevant log evidence for an order via filtered search.",
                "input_schema": AnalyzeLogsRequest.model_json_schema(),
            },
            {
                "name": "get_order_status",
                "path": "/tools/get_order_status",
                "description": "Fetch authoritative order/payment/shipment status from DB.",
                "input_schema": OrderStatusRequest.model_json_schema(),
            },
            {
                "name": "find_similar_incidents",
                "path": "/tools/find_similar_incidents",
                "description": "Find historically similar resolved incidents via full-text search.",
                "input_schema": SimilarIncidentsRequest.model_json_schema(),
            },
        ]
    }