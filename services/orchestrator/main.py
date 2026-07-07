"""
=============================================================================
Orchestrator — FastAPI + LangGraph Agent + Ollama
=============================================================================

Receives chat requests, runs the RCA flow as a LangGraph state machine,
streams the response back via Server-Sent Events.

Uses Ollama (local LLM) instead of Claude API for zero-cost local POC.
The model selection is configurable via env vars:
  - SYNTHESIS_MODEL (default: qwen2.5:7b) — for RCA generation
  - ROUTER_MODEL    (default: qwen2.5:3b) — for intent classification

Run:
    uvicorn main:app --port 8000 --reload
"""

import json
import logging
import os
import re
from typing import Annotated, Dict, List, Literal, Optional, TypedDict

import httpx
import redis.asyncio as redis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama
from langgraph.graph import StateGraph, END
from pydantic import BaseModel


# =============================================================================
# CONFIG
# =============================================================================
MCP_BASE_URL     = os.getenv("MCP_BASE_URL", "http://localhost:8001")
OLLAMA_BASE_URL  = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
SYNTHESIS_MODEL  = os.getenv("SYNTHESIS_MODEL", "qwen2.5:7b")
ROUTER_MODEL     = os.getenv("ROUTER_MODEL", "qwen2.5:3b")
REDIS_URL        = os.getenv("REDIS_URL", "redis://localhost:6379")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orchestrator")


# =============================================================================
# PROMPTS
# =============================================================================
KEYWORD_EXPANSION_PROMPT = """You are a technical search assistant helping find similar past incidents in a microservice order-management system.

Convert the business description below into technical search keywords. Think about: Java exception names, Spring Boot concepts, infrastructure components (HikariCP, Redis, Kafka, PSP gateway), HTTP status codes, and performance terms.

Business description: {description}

Respond with ONLY a single line of space-separated lowercase keywords. No explanation, no punctuation, no bullet points.

Keywords:"""


INTENT_CLASSIFIER_PROMPT = """You classify user questions into one of these intents:
- ORDER_INQUIRY:    asking about a specific order's status, failure, history (mentions order number like ORD-00042)
- STATUS_ONLY:      asking only "what's the status" with no need for log analysis
- GENERAL_QUESTION: general questions not about a specific order
- CLARIFICATION:    too vague to act on, need to ask the user a follow-up

Respond with ONLY one word: ORDER_INQUIRY, STATUS_ONLY, GENERAL_QUESTION, or CLARIFICATION.

User question: {message}
Classification:"""


RCA_SYNTHESIS_PROMPT = """You are a senior Site Reliability Engineer reviewing order activity.

ORDER STATUS (from database):
{order_status}

LOG EVIDENCE:
{log_evidence}

SIMILAR PAST INCIDENTS:
{similar_incidents}

Look at the log evidence. Decide:
- If ERROR-level logs are present → write a Root Cause Analysis using FORMAT A below.
- If only INFO/WARN logs are present (no ERROR) → write an Order Summary using FORMAT B below.

Output exactly ONE format. Do not output both. Do not repeat sections. Do not include these instructions in your answer.

==============================
FORMAT A — Root Cause Analysis
==============================

**Summary**
One or two sentences describing what failed and which service was the trigger.

**What Happened**
Factual timeline from the logs. Cite specific timestamps, service names, and log levels.

**Root Cause**
The single underlying trigger. If multiple failures exist across transactions, identify the common root cause or list each cause once as a sub-bullet. Do not repeat the same cause.

**Contributing Factors**
Pre-existing conditions that worsened the failure. Write "Insufficient evidence" if none found in the logs.

**Recommended Actions**
1. Immediate: ...
2. Short-term: ...
3. Long-term: ...

**Evidence**
Consolidated bullet list of the specific log lines (timestamp + service + message) that support this analysis.

=========================
FORMAT B — Order Summary
=========================

**Summary**
One or two sentences confirming the order completed successfully.

**What Happened**
Factual timeline from the logs. Cite specific timestamps and service names.

**Evidence**
Bullet list of the key log lines that confirm successful completion.

==============================
RULES:
- Output only the chosen format — no headers like "STEP 1", "FORMAT A", "IF ERRORS", etc.
- Use only facts from the log evidence. Never fabricate failures not present in the logs.
- If the same root cause appears across multiple transactions, state it once.
- Cite timestamps and service names directly from the evidence.
"""


# =============================================================================
# STATE
# =============================================================================
class AgentState(TypedDict):
    user_message: str
    session_id: str
    order_no: Optional[str]
    intent: Optional[str]
    order_status: Optional[Dict]
    log_evidence: Optional[Dict]
    similar_incidents: Optional[List[Dict]]
    response: Optional[str]


# =============================================================================
# UTILITIES
# =============================================================================
ORDER_NO_PATTERN = re.compile(r"\bORD-\d{1,6}\b", re.IGNORECASE)


def extract_order_no(text: str) -> Optional[str]:
    """Find a string like ORD-00042 in the user message."""
    match = ORDER_NO_PATTERN.search(text)
    if match:
        order_no = match.group(0).upper()
        # Normalize to 5-digit padding (matches our seed data)
        prefix, num = order_no.split("-")
        return f"{prefix}-{int(num):05d}"
    return None


async def call_mcp(tool_path: str, payload: dict) -> dict:
    """Call an MCP tool over HTTP."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{MCP_BASE_URL}{tool_path}", json=payload)
        resp.raise_for_status()
        return resp.json()


# =============================================================================
# LLMs
# =============================================================================
def get_router_llm():
    return ChatOllama(
        model=ROUTER_MODEL,
        base_url=OLLAMA_BASE_URL,
        temperature=0.0,
    )


def get_synthesis_llm():
    return ChatOllama(
        model=SYNTHESIS_MODEL,
        base_url=OLLAMA_BASE_URL,
        temperature=0.2,
    )


# =============================================================================
# LANGGRAPH NODES
# =============================================================================
async def classify_intent(state: AgentState) -> AgentState:
    """Use the small router LLM to classify intent."""
    log.info("Node: classify_intent")
    message = state["user_message"]

    # Quick rule: if it contains an order number, it's an inquiry
    order_no = extract_order_no(message)
    state["order_no"] = order_no

    if order_no:
        # Heuristic: if "status" is the only thing they want, route accordingly
        lower = message.lower()
        if any(w in lower for w in ["just status", "only status", "what is the status", "current status"]):
            state["intent"] = "STATUS_ONLY"
        else:
            state["intent"] = "ORDER_INQUIRY"
        return state

    # No order number — ask the LLM
    llm = get_router_llm()
    result = await llm.ainvoke([HumanMessage(content=INTENT_CLASSIFIER_PROMPT.format(message=message))])
    classification = result.content.strip().upper().split()[0].rstrip(".,")
    if classification not in {"ORDER_INQUIRY", "STATUS_ONLY", "GENERAL_QUESTION", "CLARIFICATION"}:
        classification = "CLARIFICATION"
    state["intent"] = classification
    return state


async def fetch_order_status(state: AgentState) -> AgentState:
    log.info("Node: fetch_order_status")
    if not state.get("order_no"):
        return state
    try:
        result = await call_mcp("/tools/get_order_status", {"order_no": state["order_no"]})
        state["order_status"] = result
    except Exception as e:
        log.error(f"get_order_status failed: {e}")
        state["order_status"] = {"found": False, "error": str(e)}
    return state


async def analyze_logs(state: AgentState) -> AgentState:
    log.info("Node: analyze_logs")
    if not state.get("order_no"):
        return state
    try:
        result = await call_mcp("/tools/analyze_order_logs", {
            "order_no": state["order_no"],
            "additional_context": state["user_message"],
            "top_k": 20,
        })
        state["log_evidence"] = result
    except Exception as e:
        log.error(f"analyze_order_logs failed: {e}")
        state["log_evidence"] = {"log_chunks": [], "error": str(e)}
    return state


async def _expand_to_technical_keywords(description: str) -> str:
    """Use the router LLM to convert business language into technical search terms."""
    llm = get_router_llm()
    result = await llm.ainvoke([
        HumanMessage(content=KEYWORD_EXPANSION_PROMPT.format(description=description))
    ])
    expanded = result.content.strip().splitlines()[0].strip()
    log.info(f"Keyword expansion: '{description[:60]}' → '{expanded[:120]}'")
    return expanded if expanded else description


async def find_similar(state: AgentState) -> AgentState:
    log.info("Node: find_similar_incidents")
    description = state["user_message"]

    if state.get("log_evidence", {}).get("log_chunks"):
        # Prefer actual error log text — already has specific exception names
        for chunk in state["log_evidence"]["log_chunks"]:
            if chunk.get("has_error"):
                description = chunk["message"][:500]
                break
        else:
            # Logs found but none with errors — expand business language
            description = await _expand_to_technical_keywords(description)
    else:
        # No log evidence at all — expand business language to technical terms
        description = await _expand_to_technical_keywords(description)

    try:
        result = await call_mcp("/tools/find_similar_incidents", {
            "description": description, "top_k": 3,
        })
        state["similar_incidents"] = result.get("incidents", [])
    except Exception as e:
        log.error(f"find_similar_incidents failed: {e}")
        state["similar_incidents"] = []
    return state


def _format_evidence(chunks: list) -> str:
    """Format log chunks into clearly separated transactions with error highlights.
    Pre-extracting errors per chunk prevents the LLM from conflating multiple
    distinct failure scenarios into a single incorrect narrative."""
    if not chunks:
        return "No log evidence found."

    sections = []
    for i, c in enumerate(chunks[:10], 1):
        log_lines = [l.strip() for l in c.get("message", "").split("\n") if l.strip()]

        error_lines = [l for l in log_lines if "] ERROR:" in l or "] WARN:" in l]
        error_summary = (
            "  Errors/Warnings detected:\n" +
            "\n".join(f"    • {l}" for l in error_lines)
        ) if error_lines else "  No errors — all steps completed successfully."

        sections.append(
            f"--- TRANSACTION {i} ---\n"
            f"Time range : {c.get('earliest_ts', '')[:19]} → {c.get('latest_ts', '')[:19]}\n"
            f"Services   : {', '.join(c.get('services', []))}\n"
            f"Has errors : {c.get('has_error', False)}\n"
            f"{error_summary}\n\n"
            f"Full log:\n{c.get('message', '')}"
        )

    return "\n\n" + ("=" * 60 + "\n").join(sections)


async def synthesize_rca(state: AgentState) -> AgentState:
    """Build the prompt and let the LLM synthesize the RCA."""
    log.info("Node: synthesize_rca")

    status_str   = json.dumps(state.get("order_status", {"found": False}), indent=2)
    evidence_str = _format_evidence(state.get("log_evidence", {}).get("log_chunks", []))

    incidents     = state.get("similar_incidents", [])
    incidents_str = "\n\n".join([
        f"[{i['incident_id']}] {i['summary']}\nRoot Cause: {i['root_cause']}\nResolution: {i['resolution']}"
        for i in incidents
    ]) or "No similar past incidents found."

    prompt = RCA_SYNTHESIS_PROMPT.format(
        order_status=status_str,
        log_evidence=evidence_str,
        similar_incidents=incidents_str,
    )

    llm = get_synthesis_llm()
    result = await llm.ainvoke([
        SystemMessage(content="You are an expert SRE."),
        HumanMessage(content=prompt),
    ])
    state["response"] = result.content
    return state


async def status_only_response(state: AgentState) -> AgentState:
    """Just format the structured status into plain English."""
    log.info("Node: status_only_response")
    if not state.get("order_no"):
        state["response"] = "Please tell me the order number (e.g., ORD-00042)."
        return state

    status = state.get("order_status") or {}
    if not status.get("found"):
        state["response"] = f"I couldn't find order {state['order_no']}."
        return state

    location = status.get("location_name", "unknown location")
    region = status.get("region", "")
    state["response"] = (
        f"**Order {status['order_no']}**\n\n"
        f"- Location: {location} ({region})\n"
        f"- Status: **{status.get('status')}**"
        + (f" — failed at step: {status.get('failed_step')}" if status.get('failed_step') else "")
        + f"\n- Amount: {status.get('amount')} {status.get('currency')}\n"
        f"- Payment: {status.get('payment_status', 'n/a')} via {status.get('payment_method', 'n/a')}"
        + (f"\n  Failure reason: {status['payment_failure_reason']}" if status.get('payment_failure_reason') else "")
        + (f"\n- Shipment: {status.get('shipment_status')} via {status.get('carrier')} (tracking: {status.get('tracking_id')})"
           if status.get('shipment_status') else "")
    )
    return state


async def happy_path_summary(state: AgentState) -> AgentState:
    """Format executed steps for orders whose logs contain no errors.
    Purely programmatic — no LLM involved, zero hallucination risk."""
    log.info("Node: happy_path_summary")

    order_no = state.get("order_no", "unknown")
    status   = state.get("order_status") or {}
    chunks   = state.get("log_evidence", {}).get("log_chunks", [])

    lines = [f"**Order {order_no} — Executed Steps**\n"]

    # ── Current status from DB ──────────────────────────────────────────────
    if status.get("found"):
        lines.append(
            f"**Current Status**: {status.get('status')} | "
            f"Amount: {status.get('amount')} {status.get('currency')} | "
            f"Location: {status.get('location_name')} ({status.get('region')})"
        )
        if status.get("payment_status"):
            line = f"**Payment**: {status['payment_status']} via {status.get('payment_method', 'N/A')}"
            if status.get("payment_failure_reason"):
                line += f" — {status['payment_failure_reason']}"
            lines.append(line)
        if status.get("shipment_status"):
            lines.append(
                f"**Shipment**: {status['shipment_status']} via "
                f"{status.get('carrier', 'N/A')} "
                f"(tracking: {status.get('tracking_id', 'N/A')})"
            )
        lines.append("")

    # ── Log timeline ────────────────────────────────────────────────────────
    if not chunks:
        lines.append("_No log evidence found for this order in the current index._")
        state["response"] = "\n".join(lines)
        return state

    for i, chunk in enumerate(chunks, 1):
        header = (
            f"**Transaction {i}**  "
            f"({chunk.get('earliest_ts', '')[:19]} → {chunk.get('latest_ts', '')[:19]})"
            f"  services: {', '.join(chunk.get('services', []))}"
        )
        lines.append(header)

        log_lines = [l.strip() for l in chunk.get("message", "").split("\n") if l.strip()]
        for j, entry in enumerate(log_lines, 1):
            lines.append(f"  {j}. {entry}")
        lines.append("")

    state["response"] = "\n".join(lines)
    return state


async def clarify_response(state: AgentState) -> AgentState:
    state["response"] = (
        "I'd be happy to help — could you tell me the order number? "
        "It looks like `ORD-00042` (with the prefix and 5-digit number)."
    )
    return state


async def general_response(state: AgentState) -> AgentState:
    state["response"] = (
        "I'm an assistant specialized in order log analysis and RCA. "
        "Ask me about a specific order using its order number (e.g., 'Why did ORD-00042 fail?')."
    )
    return state


# =============================================================================
# ROUTING
# =============================================================================
def route_after_logs(state: AgentState) -> Literal["rca", "happy_path"]:
    evidence = state.get("log_evidence") or {}
    chunks   = evidence.get("log_chunks", [])
    if chunks and not evidence.get("has_errors", False):
        return "happy_path"
    return "rca"


def route_after_classify(state: AgentState) -> Literal["evidence", "status_only", "general", "clarify"]:
    intent = state.get("intent")
    if intent == "ORDER_INQUIRY":
        return "evidence"
    if intent == "STATUS_ONLY":
        return "status_only"
    if intent == "GENERAL_QUESTION":
        return "general"
    return "clarify"


# =============================================================================
# BUILD THE GRAPH
# =============================================================================
def build_graph():
    graph = StateGraph(AgentState)

    graph.add_node("classify", classify_intent)
    graph.add_node("fetch_status_inquiry", fetch_order_status)
    graph.add_node("analyze_logs", analyze_logs)
    graph.add_node("happy_path_summary", happy_path_summary)
    graph.add_node("find_similar", find_similar)
    graph.add_node("synthesize", synthesize_rca)
    graph.add_node("fetch_status_only", fetch_order_status)
    graph.add_node("status_response", status_only_response)
    graph.add_node("clarify", clarify_response)
    graph.add_node("general", general_response)

    graph.set_entry_point("classify")

    graph.add_conditional_edges("classify", route_after_classify, {
        "evidence":     "fetch_status_inquiry",
        "status_only":  "fetch_status_only",
        "general":      "general",
        "clarify":      "clarify",
    })

    graph.add_edge("fetch_status_inquiry", "analyze_logs")

    # After log retrieval: happy-path logs → clean step summary (no LLM)
    #                      error logs / no logs → RCA pipeline
    graph.add_conditional_edges("analyze_logs", route_after_logs, {
        "happy_path": "happy_path_summary",
        "rca":        "find_similar",
    })
    graph.add_edge("happy_path_summary", END)

    graph.add_edge("find_similar", "synthesize")
    graph.add_edge("synthesize", END)

    graph.add_edge("fetch_status_only", "status_response")
    graph.add_edge("status_response", END)
    graph.add_edge("clarify", END)
    graph.add_edge("general", END)

    return graph.compile()


# =============================================================================
# FASTAPI APP
# =============================================================================
app = FastAPI(title="GenAI RCA Orchestrator")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

agent = build_graph()
redis_client: Optional[redis.Redis] = None


@app.on_event("startup")
async def _startup():
    global redis_client
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        log.info("✅ Redis connected.")
    except Exception as e:
        log.warning(f"Redis unavailable, sessions disabled: {e}")
        redis_client = None


class ChatRequest(BaseModel):
    message: str
    session_id: str = "default"


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.post("/api/v1/chat")
async def chat(req: ChatRequest):
    """Non-streaming version (simpler, for testing with curl)."""
    state = {
        "user_message": req.message,
        "session_id": req.session_id,
        "order_no": None, "intent": None,
        "order_status": None, "log_evidence": None,
        "similar_incidents": None, "response": None,
    }
    result = await agent.ainvoke(state)
    return {
        "response": result.get("response", "Sorry, no response."),
        "intent": result.get("intent"),
        "order_no": result.get("order_no"),
        "evidence_count": len(result.get("log_evidence", {}).get("log_chunks", [])) if result.get("log_evidence") else 0,
    }


@app.post("/api/v1/chat/stream")
async def chat_stream(req: ChatRequest):
    """Streaming version via SSE — emits status events as each node executes."""
    async def event_stream():
        state = {
            "user_message": req.message,
            "session_id": req.session_id,
            "order_no": None, "intent": None,
            "order_status": None, "log_evidence": None,
            "similar_incidents": None, "response": None,
        }

        yield f"data: {json.dumps({'type': 'start'})}\n\n"

        # Stream node-by-node updates
        async for chunk in agent.astream(state):
            for node_name, node_state in chunk.items():
                # Emit a progress event
                payload = {"type": "node", "node": node_name}
                if node_name == "classify":
                    payload["intent"] = node_state.get("intent")
                    payload["order_no"] = node_state.get("order_no")
                elif node_name == "analyze_logs":
                    payload["chunks_found"] = len(
                        node_state.get("log_evidence", {}).get("log_chunks", [])
                    )
                yield f"data: {json.dumps(payload)}\n\n"

                # When we reach a terminal node with a response, emit it
                if node_state.get("response"):
                    yield f"data: {json.dumps({'type': 'response', 'content': node_state['response']})}\n\n"

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
