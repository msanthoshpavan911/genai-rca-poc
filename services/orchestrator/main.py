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

import asyncio
import json
import logging
import os
import re
from typing import Dict, List, Literal, Optional, TypedDict

import httpx
import redis.asyncio as redis
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from langgraph.graph import StateGraph, END
from pydantic import BaseModel


# =============================================================================
# CONFIG
# =============================================================================
MCP_BASE_URL     = os.getenv("MCP_BASE_URL", "http://localhost:8001")
GOOGLE_API_KEY   = os.getenv("GOOGLE_API_KEY")
SYNTHESIS_MODEL  = os.getenv("SYNTHESIS_MODEL", "gemini-2.0-flash-lite")
ROUTER_MODEL     = os.getenv("ROUTER_MODEL", "gemini-2.0-flash-lite")
REDIS_URL        = os.getenv("REDIS_URL", "redis://localhost:6379")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orchestrator")


# =============================================================================
# PROMPTS
# =============================================================================
INTENT_CLASSIFIER_PROMPT = """You classify user questions into one of these intents:
- ORDER_INQUIRY:    asking about a specific order's status, failure, history (mentions order number like ORD-00042)
- STATUS_ONLY:      asking only "what's the status" with no need for log analysis
- GENERAL_QUESTION: general questions not about a specific order
- CLARIFICATION:    too vague to act on, need to ask the user a follow-up

Respond with ONLY one word: ORDER_INQUIRY, STATUS_ONLY, GENERAL_QUESTION, or CLARIFICATION.

User question: {message}
Classification:"""


RCA_SYNTHESIS_PROMPT = """You are a senior Site Reliability Engineer analyzing application logs to produce a Root Cause Analysis (RCA).

ORDER STATUS:
{order_status}

LOG EVIDENCE (chronological):
{log_evidence}

SIMILAR PAST INCIDENTS:
{similar_incidents}

Produce an RCA in plain English using this exact structure:

**Summary**
One or two sentences stating what happened.

**What Happened**
A factual timeline citing specific timestamps and services.

**Root Cause**
The single underlying trigger.

**Contributing Factors**
Any pre-existing conditions that worsened the failure.

**Recommended Actions**
1. Immediate: ...
2. Short-term: ...
3. Long-term: ...

**Evidence**
Bullet list of the specific log timestamps and services you relied on.

STRICT RULES:
- Use ONLY facts present in the evidence above.
- If evidence is insufficient for any section, write "Insufficient evidence for [X]" instead of speculating.
- Distinguish symptoms (visible errors) from root cause (the trigger that produced them).
- Cite specific log timestamps and service names — do not paraphrase.
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
# LLM — Gemini REST API (works with all key formats including AQ. keys)
# =============================================================================
_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/models"


async def call_gemini(model: str, prompt: str, system: str = None, temperature: float = 0.0) -> str:
    """Call Gemini generateContent REST endpoint with retry on rate limit."""
    url = f"{_GEMINI_BASE_URL}/{model}:generateContent"
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": temperature},
    }
    if system:
        body["systemInstruction"] = {"parts": [{"text": system}]}

    for attempt in range(4):
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                url,
                headers={"x-goog-api-key": GOOGLE_API_KEY},
                json=body,
            )
        if resp.status_code == 429:
            log.warning(f"Gemini 429 detail: {resp.text}")
            try:
                retry_delay = resp.json()["error"]["details"][-1].get("retryDelay", "60s")
                wait = int(retry_delay.rstrip("s")) + 2
            except Exception:
                wait = 15 * (2 ** attempt)
            log.warning(f"Gemini rate limit (429), retrying in {wait}s (attempt {attempt + 1}/4)")
            await asyncio.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()["candidates"][0]["content"]["parts"][0]["text"]

    resp.raise_for_status()


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
    result = await call_gemini(ROUTER_MODEL, INTENT_CLASSIFIER_PROMPT.format(message=message), temperature=0.0)
    classification = result.strip().upper().split()[0].rstrip(".,")
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


async def find_similar(state: AgentState) -> AgentState:
    log.info("Node: find_similar_incidents")
    description = state["user_message"]
    if state.get("log_evidence", {}).get("log_chunks"):
        # Use the first error chunk's message as a richer signal
        for chunk in state["log_evidence"]["log_chunks"]:
            if chunk.get("has_error"):
                description = chunk["message"][:500]
                break
    try:
        result = await call_mcp("/tools/find_similar_incidents", {
            "description": description, "top_k": 3,
        })
        state["similar_incidents"] = result.get("incidents", [])
    except Exception as e:
        log.error(f"find_similar_incidents failed: {e}")
        state["similar_incidents"] = []
    return state


async def synthesize_rca(state: AgentState) -> AgentState:
    """Build the prompt and let the LLM stream the RCA."""
    log.info("Node: synthesize_rca")

    status_str = json.dumps(state.get("order_status", {"found": False}), indent=2)

    chunks = state.get("log_evidence", {}).get("log_chunks", [])
    evidence_str = "\n\n".join([
        f"[Chunk {i+1}] services={c['services']} levels={c['log_levels']}\n{c['message']}"
        for i, c in enumerate(chunks[:10])  # cap to 10 chunks for context budget
    ]) or "No log evidence found."

    incidents = state.get("similar_incidents", [])
    incidents_str = "\n\n".join([
        f"[{i['incident_id']}] {i['summary']}\nRoot Cause: {i['root_cause']}\nResolution: {i['resolution']}"
        for i in incidents
    ]) or "No similar past incidents found."

    prompt = RCA_SYNTHESIS_PROMPT.format(
        order_status=status_str,
        log_evidence=evidence_str,
        similar_incidents=incidents_str,
    )

    result = await call_gemini(
        SYNTHESIS_MODEL, prompt,
        system="You are an expert SRE.",
        temperature=0.2,
    )
    state["response"] = result
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

    # ORDER_INQUIRY pipeline: status → logs → similar → synthesize
    # (Run sequentially for simplicity; LangGraph supports true parallel too.)
    graph.add_edge("fetch_status_inquiry", "analyze_logs")
    graph.add_edge("analyze_logs", "find_similar")
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
agent = build_graph()
redis_client: Optional[redis.Redis] = None


@asynccontextmanager
async def lifespan(_):
    global redis_client
    if not GOOGLE_API_KEY:
        raise RuntimeError(
            "GOOGLE_API_KEY environment variable is not set. "
            "Run: $env:GOOGLE_API_KEY='your_key' before starting the orchestrator."
        )
    log.info("GOOGLE_API_KEY is set.")
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        await redis_client.ping()
        log.info("Redis connected.")
    except Exception as e:
        log.warning(f"Redis unavailable, sessions disabled: {e}")
        redis_client = None
    yield


app = FastAPI(title="GenAI RCA Orchestrator", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


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
