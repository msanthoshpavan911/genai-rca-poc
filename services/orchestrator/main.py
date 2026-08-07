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
from datetime import datetime, timezone
from typing import Annotated, Dict, List, Literal, Optional, TypedDict

import httpx
import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import StateGraph, END
from pydantic import BaseModel


# Automatically load .env file from project root or working directory
load_dotenv(override=True)



# =============================================================================
# CONFIG
# =============================================================================
MCP_BASE_URL     = os.getenv("MCP_BASE_URL", "http://localhost:8001")
OLLAMA_BASE_URL  = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
REDIS_URL        = os.getenv("REDIS_URL", "redis://localhost:6379")

# LLM Provider Config: "gemini" / "google" (via Google AI Studio) or "ollama" (local)
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
LLM_PROVIDER     = os.getenv("LLM_PROVIDER", "gemini" if GEMINI_API_KEY else "ollama").lower()
GEMINI_MODEL     = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

SYNTHESIS_MODEL  = os.getenv("SYNTHESIS_MODEL", GEMINI_MODEL if LLM_PROVIDER in ("gemini", "google") else "qwen2.5:7b")
ROUTER_MODEL     = os.getenv("ROUTER_MODEL", GEMINI_MODEL if LLM_PROVIDER in ("gemini", "google") else "qwen2.5:3b")


# Used when the client doesn't yet send a project_id (e.g. old curl demos).
# Module 4 (chat UI) is expected to make project selection explicit per session.
DEFAULT_PROJECT_ID = os.getenv("DEFAULT_PROJECT_ID", "app_launchpad")

# Persona picked in the chat UI, locked for the session (Module 4/5). Only
# changes wording/detail level in the RCA — never the section structure, so
# _parse_rca_sections (write-back) keeps working regardless of persona.
DEFAULT_PERSONA = "technical"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orchestrator")


# =============================================================================
# PROMPTS
# =============================================================================
PERSONA_INSTRUCTIONS = {
    "technical": (
        "AUDIENCE: a technical/SRE consultant. Use precise technical language — exception "
        "class names, stack trace details, service/component names, and infrastructure terms "
        "(connection pools, circuit breakers, HTTP status codes, cache TTLs, etc). Assume the "
        "reader can act directly on engineering-level detail."
    ),
    "business": (
        "AUDIENCE: a business user with no technical background. Do NOT use stack traces, "
        "exception class names, or infrastructure jargon in the main narrative (e.g. instead "
        "of 'HikariCP connection pool exhausted', say 'the system couldn't connect to the "
        "database in time'). Focus on customer/business impact: what broke, who or what was "
        "affected, and what is being done about it, in plain English a non-engineer can "
        "follow. Technical specifics may still appear in the Evidence section since that is "
        "meant to be a literal citation of the logs, not prose."
    ),
}

KEYWORD_EXPANSION_PROMPT = """You are a technical search assistant helping find similar past incidents in a microservice order-management system.

Convert the business description below into technical search keywords. Think about: Java exception names, Spring Boot concepts, infrastructure components (HikariCP, Redis, Kafka, PSP gateway), HTTP status codes, and performance terms.

Business description: {description}

Respond with ONLY a single line of space-separated lowercase keywords. No explanation, no punctuation, no bullet points.

Keywords:"""


INTENT_CLASSIFIER_PROMPT = """You classify user questions into one of these intents:
- ORDER_INQUIRY:    asking about a specific order's failure, history, or logs (mentions order number like ORD-00042)
- GENERAL_QUESTION: general questions not about a specific order
- CLARIFICATION:    too vague to act on, need to ask the user a follow-up

Respond with ONLY one word: ORDER_INQUIRY, GENERAL_QUESTION, or CLARIFICATION.

User question: {message}
Classification:"""

TIME_WINDOW_EXTRACTION_PROMPT = """Extract a time window from the user message below.

Current UTC datetime: {now}

Rules:
- Specific date ("7th July", "July 7", "07/07/2026") → full calendar day in UTC (00:00:00 to 23:59:59).
- "yesterday" → the previous calendar day.
- "last Monday", "last week", etc. → compute from the current date above.
- Time hint ("around 2pm", "at 14:00") → 2-hour window centred on that time on the mentioned date.
- No date or time mentioned → return null for both fields.

Respond with ONLY valid JSON, no explanation, no markdown:
{{"start": "<ISO datetime or null>", "end": "<ISO datetime or null>"}}

User message: {message}
JSON:"""


RCA_SYNTHESIS_PROMPT = """You are a senior Site Reliability Engineer reviewing order activity.

{persona_instructions}

LOG EVIDENCE:
{log_evidence}

SIMILAR PAST INCIDENTS:
{similar_incidents}

{format_directive}

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
FORMAT B — Insufficient Evidence
=========================

**Summary**
One or two sentences stating that no log evidence was found for this entity in the searched time window.

**What Happened**
State plainly that no log evidence was available to reconstruct a timeline. Do not speculate or fabricate a sequence of events.

**Evidence**
Write "No log evidence found."

==============================
RULES:
- Output only the chosen format — no headers like "STEP 1", "FORMAT A", "IF ERRORS", etc.
- Do not add any title or heading before **Summary** (e.g. do NOT write "Order Summary" or
  "Root Cause Analysis" as a leading line) — the response must begin directly with **Summary**.
- Use only facts from the log evidence. Never fabricate failures not present in the logs.
- A single ERROR anywhere in the evidence means FORMAT A applies to the whole response, even if
  other parts of the same order completed successfully (e.g. payment captured but shipping
  failed is still a failure requiring root cause analysis — do not downgrade it to a summary
  just because some steps succeeded).
- If the same root cause appears across multiple transactions, state it once.
- Cite timestamps and service names directly from the evidence.
- If a transaction is marked with a "⚠ Only one service present" coverage warning,
  treat it as a partial slice of the request, not the complete picture. Do not assert
  a definitive single-service root cause from it alone — say the evidence appears
  incomplete and name what's missing (e.g. "no downstream service logs correlated to
  this request") rather than guessing what happened elsewhere.
- The audience instructions above govern WORDING and level of technical detail only.
  Always keep the exact section headers (**Summary**, **What Happened**, **Root Cause**,
  **Contributing Factors**, **Recommended Actions**, **Evidence**) as given — never rename,
  merge, or drop them regardless of audience.
"""


# =============================================================================
# STATE
# =============================================================================
class AgentState(TypedDict):
    user_message: str
    session_id: str
    project_id: str
    persona: str
    order_no: Optional[str]
    intent: Optional[str]
    time_window_start: Optional[str]   # ISO datetime extracted from user message
    time_window_end:   Optional[str]   # ISO datetime; None = open-ended (use default look-back)
    log_evidence: Optional[Dict]
    similar_incidents: Optional[List[Dict]]
    response: Optional[str]
    rca_sections: Optional[Dict[str, str]]


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


async def extract_time_window(message: str) -> tuple[Optional[str], Optional[str]]:
    """Ask the router LLM to extract a time window from the user's message.
    Returns (start_iso, end_iso) or (None, None) if no date is mentioned."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    llm = get_router_llm()
    try:
        result = await llm.ainvoke([
            HumanMessage(content=TIME_WINDOW_EXTRACTION_PROMPT.format(now=now, message=message))
        ])
        raw = result.content.strip()
        # The model sometimes wraps JSON in markdown fences — strip them
        raw = re.sub(r"^```[a-z]*\n?|```$", "", raw, flags=re.MULTILINE).strip()
        parsed = json.loads(raw)
        start = parsed.get("start") or None
        end   = parsed.get("end")   or None
        if start:
            log.info(f"Time window extracted: {start} → {end or 'open'}")
        return start, end
    except Exception as e:
        log.warning(f"Time window extraction failed ({e}), using default look-back")
        return None, None


async def call_mcp(tool_path: str, payload: dict) -> dict:
    """Call an MCP tool over HTTP."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{MCP_BASE_URL}{tool_path}", json=payload)
        resp.raise_for_status()
        return resp.json()


# =============================================================================
# LLMs (Google AI Studio Gemini API Only — Ollama Disabled)
# =============================================================================
def _resolve_gemini_config():
    load_dotenv()
    gemini_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not gemini_key:
        raise ValueError(
            "GEMINI_API_KEY is not set in environment or .env file! "
            "Ollama has been completely disabled. Please set GEMINI_API_KEY."
        )
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-flash-latest")
    
    raw_router = os.getenv("ROUTER_MODEL", gemini_model)
    raw_synthesis = os.getenv("SYNTHESIS_MODEL", gemini_model)
    
    router_model = raw_router if any(raw_router.startswith(prefix) for prefix in ("gemini", "gemma")) else gemini_model
    synthesis_model = raw_synthesis if any(raw_synthesis.startswith(prefix) for prefix in ("gemini", "gemma")) else gemini_model

    return gemini_key, router_model, synthesis_model


def get_router_llm():
    gemini_key, router_model, _ = _resolve_gemini_config()
    log.info(f"Using Google AI Studio Gemini API (model: {router_model}) for Router LLM")
    return ChatGoogleGenerativeAI(
        model=router_model,
        google_api_key=gemini_key,
        temperature=0.0,
    )


def get_synthesis_llm():
    gemini_key, _, synthesis_model = _resolve_gemini_config()
    log.info(f"Using Google AI Studio Gemini API (model: {synthesis_model}) for Synthesis LLM")
    return ChatGoogleGenerativeAI(
        model=synthesis_model,
        google_api_key=gemini_key,
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
        state["intent"] = "ORDER_INQUIRY"
        start, end = await extract_time_window(message)
        state["time_window_start"] = start
        state["time_window_end"]   = end
        return state

    # No order number — ask the LLM
    llm = get_router_llm()
    result = await llm.ainvoke([HumanMessage(content=INTENT_CLASSIFIER_PROMPT.format(message=message))])
    classification = result.content.strip().upper().split()[0].rstrip(".,")
    if classification not in {"ORDER_INQUIRY", "GENERAL_QUESTION", "CLARIFICATION"}:
        classification = "CLARIFICATION"
    state["intent"] = classification
    return state


async def analyze_logs(state: AgentState) -> AgentState:
    log.info("Node: analyze_logs")
    if not state.get("order_no"):
        return state
    try:
        payload = {
            "project_id": state["project_id"],
            "order_no":   state["order_no"],
            "additional_context": state["user_message"],
            "top_k": 3,
        }
        if state.get("time_window_start"):
            payload["time_window_start"] = state["time_window_start"]
        if state.get("time_window_end"):
            payload["time_window_end"] = state["time_window_end"]
        result = await call_mcp("/tools/analyze_order_logs", payload)
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
    distinct failure scenarios into a single incorrect narrative.

    Production logs are correlated by loggingId, which is not guaranteed to
    propagate across every service hop (unlike the POC's synthetic trace_id).
    A chunk touching only one service may be a partial slice of a larger
    failure, not the whole story — flagged here so the synthesis prompt can
    hedge instead of asserting a confident single-service root cause."""
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

        services = c.get("services", [])
        partial_flag = (
            "\nCoverage   : ⚠ Only one service present — this may be a PARTIAL "
            "trace if the correlation ID didn't propagate across every service "
            "involved. Do not assume this is the full request lifecycle."
            if len(services) <= 1 else ""
        )

        sections.append(
            f"--- TRANSACTION {i} ---\n"
            f"Time range : {c.get('earliest_ts', '')[:19]} → {c.get('latest_ts', '')[:19]}\n"
            f"Services   : {', '.join(services)}"
            f"{partial_flag}\n"
            f"Has errors : {c.get('has_error', False)}\n"
            f"{error_summary}\n\n"
            f"Full log:\n{c.get('message', '')}"
        )

    return "\n\n" + ("=" * 60 + "\n").join(sections)


def _format_directive(chunks: list) -> str:
    """Decide FORMAT A vs FORMAT B in code, not in the LLM's head.

    synthesize_rca is only ever reached when chunks is non-empty with at
    least one ERROR (route_after_logs already intercepted the "chunks exist,
    no errors" case and sent it to the non-LLM happy_path_summary instead),
    or when chunks is empty (no evidence found at all). Leaving this as an
    LLM judgment call caused it to pick FORMAT B ("order completed
    successfully") for orders that had a real ERROR alongside unrelated
    successful steps (e.g. payment captured but shipping failed) — it was
    weighing overall outcome instead of "was there a confirmed error." Since
    we already know the answer from the MCP tool's has_error flag, just
    tell it."""
    if chunks:
        return (
            "The evidence below has already been confirmed programmatically to contain at least "
            "one ERROR-level log entry. You MUST write FORMAT A — Root Cause Analysis — below. "
            "This applies even if other parts of the same order completed successfully elsewhere "
            "(e.g. payment captured despite a shipping failure) — any confirmed error requires a "
            "full root cause analysis, never FORMAT B."
        )
    return (
        "No log evidence was found for this entity in the searched time window. Write FORMAT B "
        "below, stating plainly that evidence is insufficient — do not guess or fabricate a "
        "timeline."
    )


async def synthesize_rca(state: AgentState) -> AgentState:
    """Build the prompt and let the LLM synthesize the RCA."""
    log.info("Node: synthesize_rca")

    chunks = state.get("log_evidence", {}).get("log_chunks", [])
    evidence_str = _format_evidence(chunks)

    incidents     = state.get("similar_incidents", [])
    incidents_str = "\n\n".join([
        f"[{i['incident_id']}] {i['summary']}\nRoot Cause: {i['root_cause']}\nResolution: {i['resolution']}"
        for i in incidents
    ]) or "No similar past incidents found."

    persona = state.get("persona") or DEFAULT_PERSONA
    persona_instructions = PERSONA_INSTRUCTIONS.get(persona, PERSONA_INSTRUCTIONS[DEFAULT_PERSONA])

    prompt = RCA_SYNTHESIS_PROMPT.format(
        persona_instructions=persona_instructions,
        format_directive=_format_directive(chunks),
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


def _parse_rca_sections(rca_text: str) -> Optional[Dict[str, str]]:
    """Pull Summary/Root Cause/Recommended Actions out of the structured RCA
    markdown (FORMAT A in RCA_SYNTHESIS_PROMPT) so it can be written back to
    incidents-historical. Returns None if this isn't a real RCA (e.g. it's a
    FORMAT B order summary with no root cause) — nothing to save in that case."""
    if "**Root Cause**" not in rca_text:
        return None

    def _section(name: str) -> str:
        pattern = rf"\*\*{re.escape(name)}\*\*\s*\n(.*?)(?=\n\*\*|\Z)"
        m = re.search(pattern, rca_text, re.DOTALL)
        return m.group(1).strip() if m else ""

    return {
        "summary": _section("Summary"),
        "root_cause": _section("Root Cause"),
        "resolution": _section("Recommended Actions"),
    }


async def save_incident(state: AgentState) -> AgentState:
    """Push the completed RCA back into incidents-historical for future
    find_similar_incidents lookups. A tracking side effect only — failures
    here must never block the response already computed for the user.

    Also stores the parsed sections on state as `rca_sections`, so callers
    (the /api/v1/chat response, and later Module 3's poller) can tell a real
    RCA apart from a happy-path/status response without re-parsing markdown."""
    log.info("Node: save_incident")
    sections = _parse_rca_sections(state.get("response", ""))
    state["rca_sections"] = sections
    if not sections:
        return state
    try:
        await call_mcp("/tools/save_incident", {
            "order_no": state.get("order_no"),
            "project_id": state.get("project_id"),
            "summary": sections["summary"],
            "root_cause": sections["root_cause"],
            "resolution": sections["resolution"],
            "source": "auto-generated",
        })
    except Exception as e:
        log.error(f"save_incident failed: {e}")
    return state


async def happy_path_summary(state: AgentState) -> AgentState:
    """Format executed steps for orders whose logs contain no errors.
    Purely programmatic — no LLM involved, zero hallucination risk.

    For the business persona, the raw per-line log dump (service names,
    log levels, raw timestamps) is skipped in favor of a one-line plain
    confirmation — that dump is only useful to a technical reader."""
    log.info("Node: happy_path_summary")

    order_no = state.get("order_no", "unknown")
    chunks   = state.get("log_evidence", {}).get("log_chunks", [])
    persona  = state.get("persona") or DEFAULT_PERSONA

    lines = [f"**Order {order_no} — Executed Steps**\n"]

    # ── Log timeline ────────────────────────────────────────────────────────
    if not chunks:
        lines.append("_No log evidence found for this order in the current index._")
        state["response"] = "\n".join(lines)
        return state

    if persona == "business":
        lines.append(f"✅ No issues found — {len(chunks)} transaction(s) completed without errors.")
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


async def no_evidence_response(state: AgentState) -> AgentState:
    order_no = state.get("order_no", "the requested order")
    state["response"] = (
        f"No log evidence was found for **{order_no}** in the current index. "
        "This usually means the order does not exist in the logs, "
        "falls outside the search window, or has not been processed yet. "
        "Please verify the order number and try again."
    )
    state["is_rca"] = False
    state["evidence_count"] = 0
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
def route_after_logs(state: AgentState) -> Literal["rca", "happy_path", "no_evidence"]:
    evidence = state.get("log_evidence") or {}
    chunks   = evidence.get("log_chunks", [])
    if not chunks:
        return "no_evidence"
    if not evidence.get("has_errors", False):
        return "happy_path"
    return "rca"


def route_after_classify(state: AgentState) -> Literal["evidence", "general", "clarify"]:
    intent = state.get("intent")
    if intent == "ORDER_INQUIRY":
        return "evidence"
    if intent == "GENERAL_QUESTION":
        return "general"
    return "clarify"


# =============================================================================
# BUILD THE GRAPH
# =============================================================================
def build_graph():
    graph = StateGraph(AgentState)

    graph.add_node("classify", classify_intent)
    graph.add_node("analyze_logs", analyze_logs)
    graph.add_node("happy_path_summary", happy_path_summary)
    graph.add_node("no_evidence", no_evidence_response)
    graph.add_node("find_similar", find_similar)
    graph.add_node("synthesize", synthesize_rca)
    graph.add_node("save_incident", save_incident)
    graph.add_node("clarify", clarify_response)
    graph.add_node("general", general_response)

    graph.set_entry_point("classify")

    graph.add_conditional_edges("classify", route_after_classify, {
        "evidence": "analyze_logs",
        "general":  "general",
        "clarify":  "clarify",
    })

    # After log retrieval: no logs found → no_evidence message (no LLM, no hallucination)
    #                      logs with no errors → clean step summary (no LLM)
    #                      logs with errors → full RCA pipeline
    graph.add_conditional_edges("analyze_logs", route_after_logs, {
        "no_evidence": "no_evidence",
        "happy_path":  "happy_path_summary",
        "rca":         "find_similar",
    })
    graph.add_edge("no_evidence", END)
    graph.add_edge("happy_path_summary", END)

    graph.add_edge("find_similar", "synthesize")
    graph.add_edge("synthesize", "save_incident")
    graph.add_edge("save_incident", END)

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
    project_id: str = DEFAULT_PROJECT_ID
    persona: str = DEFAULT_PERSONA


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/api/v1/projects")
async def list_projects_for_ui():
    """Passthrough to the MCP server's project list, so the chat UI only
    ever talks to the orchestrator and never calls MCP directly."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(f"{MCP_BASE_URL}/projects")
        resp.raise_for_status()
        return resp.json()


@app.post("/api/v1/chat")
async def chat(req: ChatRequest):
    """Non-streaming version (simpler, for testing with curl)."""
    state = {
        "user_message": req.message,
        "session_id": req.session_id,
        "project_id": req.project_id,
        "persona": req.persona,
        "order_no": None, "intent": None,
        "time_window_start": None, "time_window_end": None,
        "log_evidence": None,
        "similar_incidents": None, "response": None,
        "rca_sections": None,
    }
    result = await agent.ainvoke(state)
    return {
        "response": result.get("response", "Sorry, no response."),
        "intent": result.get("intent"),
        "order_no": result.get("order_no"),
        "evidence_count": len(result.get("log_evidence", {}).get("log_chunks", [])) if result.get("log_evidence") else 0,
        "is_rca": result.get("rca_sections") is not None,
    }


@app.post("/api/v1/chat/stream")
async def chat_stream(req: ChatRequest):
    """Streaming version via SSE — emits status events as each node executes."""
    async def event_stream():
        state = {
            "user_message": req.message,
            "session_id": req.session_id,
            "project_id": req.project_id,
            "persona": req.persona,
            "order_no": None, "intent": None,
            "time_window_start": None, "time_window_end": None,
            "log_evidence": None,
            "similar_incidents": None, "response": None,
            "rca_sections": None,
        }

        yield f"data: {json.dumps({'type': 'start'})}\n\n"

        # Stream node-by-node updates
        response_sent = False
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

                # Emit the response exactly once. synthesize sets it first, but
                # the save_incident node runs after synthesize on the RCA path
                # and still carries the same response forward — guard so the
                # client doesn't receive it twice. is_rca is derived directly
                # from the text (same check _parse_rca_sections uses) rather
                # than from state["rca_sections"], since that field isn't
                # populated yet at the point synthesize's response first appears.
                if node_state.get("response") and not response_sent:
                    response_sent = True
                    is_rca = "**Root Cause**" in node_state["response"]
                    yield f"data: {json.dumps({'type': 'response', 'content': node_state['response'], 'is_rca': is_rca})}\n\n"

        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# =============================================================================
# INTERNAL: proactive incident analysis (Module 3's poller entry point)
# =============================================================================
class AnalyzeIncidentRequest(BaseModel):
    project_id: str
    logging_id: str
    # Proactive alerts default to the technical persona since they go to an
    # engineering DL/Slack channel, not a live user who picked their own persona.
    persona: str = DEFAULT_PERSONA


@app.post("/api/v1/internal/analyze_incident")
async def analyze_incident(req: AnalyzeIncidentRequest):
    """Entry point for the proactive error poller: given a project and a
    loggingId already known to contain a fresh ERROR (from find_new_errors),
    fetch its full trace and run it through the exact same RCA synthesis
    engine the chatbot uses (_format_evidence, RCA_SYNTHESIS_PROMPT,
    _parse_rca_sections, save_incident) — reused unchanged, not duplicated.
    There is no user_message or order_no here; this isn't a chat turn."""
    log.info(f"Node: analyze_incident (proactive) project={req.project_id} loggingId={req.logging_id}")

    try:
        trace_resp = await call_mcp("/tools/get_trace_by_logging_id", {
            "project_id": req.project_id,
            "logging_id": req.logging_id,
        })
    except Exception as e:
        log.error(f"get_trace_by_logging_id failed: {e}")
        return {"response": "Could not retrieve trace.", "is_rca": False, "logging_id": req.logging_id}

    if not trace_resp.get("found"):
        return {"response": "Trace no longer available.", "is_rca": False, "logging_id": req.logging_id}

    chunk = trace_resp["chunk"]
    evidence_str = _format_evidence([chunk])

    # No user question to search with — fall back to the raw error text
    # itself, same as the chat flow's find_similar fallback when no
    # error-bearing chunk is available to seed the description.
    description = chunk["message"][:500]
    try:
        similar_resp = await call_mcp("/tools/find_similar_incidents", {
            "description": description, "top_k": 3,
        })
        similar_incidents = similar_resp.get("incidents", [])
    except Exception as e:
        log.error(f"find_similar_incidents failed: {e}")
        similar_incidents = []

    incidents_str = "\n\n".join([
        f"[{i['incident_id']}] {i['summary']}\nRoot Cause: {i['root_cause']}\nResolution: {i['resolution']}"
        for i in similar_incidents
    ]) or "No similar past incidents found."

    persona_instructions = PERSONA_INSTRUCTIONS.get(req.persona, PERSONA_INSTRUCTIONS[DEFAULT_PERSONA])
    prompt = RCA_SYNTHESIS_PROMPT.format(
        persona_instructions=persona_instructions,
        format_directive=_format_directive([chunk]),
        order_status=json.dumps(
            {"found": False, "note": "Proactively detected — no order lookup performed."}, indent=2
        ),
        log_evidence=evidence_str,
        similar_incidents=incidents_str,
    )

    llm = get_synthesis_llm()
    result = await llm.ainvoke([
        SystemMessage(content="You are an expert SRE."),
        HumanMessage(content=prompt),
    ])
    response_text = result.content

    sections = _parse_rca_sections(response_text)
    if sections:
        try:
            await call_mcp("/tools/save_incident", {
                "project_id": req.project_id,
                "summary": sections["summary"],
                "root_cause": sections["root_cause"],
                "resolution": sections["resolution"],
                "source": "proactive-alert",
            })
        except Exception as e:
            log.error(f"save_incident failed: {e}")

    return {
        "response": response_text,
        "is_rca": sections is not None,
        "logging_id": req.logging_id,
    }
