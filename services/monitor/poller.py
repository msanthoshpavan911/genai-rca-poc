"""
=============================================================================
Proactive Error Monitor — Module 3
=============================================================================

Polls every configured project's OpenSearch index every POLL_INTERVAL_SECONDS
(default 600 = 10 min) for new ERROR-level logs since the last checkpoint,
dedups to one incident per distinct loggingId, and for each new incident:
  1. Fetches the full correlated trace  (MCP: get_trace_by_logging_id)
  2. Runs it through the orchestrator's RCA synthesis engine
     (POST /api/v1/internal/analyze_incident) — reuses Module 2 unchanged
  3. Dispatches the resulting RCA to email (DL) and/or Slack

Checkpoints (last-seen timestamp per project) and already-alerted loggingIds
are stored in Redis so restarts and overlapping polls don't double-alert or
re-scan the same window. The project list itself is fetched from the MCP
server's /projects endpoint (not hardcoded here), so new projects added to
project_config.py show up automatically.

SMTP/Slack credentials aren't required to run this locally — if unset, the
poller logs a warning and skips that channel rather than failing, so
detection + synthesis can be validated before notification is wired up.

Run:
    python poller.py
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import List

import aiosmtplib
import httpx
import redis.asyncio as redis


# =============================================================================
# CONFIG
# =============================================================================
MCP_BASE_URL           = os.getenv("MCP_BASE_URL", "http://localhost:8001")
ORCHESTRATOR_BASE_URL  = os.getenv("ORCHESTRATOR_BASE_URL", "http://localhost:8000")
REDIS_URL              = os.getenv("REDIS_URL", "redis://localhost:6379")

POLL_INTERVAL_SECONDS  = int(os.getenv("POLL_INTERVAL_SECONDS", "600"))     # 10 min, per requirement
INITIAL_LOOKBACK_HOURS = int(os.getenv("INITIAL_LOOKBACK_HOURS", "1"))
ALERTED_TTL_SECONDS    = int(os.getenv("ALERTED_TTL_SECONDS", str(24 * 3600)))

SMTP_HOST         = os.getenv("SMTP_HOST", "")
SMTP_PORT         = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER         = os.getenv("SMTP_USER", "")
SMTP_PASSWORD     = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM         = os.getenv("SMTP_FROM", "genai-rca-alerts@example.com")
ALERT_EMAIL_DL    = os.getenv("ALERT_EMAIL_DL", "")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("monitor")


# =============================================================================
# REDIS: checkpoints + alert dedup
# =============================================================================
def _checkpoint_key(project_id: str) -> str:
    return f"monitor:checkpoint:{project_id}"


def _alerted_key(project_id: str, logging_id: str) -> str:
    return f"monitor:alerted:{project_id}:{logging_id}"


async def get_checkpoint(r: redis.Redis, project_id: str) -> str:
    val = await r.get(_checkpoint_key(project_id))
    if val:
        return val
    # First run for this project: don't scan all history, just a short
    # initial lookback so startup doesn't flood alerts for pre-existing errors.
    return (datetime.now(timezone.utc) - timedelta(hours=INITIAL_LOOKBACK_HOURS)).isoformat()


async def set_checkpoint(r: redis.Redis, project_id: str, ts: str):
    await r.set(_checkpoint_key(project_id), ts)


async def already_alerted(r: redis.Redis, project_id: str, logging_id: str) -> bool:
    return bool(await r.exists(_alerted_key(project_id, logging_id)))


async def mark_alerted(r: redis.Redis, project_id: str, logging_id: str):
    await r.setex(_alerted_key(project_id, logging_id), ALERTED_TTL_SECONDS, "1")


# =============================================================================
# NOTIFICATIONS
# =============================================================================
async def send_email_alert(subject: str, body: str):
    if not (SMTP_HOST and ALERT_EMAIL_DL):
        log.warning("SMTP not configured — skipping email alert (set SMTP_HOST + ALERT_EMAIL_DL)")
        return
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = ALERT_EMAIL_DL
    msg.set_content(body)
    try:
        await aiosmtplib.send(
            msg,
            hostname=SMTP_HOST,
            port=SMTP_PORT,
            username=SMTP_USER or None,
            password=SMTP_PASSWORD or None,
            start_tls=True,
        )
        log.info(f"✅ Email alert sent to {ALERT_EMAIL_DL}")
    except Exception as e:
        log.error(f"Email alert failed: {e}")


async def send_slack_alert(text: str):
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not configured — skipping Slack alert")
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(SLACK_WEBHOOK_URL, json={"text": text})
            resp.raise_for_status()
        log.info("✅ Slack alert sent")
    except Exception as e:
        log.error(f"Slack alert failed: {e}")


# =============================================================================
# POLL CYCLE
# =============================================================================
async def get_project_ids(http: httpx.AsyncClient) -> List[str]:
    resp = await http.get(f"{MCP_BASE_URL}/projects")
    resp.raise_for_status()
    return [p["project_id"] for p in resp.json()["projects"]]


async def process_incident(r: redis.Redis, http: httpx.AsyncClient, project_id: str, logging_id: str, sample: str):
    log.info(f"[{project_id}] New error incident: loggingId={logging_id} — {sample[:80]}")

    try:
        rca_resp = await http.post(
            f"{ORCHESTRATOR_BASE_URL}/api/v1/internal/analyze_incident",
            json={"project_id": project_id, "logging_id": logging_id},
            timeout=120.0,
        )
        rca_resp.raise_for_status()
        rca = rca_resp.json()
    except Exception as e:
        log.error(f"[{project_id}] analyze_incident failed for {logging_id}: {e}")
        return

    # Mark alerted regardless of outcome — a non-RCA result (e.g. insufficient
    # evidence) shouldn't be retried every 10 minutes until the TTL forgets it.
    await mark_alerted(r, project_id, logging_id)

    if not rca.get("is_rca"):
        log.info(f"[{project_id}] {logging_id} did not resolve to a root cause — skipping notification")
        return

    subject = f"[GenAI RCA] New error detected — {project_id} ({logging_id})"
    body = rca.get("response", "")
    await send_email_alert(subject, body)
    await send_slack_alert(f"*{subject}*\n{body[:1500]}")


async def poll_once(r: redis.Redis, http: httpx.AsyncClient):
    project_ids = await get_project_ids(http)

    for project_id in project_ids:
        since = await get_checkpoint(r, project_id)
        try:
            resp = await http.post(f"{MCP_BASE_URL}/tools/find_new_errors", json={
                "project_id": project_id, "since": since,
            })
            resp.raise_for_status()
            result = resp.json()
        except Exception as e:
            log.error(f"[{project_id}] find_new_errors failed: {e}")
            continue

        incidents = result.get("incidents", [])
        new_count = 0
        for incident in incidents:
            lid = incident["logging_id"]
            if await already_alerted(r, project_id, lid):
                continue
            await process_incident(r, http, project_id, lid, incident.get("sample_message", ""))
            new_count += 1

        # Advance the checkpoint only after the batch is processed — a
        # mid-batch crash simply re-scans this window on the next cycle,
        # which is safe because already_alerted() dedupes.
        await set_checkpoint(r, project_id, result["checked_until"])
        if new_count:
            log.info(f"[{project_id}] Processed {new_count} new incident(s).")


async def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()
    log.info(f"✅ Redis connected. Polling every {POLL_INTERVAL_SECONDS}s.")

    async with httpx.AsyncClient(timeout=30.0) as http:
        while True:
            try:
                await poll_once(r, http)
            except Exception as e:
                log.error(f"Poll cycle failed: {e}")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Stopped by user.")
