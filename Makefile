# =============================================================================
# GenAI RCA — Convenience Makefile
# =============================================================================
# The live architecture reads directly from an existing production OpenSearch
# index — there is no Kafka/ingestor step. See docs/STARTUP_V2.md.

.PHONY: help up down nuke seed-incidents generate-logs mcp orch monitor ui smoke

help:
	@echo "GenAI RCA — common commands:"
	@echo ""
	@echo "  make up                 Bring up local infra (OpenSearch, Postgres, Redis)"
	@echo "  make down               Stop infrastructure (preserves data)"
	@echo "  make nuke               Stop and DELETE all data"
	@echo "  make smoke              Run smoke test against all services"
	@echo ""
	@echo "  make install            Install Python deps for all services"
	@echo "  make ollama-pull        Pull required Ollama models"
	@echo ""
	@echo "  make seed-incidents     Seed historical incidents into OpenSearch"
	@echo "  make generate-logs      Seed 100 mock transactions directly into OpenSearch (local testing only)"
	@echo "  make generate-burst     Seed 1000 mock transactions directly into OpenSearch (local testing only)"
	@echo ""
	@echo "  make mcp                Run the MCP server (port 8001)"
	@echo "  make orch               Run the orchestrator (port 8000)"
	@echo "  make monitor            Run the proactive error monitor (Module 3, no port)"
	@echo "  make ui                 Serve the React UI (port 3000)"
	@echo ""
	@echo "  make demo               One-shot test: ask about a failed order"

up:
	cd infra && docker compose up -d
	@echo "Waiting for services to be healthy (≈ 20 sec)..."
	@sleep 20
	cd infra && docker compose ps

down:
	cd infra && docker compose down

nuke:
	cd infra && docker compose down -v

smoke:
	python scripts/smoke_test.py

install:
	@echo "Installing log-generator deps..."
	pip install -r services/log-generator/requirements.txt
	@echo "Installing mcp-server deps..."
	pip install -r services/mcp-server/requirements.txt
	@echo "Installing orchestrator deps..."
	pip install -r services/orchestrator/requirements.txt
	@echo "Installing monitor deps..."
	pip install -r services/monitor/requirements.txt

ollama-pull:
	ollama pull qwen2.5:7b
	ollama pull qwen2.5:3b

seed-incidents:
	python scripts/seed_incidents.py

generate-logs:
	python services/log-generator/generate_logs_opensearch.py --transactions 100

generate-burst:
	python services/log-generator/generate_logs_opensearch.py --transactions 1000

mcp:
	cd services/mcp-server && uvicorn server:app --port 8001 --reload

orch:
	cd services/orchestrator && uvicorn main:app --port 8000 --reload

monitor:
	cd services/monitor && python poller.py

ui:
	cd services/ui && python -m http.server 3000

demo:
	@echo ""
	@echo "📤 Asking: 'Why did ORD-00005 fail?'"
	@echo ""
	@curl -sS -X POST http://localhost:8000/api/v1/chat \
		-H "Content-Type: application/json" \
		-d '{"message": "Why did ORD-00005 fail?", "session_id": "demo", "project_id": "app_launchpad"}' \
		| python -m json.tool
