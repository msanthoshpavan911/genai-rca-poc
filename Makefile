# =============================================================================
# GenAI Log Analysis & RCA POC — Convenience Makefile
# =============================================================================

.PHONY: help up down nuke seed-incidents generate-logs ingest mcp orch ui smoke

help:
	@echo "GenAI RCA POC — common commands:"
	@echo ""
	@echo "  make up                 Bring up infrastructure (Kafka, OpenSearch, etc.)"
	@echo "  make down               Stop infrastructure (preserves data)"
	@echo "  make nuke               Stop and DELETE all data"
	@echo "  make smoke              Run smoke test against all services"
	@echo ""
	@echo "  make install            Install Python deps for all services"
	@echo "  make ollama-pull        Pull required Ollama models"
	@echo ""
	@echo "  make seed-incidents     Seed historical incidents into OpenSearch"
	@echo "  make generate-logs      Generate 100 mock transactions to Kafka"
	@echo "  make generate-burst     Generate 1000 mock transactions fast"
	@echo ""
	@echo "  make ingest             Run the Kafka -> vector ingestor"
	@echo "  make mcp                Run the MCP server (port 8001)"
	@echo "  make orch               Run the orchestrator (port 8000)"
	@echo "  make ui                 Serve the React UI (port 3000)"
	@echo ""
	@echo "  make demo               One-shot test: ask about a failed order"

up:
	cd infra && docker compose up -d
	@echo "Waiting for services to be healthy (≈ 30 sec)..."
	@sleep 25
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
	@echo "Installing ingestor deps..."
	pip install -r services/ingestor/requirements.txt
	@echo "Installing mcp-server deps..."
	pip install -r services/mcp-server/requirements.txt
	@echo "Installing orchestrator deps..."
	pip install -r services/orchestrator/requirements.txt

ollama-pull:
	ollama pull qwen2.5:7b
	ollama pull qwen2.5:3b

seed-incidents:
	python scripts/seed_incidents.py

generate-logs:
	python services/log-generator/generate_logs.py --transactions 100 --rate 5

generate-burst:
	python services/log-generator/generate_logs.py --transactions 1000 --burst

ingest:
	cd services/ingestor && python ingestor.py

mcp:
	cd services/mcp-server && uvicorn server:app --port 8001 --reload

orch:
	cd services/orchestrator && uvicorn main:app --port 8000 --reload

ui:
	cd services/ui && python -m http.server 3000

demo:
	@echo ""
	@echo "📤 Asking: 'Why did ORD-00005 fail?'"
	@echo ""
	@curl -sS -X POST http://localhost:8000/api/v1/chat \
		-H "Content-Type: application/json" \
		-d '{"message": "Why did ORD-00005 fail?", "session_id": "demo"}' \
		| python -m json.tool
