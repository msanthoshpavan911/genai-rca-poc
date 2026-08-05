# GenAI RCA — Module 1: Production Log Retrieval Layer

Adapts log retrieval to query already-populated OpenSearch indices directly and fetch correlated traces using the `loggingId` correlation field.

## Features

- **Direct OpenSearch Queries**: Reads directly from production OpenSearch indices (e.g. `fluentbit-csg-gr2v_app_launchpad-alias`).
- **Two-Step Trace Correlation**:
  1. Full-text search for `order_id` in configured log message field.
  2. Term filter expansion on `loggingId` to pull the complete microservice trace for each matching transaction.
- **24-Hour Default Window**: Automatically searches the last 24 hours if no `time_from` timestamp is specified.
- **Dynamic Configuration**: Fully configurable at server startup via environment variables (`OPENSEARCH_HOST`, `OPENSEARCH_PORT`, `OPENSEARCH_USER`, `OPENSEARCH_PASSWORD`, `OPENSEARCH_USE_SSL`).

## Architecture

```
                                +-----------------------------+
                                |  opensearch-mock-simulator  |
                                |  (or Prod OpenSearch)       |
                                |  Port 9200                  |
                                +--------------+--------------+
                                               ^
                                               | (AsyncOpenSearch)
                                               |
+-------------------+           +--------------+--------------+
|                   |  HTTP GET |                             |
|  API Client /     | --------> |  genai-rca-poc              |
|  Future Modules   |           |  Log Retrieval Server       |
|                   | <-------- |  Port 8001                  |
+-------------------+           +-----------------------------+
```

## Quick Start

### 1. External OpenSearch Mock Setup

To run an OpenSearch instance with mock log data for local testing, use the separate project `opensearch-mock-simulator`:

```powershell
cd C:\Mamidi\2026\opensearch-mock-simulator
docker compose up -d
python generator/generate_logs_opensearch.py
```

### 2. Run Log Retrieval Server

In `genai-rca-poc`:

```powershell
.\run.ps1 install
.\run.ps1 server
```

Server starts at `http://localhost:8001`.

### 3. API Usage

#### GET `/api/v1/logs`
Fetch correlated logs for an order ID.

**Query Parameters**:
- `order_id` (required): Order / Entity ID (e.g. `ORD-00042`)
- `project_id` (required): Project identifier (e.g. `app_launchpad`)
- `time_from` (optional): ISO8601 start time. Defaults to 24 hours ago.
- `time_to` (optional): ISO8601 end time. Defaults to now.

**Example**:
```bash
curl "http://localhost:8001/api/v1/logs?order_id=ORD-00042&project_id=app_launchpad"
```

#### POST `/api/v1/logs/trace`
Fetch a trace directly by `logging_id`.

**Request Body**:
```json
{
  "project_id": "app_launchpad",
  "logging_id": "trace-a1b2c3d4e5f6",
  "time_window_hours": 24
}
```

#### GET `/projects`
Lists configured projects and target OpenSearch indices.

#### GET `/healthz`
Cluster health status check.
