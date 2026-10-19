# LibreChat on Databricks Apps (DAB)

Deploy an **unmodified** instance of [LibreChat](https://github.com/LibreChat-AI/LibreChat) as a **Databricks App** using a **Databricks Asset Bundle (DAB)**, wired out of the box to:

1. **Lakebase Autoscaling (PostgreSQL 17 + `pgvector`)** for LibreChat's persistent PostgreSQL / RAG storage — changing only the PostgreSQL connection string from LibreChat's perspective.
2. **Embedded Ephemeral MongoDB** running inside the same Databricks App compute (`MONGO_URI=mongodb://127.0.0.1:27017/LibreChat`) — changing only the MongoDB connection string from LibreChat's perspective.
3. **[Databricks Unity Gateway](https://docs.databricks.com/aws/en/ai-gateway/)** for both **Model Services** and **MCP Services** addressed via three-level Unity Catalog namespaces (`catalog.schema.service_name`), using **On-Behalf-Of (OBO)** user authentication (`x-forwarded-access-token`) so every model and MCP call inherits the logged-in Databricks user's Unity Catalog permissions, rate limits, guardrails, and audit lineage.

---

## Architecture

```text
┌───────────────────────────────────────────────────────────────────────────────────┐
│ Databricks User Browser (SSO Authenticated)                                       │
└─────────────────────────────────────────┬─────────────────────────────────────────┘
                                          │ HTTPS
                                          ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│ Databricks Apps Reverse Proxy                                                     │
│ Injects: x-forwarded-access-token (OBO user token), x-forwarded-email             │
└─────────────────────────────────────────┬─────────────────────────────────────────┘
                                          │
                                          ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│ Databricks App Compute (LARGE)                                                    │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────────┐  │
│  │ 1. Ingress Proxy & Orchestrator (scripts/start_app.py on $DATABRICKS_APP_PORT)│
│  │    • Captures x-forwarded-access-token & x-forwarded-email per user         │  │
│  │    • Auto-provisions Databricks SSO user into ephemeral MongoDB             │  │
│  │    • Mints & refreshes 1-hr Lakebase OAuth credentials (PGSSLMODE=require)  │  │
│  └──────────────────────────────┬──────────────────────────────────────────────┘  │
│                                 │ HTTP / SSE / WS (127.0.0.1:3080)                │
│                                 ▼                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐  │
│  │ 2. Unmodified LibreChat (https://github.com/LibreChat-AI/LibreChat)         │  │
│  │    + NODE_OPTIONS="--require scripts/databricks_obo_preload.cjs"            │  │
│  │    • Zero code modifications to LibreChat                                   │  │
│  │    • Preload hook swaps outbound Unity Gateway Authorization header with    │  │
│  │      the user's OBO Bearer token (x-forwarded-access-token)                 │  │
│  └──────┬───────────────────────────────┬───────────────────────────────┬──────┘  │
│         │                               │                               │         │
│         │ MONGO_URI                     │ RAG_API_URL                   │ HTTPS   │
│         ▼                               ▼                               │ (OBO)   │
│  ┌──────────────────────────┐    ┌───────────────────────────────────┐  │         │
│  │ 3. Embedded MongoDB      │    │ 4. Unmodified LibreChat RAG API   │  │         │
│  │ 127.0.0.1:27017/LibreChat│    │ 127.0.0.1:8001 (pgvector client)  │  │         │
│  │ (Ephemeral on app compute│    └─────────────────┬─────────────────┘  │         │
│  └──────────────────────────┘                      │                    │         │
└────────────────────────────────────────────────────┼────────────────────┼─────────┘
                                                     │                    │
                     DATABASE_URL (sslmode=require)  │                    │
                     + Lakebase OAuth Token          │                    │
                                                     ▼                    ▼
                            ┌──────────────────────────────┐   ┌──────────────────────────────────────────┐
                            │ Lakebase Autoscaling Project │   │ Databricks Unity Gateway                 │
                            │ PostgreSQL 17 + pgvector     │   │                                          │
                            │ • Branch: production         │   │ • Models (/ai-gateway/mlflow/v1):        │
                            │ • DB: databricks_postgres    │   │   catalog.schema.model_service_name      │
                            │ • Schema: librechat          │   │   e.g. system.ai.claude-sonnet-4-5       │
                            └──────────────────────────────┘   │                                          │
                                                               │ • MCPs (/ai-gateway/mcp-services/...):   │
                                                               │   catalog.schema.mcp_service_name        │
                                                               │   e.g. system.ai.dbsql,                  │
                                                               │        system.ai.genie_one_mcp           │
                                                               └──────────────────────────────────────────┘
```

---

## Repository Structure

```text
.
├── databricks.yml                      # Root DAB configuration & customizable variables
├── resources/
│   ├── lakebase.postgres.yml           # Lakebase Autoscaling Postgres 17 project & branch
│   └── librechat.app.yml               # Databricks App resource, OBO scopes, and Lakebase binding
└── app/
    ├── app.yaml                        # Databricks App runtime command & environment variables
    ├── librechat.yaml                  # LibreChat config for Unity Gateway 3-level Models & MCPs
    ├── requirements.txt                # Python dependencies for orchestrator & Lakebase setup
    └── scripts/
        ├── start_app.py                # App entrypoint: Lakebase auth, MongoDB, RAG API, LibreChat & OBO proxy
        ├── start_mongodb.cjs           # Embedded ephemeral MongoDB launcher (127.0.0.1:27017)
        └── databricks_obo_preload.cjs  # Node preload hook injecting OBO tokens on Unity Gateway calls
```

---

## Prerequisites

1. **Databricks CLI** (`v0.250.0+` or `v1.x` with Lakebase & Apps bundle support) installed and authenticated:
   ```bash
   databricks --version
   databricks auth profiles
   ```
2. A Databricks workspace with:
   - **Databricks Apps** enabled
   - **Lakebase Autoscaling** enabled
   - **[Unity Gateway](https://docs.databricks.com/aws/en/ai-gateway/)** enabled

---

## Quickstart

### 1. Validate the Bundle

```bash
databricks bundle validate -p <your-databricks-profile>
```

### 2. Deploy the Lakebase Project and Databricks App

```bash
databricks bundle deploy -p <your-databricks-profile>
```

This provisions:
- A **Lakebase Autoscaling PostgreSQL 17 project** (`librechat-lakebase-dev` in `dev` target or `librechat-lakebase` in `prod` target) with its default `production` branch and `databricks_postgres` database.
- A **Databricks App** (`librechat-dev` in `dev` or `librechat` in `prod`) with `CAN_CONNECT_AND_CREATE` permission on the Lakebase database and On-Behalf-Of `user_api_scopes` (`ai-gateway`, `model-serving`, `sql`, `genie`, `files`, `vector-search`, `postgres`, `iam.current-user:read`, `iam.access-control:read`).

### 3. Run / Start the Databricks App

```bash
databricks bundle run librechat_app -p <your-databricks-profile>
```

Once started, open the Databricks App URL from the CLI output or the **Compute → Apps** UI in your Databricks workspace. During initial boot (while LibreChat clones and builds its frontend assets), the app serves a live status dashboard showing real-time progress for Lakebase PostgreSQL, Embedded MongoDB, Unity Gateway, and LibreChat.

---

## Configuring Models & MCP Servers (Three-Level Namespace)

### Model Services (`catalog.schema.model_service_name`)
LibreChat queries foundation and custom models through Unity Gateway's [OpenAI-compatible Unified API](https://docs.databricks.com/aws/en/ai-gateway/query-model-services):
- **Endpoint Base URL**: `https://${DATABRICKS_HOST}/ai-gateway/mlflow/v1`
- **Default Models**:
  - `system.ai.claude-sonnet-4-5`
  - `system.ai.claude-opus-4-5`
  - `system.ai.gpt-5-6-sol`
- **Default Embedding Model (for RAG + Lakebase `pgvector`)**:
  - `system.ai.databricks-gte-large-en`

### MCP Services (`catalog.schema.mcp_service_name`)
LibreChat connects to governed [MCP servers through Unity Gateway](https://docs.databricks.com/aws/en/agents/mcp-tools/) using `streamable-http` transport:
- **Endpoint URL Pattern**: `https://${DATABRICKS_HOST}/ai-gateway/mcp-services/<catalog>.<schema>.<mcp_service_name>`
- **Default MCP Services**:
  - `system.ai.dbsql` — Databricks SQL MCP server
  - `system.ai.genie_one_mcp` — Databricks Genie One MCP server
  - `system.ai.web_search` — Databricks Web Search MCP server

### Overriding Models or MCP Services
You can customize the three-level namespace services in `app/app.yaml`, `app/librechat.yaml`, or via DAB variables in `databricks.yml`:

```bash
databricks bundle deploy -p <your-databricks-profile> \
  --var="unity_gateway_default_model=system.ai.claude-sonnet-4-5" \
  --var="unity_gateway_models=system.ai.claude-sonnet-4-5,system.ai.gpt-5-6-sol,main.ai.my_custom_model" \
  --var="unity_gateway_mcp_services=system.ai.dbsql,system.ai.genie_one_mcp,main.ai.my_custom_mcp"
```

---

## How It Works Under the Hood

### 1. Zero Changes to Core LibreChat
- `app/scripts/start_app.py` clones the official upstream `https://github.com/LibreChat-AI/LibreChat` repository at the tag specified by `LIBRECHAT_VERSION` (default `v0.8.8`) and runs it unmodified.
- All Databricks integrations happen purely through environment variables (`MONGO_URI`, `DATABASE_URL`, `POSTGRES_*`, `CONFIG_PATH`) and a Node.js `--require` preload module (`app/scripts/databricks_obo_preload.cjs`).

### 2. Lakebase Autoscaling for PostgreSQL (`pgvector`)
- Attaching the `postgres` resource in `resources/librechat.app.yml` causes Databricks Apps to inject `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGSSLMODE=require`, and `LAKEBASE_ENDPOINT`.
- `app/scripts/start_app.py` calls `w.postgres.generate_database_credential(endpoint=os.environ["LAKEBASE_ENDPOINT"])` to mint an OAuth token (automatically refreshed in the background before the 1-hour expiry), initializes the `librechat` schema and `vector` (`pgvector`) extension, and constructs the standard PostgreSQL connection string:
  ```text
  postgresql://<PGUSER>:<OAUTH_TOKEN>@<PGHOST>:5432/<PGDATABASE>?sslmode=require
  ```

### 3. Embedded Ephemeral MongoDB & Seamless SSO User Provisioning
- `app/scripts/start_mongodb.cjs` starts a local `mongod` instance inside the Databricks App container on `127.0.0.1:27017` backed by `/tmp/librechat-mongodb`, exposing `MONGO_URI=mongodb://127.0.0.1:27017/LibreChat`.
- Because `/tmp/librechat-mongodb` is ephemeral when the Databricks App compute spins down, the ingress proxy inspects `/api/auth/refresh` requests and automatically registers/logs in the authenticated Databricks user (`x-forwarded-email`) using LibreChat's native `/api/auth/register` and `/api/auth/login` endpoints. Users never have to manually create an account after an app restart, and `{{LIBRECHAT_USER_EMAIL}}` in LibreChat always matches the caller's Databricks identity.

### 4. On-Behalf-Of (OBO) Authentication to Unity Gateway
- Databricks Apps forwards the logged-in user's OAuth token in the `x-forwarded-access-token` request header.
- `app/librechat.yaml` passes `x-librechat-user-email: "{{LIBRECHAT_USER_EMAIL}}"` on both `endpoints.custom` (Unity Gateway Models) and `mcpServers` (Unity Gateway MCPs), which also instructs LibreChat to isolate MCP connections per user (`startup: false`).
- `app/scripts/databricks_obo_preload.cjs` intercepts outbound `fetch` / `undici` / `https` requests targeting `https://${DATABRICKS_HOST}/ai-gateway/*` and replaces the `Authorization` header with `Bearer <x-forwarded-access-token>` for that user (with automatic fallback to the App Service Principal token if no user token is present).

---

## Health & Diagnostics

At any time while the app is running, you can query `/api/databricks/health` on the Databricks App URL to inspect the live status of Lakebase, Embedded MongoDB, the RAG API, and OBO token detection:

```json
{
  "ready": true,
  "phase": "LibreChat is running and ready.",
  "error": null,
  "lakebase": {
    "status": "connected",
    "host": "<endpoint>.database.cloud.databricks.com",
    "database": "databricks_postgres"
  },
  "mongodb": {
    "status": "running",
    "uri": "mongodb://127.0.0.1:27017/LibreChat"
  },
  "rag_api": {
    "status": "running"
  },
  "unity_gateway": {
    "host": "<workspace>.cloud.databricks.com",
    "obo_user_detected": true,
    "obo_user_email": "user@company.com",
    "models": [
      "system.ai.claude-sonnet-4-5",
      "system.ai.claude-opus-4-5",
      "system.ai.gpt-5-6-sol"
    ],
    "mcp_services": [
      "system.ai.dbsql",
      "system.ai.genie_one_mcp",
      "system.ai.web_search"
    ]
  }
}
```
