# LibreChat on Databricks Apps (DAB)

Run an **unmodified** [LibreChat](https://github.com/LibreChat-AI/LibreChat) as the front end
for an enterprise chatbot on **Databricks Apps**, deployed with a **Declarative Automation
Bundle (DAB)**, wired to:

1. **Databricks Unity Gateway** for **Model Services** and **MCP Services** addressed by
   three-level Unity Catalog namespaces (`catalog.schema.service_name`), using
   **On-Behalf-Of (OBO)** user authentication so every model and MCP call inherits the
   signed-in Databricks user's permissions, rate limits, guardrails and audit lineage.
2. **Lakebase Autoscaling (PostgreSQL 17 + `pgvector`)** for LibreChat's PostgreSQL
   dependency — the RAG / file-search vector store.
3. **MongoDB** for LibreChat's own database. By default an **ephemeral** `mongod` runs inside
   the app container; point `mongo_uri` at a managed MongoDB for durable history.

Nothing in LibreChat's source is modified. All integration happens through environment
variables, a rendered `librechat.yaml`, and a Node `--require` preload hook.

---

## Read this before you deploy

**LibreChat cannot use PostgreSQL as its primary database.** LibreChat v0.8.8 is
mongoose/MongoDB-only — there is no `postgres`, `drizzle`, `prisma` or `sequelize` code
anywhere in the repository. So Lakebase serves LibreChat's *one* PostgreSQL dependency, the
RAG API's `pgvector` store. Making Lakebase the primary store would mean forking LibreChat's
data layer, which defeats the "run it unmodified" goal.

**What that means for your data, with the default `mongo_uri`:**

| Data | Where it lives | Survives an app restart? |
|------|----------------|--------------------------|
| Conversations, users, agents, prompts, presets, files metadata | Embedded MongoDB (`/tmp`) | **No** |
| RAG / file-search embeddings | Lakebase PostgreSQL + `pgvector` | Yes |

Set the `mongo_uri` variable to an external managed MongoDB to make everything durable:

```bash
databricks bundle deploy -t prod -p <profile> \
  --var="mongo_uri=mongodb+srv://<user>:<pw>@<cluster>.mongodb.net/LibreChat?retryWrites=true"
```

The app detects a non-loopback host, skips the embedded `mongod` entirely, and uses your
connection string verbatim. That is the only change needed, and once it is in place
`compute_max_instances` can safely be raised above 1.

---

## Prerequisites

1. **Databricks CLI v1.20.0+**, authenticated:
   ```bash
   databricks --version
   databricks auth profiles
   ```
2. A workspace with **Databricks Apps**, **Lakebase Autoscaling** and **Unity Gateway** enabled.
3. **Outbound network access** from the app container. LibreChat is cloned and built at
   startup, so in an egress-restricted workspace these must be allowlisted in the egress
   policy — and the app **restarted afterwards**, since egress changes need a restart:

   | Host | Needed for |
   |------|-----------|
   | `github.com` | cloning LibreChat and its RAG API |
   | `registry.npmjs.org` | `npm ci` and the frontend build |
   | `fastdl.mongodb.org` | the `mongod` binary (first cold start only — then cached in the UC Volume) |
   | `pypi.org` | the RAG API's Python dependencies |

4. Permission to create a schema and volume in the catalog named by `state_catalog`
   (default `justinm_demo`).

---

## Quickstart

```bash
# 1. Validate
databricks bundle validate --strict -t dev -p <profile>

# 2. Deploy and start (this creates the app, Lakebase project, schema and volume)
databricks bundle deploy -t dev -p <profile>

# 3. Watch it come up — the first boot builds LibreChat from source
databricks apps logs librechat-dev --follow -p <profile>
```

`lifecycle.started: true` is set on the app, so `bundle deploy` brings up compute and
returns a URL; you do not need a separate `bundle run`. (`databricks apps logs` requires
OAuth auth, not a PAT.)

While LibreChat is building, the app serves a live status page showing progress for Lakebase,
MongoDB, the RAG API and Unity Gateway. `GET /api/databricks/health` returns the same state
as JSON.

### Cold starts are slow

LibreChat's `npm ci` plus Vite frontend build runs **in the container on every cold start**.
Expect roughly 10–25 minutes. Two things make this tolerable:

- `compute_size: XLARGE` (48 GB / up to 12 vCPU) — `LARGE` is 12 GB / 4 vCPU and will be
  painfully slow, possibly OOM against the 4 GB Node heap cap.
- `compute_min_instances: 1` so the container stays warm and the build happens on deploy
  rather than on user traffic.

The app binds its HTTP port within seconds of launch, so the platform's 10-minute startup
window is satisfied regardless; the build time is a user-experience cost, not a health-check
failure.

---

## Models & MCP Servers (three-level namespace)

Both URL shapes below were verified live against a workspace.

### Model Services (`catalog.schema.model_service_name`)

OpenAI-compatible, via Unity Gateway's
[unified API](https://docs.databricks.com/aws/en/ai-gateway/query-model-services):

```
POST https://<workspace-host>/ai-gateway/mlflow/v1/chat/completions
     {"model": "system.ai.claude-sonnet-5-5", ...}
```

Defaults: `claude-opus-5-5`, `claude-sonnet-5-5`, `claude-haiku-5-5`, `gpt-5-6-sol` (all
under `system.ai`). Conversation titles use `unity_gateway_title_model`
(`system.ai.claude-haiku-5-5`) because titles are generated on every new chat.

Embeddings default to **`system.ai.gte-large-en`** (1024-dim). Note that
`system.ai.databricks-gte-large-en` does *not* exist and returns `404 NOT_FOUND`.

### MCP Services (`catalog.schema.mcp_service_name`)

```
POST https://<workspace-host>/ai-gateway/mcp-services/<catalog>.<schema>.<name>
```

Defaults: `system.ai.dbsql`, `system.ai.genie_one_mcp`, `system.ai.web_search`.
List what your workspace actually has:

```bash
databricks ai-gateway list-model-services -p <profile>
databricks ai-gateway list-mcp-services   -p <profile>
```

### Overriding them

These are real bundle variables, resolved into the app's `config.env` at deploy time:

```bash
databricks bundle deploy -t dev -p <profile> \
  --var="unity_gateway_default_model=system.ai.claude-opus-5-5" \
  --var="unity_gateway_models=system.ai.claude-opus-5-5,main.ai.my_custom_model" \
  --var="unity_gateway_mcp_services=system.ai.dbsql,main.ai.my_custom_mcp"
```

> Do **not** try to use `${var.*}` inside `app/app.yaml` — bundle variables are not
> interpolated there and would reach the container literally. `app/app.yaml` holds literal
> defaults for UI-driven deploys; the DAB-managed copy lives in
> `resources/librechat.app.yml` under `config:`. Keep the two in sync.

> A new MCP service needs no code change. Titles are sanitized automatically, because
> LibreChat validates MCP titles against ``^[\p{L}\p{N}][\p{L}\p{N}\p{M}'’ -]*$`` —
> parentheses, dots and underscores in a `title` make LibreChat **reject the whole config
> and exit at startup**, so three-level names belong in `description` only.

---

## Repository structure

```text
.
├── databricks.yml                        # Bundle config and all tunable variables
├── resources/
│   ├── lakebase.postgres.yml             # Lakebase Autoscaling Postgres 17 project & branch
│   ├── librechat_state.schema.yml        # UC schema for cached runtime artifacts
│   ├── librechat_state.volume.yml        # UC Volume caching the mongod binary
│   └── librechat.app.yml                 # App resource, OBO scopes, runtime config, bindings
└── app/
    ├── app.yaml                          # Literal runtime config for UI-driven deploys
    ├── librechat.yaml                    # LibreChat config template (rendered at startup)
    ├── requirements.txt                  # Pinned Python dependencies
    └── scripts/
        ├── start_app.py                  # Orchestrator: ingress proxy, Lakebase, Mongo, RAG, LibreChat
        ├── start_mongodb.cjs             # Embedded ephemeral MongoDB launcher
        └── databricks_obo_preload.cjs    # Node preload hook injecting per-user OBO tokens
```

---

## How it works

### 1. Zero changes to LibreChat

`start_app.py` clones `LibreChat-AI/LibreChat` at `LIBRECHAT_VERSION` and runs it as-is.
Integration is entirely via environment variables (`MONGO_URI`, `DATABASE_URL`, `POSTGRES_*`,
`CONFIG_PATH`), a rendered `/tmp/librechat.yaml`, and
`NODE_OPTIONS="--require scripts/databricks_obo_preload.cjs"`.

### 2. Lakebase for PostgreSQL

Attaching the `postgres` app resource makes the platform inject `PGHOST`, `PGPORT`,
`PGDATABASE`, `PGUSER`, `PGSSLMODE=require` and `LAKEBASE_ENDPOINT`. The orchestrator calls
`w.postgres.generate_database_credential(endpoint=os.environ["LAKEBASE_ENDPOINT"])` to mint a
1-hour OAuth token, creates the `librechat` schema and the `vector` extension, and builds:

```text
postgresql://<PGUSER>:<OAUTH_TOKEN>@<PGHOST>:5432/<PGDATABASE>?sslmode=require
```

A background loop re-mints the token every 40 minutes and **restarts the RAG API** so the
child process picks up the new password — refreshing this process's environment alone is not
enough, and without the restart Lakebase auth silently breaks about an hour in.

> **Two different database names.** The Lakebase *resource* ID is hyphenated
> (`.../databases/databricks-postgres`) while the *PostgreSQL* database name is underscored
> (`databricks_postgres`). The app binding needs the hyphenated resource ID; `PGDATABASE`
> gets the underscored name automatically. Confirm with:
> `databricks postgres list-databases projects/<project>/branches/<branch> -p <profile>`

> **Deploy before you connect locally.** The app's service principal has
> `CAN_CONNECT_AND_CREATE`, which lets it create objects but not use schemas owned by someone
> else. It must create the `librechat` schema itself to own it. If you connect with your own
> credentials first and create that schema, the SP will hit `permission denied ... 42501`.

### 3. MongoDB

With a loopback `mongo_uri`, `start_mongodb.cjs` runs a standalone `mongod` on
`127.0.0.1:27017` backed by `/tmp/librechat-mongodb`. Because that store is ephemeral, the
ingress proxy watches `/api/auth/refresh` and auto-provisions the signed-in Databricks user
through LibreChat's own `/api/auth/register` and `/api/auth/login`, so nobody has to create
an account again after a restart.

Self-registration therefore has to stay enabled — so the proxy **rejects any registration
whose email does not match the Databricks-verified `x-forwarded-email`**. Without that gate,
any app-authorized user could register under a colleague's address and, because OBO tokens
are cached per email, pick up that colleague's Unity Gateway token.

`mongodb-memory-server` is only a devDependency of LibreChat's `api` workspace, so the
orchestrator verifies it resolves and installs it explicitly if not, rather than assuming.
The downloaded `mongod` binary is cached to the UC Volume so later cold starts don't need
`fastdl.mongodb.org`.

### 4. On-Behalf-Of authentication, failing closed

Databricks Apps forwards the signed-in user's token as `x-forwarded-access-token`.
`databricks_obo_preload.cjs` intercepts outbound `fetch` / `undici` / `https` calls to
`/ai-gateway/*` and swaps in that user's bearer token, resolved in this order:

1. the token on the in-flight request (`AsyncLocalStorage`) — the normal path;
2. a token cached for **that same caller's** verified email;
3. nothing.

There is deliberately **no "most recent user" fallback**. If no token resolves, the
`Authorization` header is *removed* so the call fails visibly with a 401 rather than silently
running as the app's service principal with different privileges. Set
`DATABRICKS_OBO_ALLOW_SP_FALLBACK=true` to accept that downgrade.

One documented exception: embeddings. The RAG API calls Unity Gateway through a loopback
bridge on `127.0.0.1:3081` and carries no user identity, so **embeddings run as the app's
service principal and are not user-attributed**.

---

## Known limitations

- **Streaming is capped at 120 seconds.** The Databricks Apps reverse proxy enforces a
  non-configurable 120-second per-request timeout and may buffer SSE. LibreChat streams chat
  over SSE, so a single generation longer than 120s returns a 504 **generated at the proxy,
  with nothing in the app logs**. Long agent/tool loops over the MCP servers are the most
  likely way to hit this.
- **One instance only** while MongoDB is embedded. Each replica would have its own database
  and its own per-boot JWT secret, so sessions and history would break at random. Pinned via
  `compute_min_instances: 1` / `compute_max_instances: 1`.
- **Session secrets are regenerated on every boot.** Correct while MongoDB is ephemeral
  (there is no persisted data to decrypt), but when you move to an external MongoDB, set
  `JWT_SECRET`, `JWT_REFRESH_SECRET`, `CREDS_KEY` and `CREDS_IV` from a Databricks secret
  scope so sessions and stored credentials survive restarts.
- **Meilisearch is disabled** (`SEARCH=false`); LibreChat's full-text conversation search is
  unavailable.
- **OBO scopes can be wiped by a destructive app update.** Re-check `user_api_scopes` after
  any out-of-band change to the app.

## Health & diagnostics

```bash
curl -s https://<app-url>/api/databricks/health | jq
```

Reports `ready`/`phase`, Lakebase status and host, MongoDB status (`running (ephemeral)` vs
`external`), RAG API status, and the resolved Unity Gateway host, models, embedding model,
MCP services, and whether an OBO user token was seen on the request.
