# LibreChat on Databricks Apps (DAB)

Run an **unmodified** [LibreChat](https://github.com/LibreChat-AI/LibreChat) as the front end
for an enterprise chatbot on **Databricks Apps**, deployed with a **Declarative Automation
Bundle (DAB)**, wired to:

1. **Databricks Unity Gateway** for **Model Services** and **MCP Services** addressed by
   three-level Unity Catalog namespaces (`catalog.schema.service_name`), using
   **On-Behalf-Of (OBO)** user authentication so every model and MCP call inherits the
   signed-in Databricks user's permissions, rate limits, guardrails and audit lineage.
2. **PostgreSQL + `pgvector`** for LibreChat's PostgreSQL dependency — the RAG / file-search
   vector store. By default an **ephemeral** PostgreSQL 16 runs inside the app container;
   point `postgres_uri` at any PostgreSQL you host for a durable index.
3. **MongoDB** for LibreChat's own database. By default an **ephemeral** `mongod` runs inside
   the app container; point `mongo_uri` at a managed MongoDB for durable history.

No Lakebase is required, so this deploys into workspaces without Lakebase access (for
example, compliance-security-profile workspaces).

Nothing in LibreChat's source is modified. All integration happens through environment
variables, a rendered `librechat.yaml`, and a Node `--require` preload hook.

---

## Read this before you deploy

**LibreChat cannot use PostgreSQL as its primary database.** LibreChat v0.8.8 is
mongoose/MongoDB-only — there is no `postgres`, `drizzle`, `prisma` or `sequelize` code
anywhere in the repository. So PostgreSQL serves LibreChat's *one* PostgreSQL dependency, the
RAG API's `pgvector` store. Making PostgreSQL the primary store would mean forking LibreChat's
data layer, which defeats the "run it unmodified" goal.

**What that means for your data, with the default `mongo_uri` and `postgres_uri`:**

| Data | Where it lives | Survives an app restart? |
|------|----------------|--------------------------|
| Conversations, users, agents, prompts, presets, files metadata | Embedded MongoDB (`/tmp`) | **No** |
| RAG / file-search embeddings | Embedded PostgreSQL + `pgvector` (`/tmp`) | **No** |

Point either variable at a server you host to make that store durable:

```bash
databricks bundle deploy -t prod -p <profile> \
  --var="mongo_uri=mongodb+srv://<user>:<pw>@<cluster>.mongodb.net/LibreChat?retryWrites=true" \
  --var="postgres_uri=postgresql://<user>:<pw>@<host>:5432/<db>?sslmode=require"
```

For each one, the app detects a non-loopback host, skips the embedded server entirely, and
uses your connection string as-is. That is the only change needed. Once **both** are
external, `compute_max_instances` can safely be raised above 1.

The external PostgreSQL must have the `pgvector` extension available. The app runs
`CREATE SCHEMA IF NOT EXISTS librechat` and `CREATE EXTENSION IF NOT EXISTS vector` on
startup; if its role may not create extensions, have an administrator run the latter once.
`sslmode` defaults to `require` for an external host; add `?sslmode=...` to override.

> **Keep credentials out of plain env values.** Both URIs carry a password. For a real
> deployment, store them in a Databricks secret scope, attach the secret as an app resource,
> and reference it with `valueFrom` instead of `value` in `resources/librechat.app.yml`.

---

## Prerequisites

1. **Databricks CLI v1.20.0+**, authenticated:
   ```bash
   databricks --version
   databricks auth profiles
   ```
2. A workspace with **Databricks Apps** and **Unity Gateway** enabled. Lakebase is not used.
3. **Outbound network access** from the app container. LibreChat is cloned and built at
   startup, so in an egress-restricted workspace these must be allowlisted in the egress
   policy — and the app **restarted afterwards**, since egress changes need a restart:

   | Host | Needed for |
   |------|-----------|
   | `github.com` | cloning LibreChat and its RAG API |
   | `registry.npmjs.org` | `npm ci` and the frontend build |
   | `nodejs.org` | a build-capable Node, if the container's own is too old (first cold start only — then cached in the UC Volume) |
   | `fastdl.mongodb.org` | the `mongod` binary (first cold start only — then cached in the UC Volume) |
   | `pypi.org` | the RAG API's Python dependencies, and `pgserver` (the embedded PostgreSQL binaries) at deploy time |

4. Permission to create a schema and volume in the catalog named by `state_catalog`
   (default `justinm_demo`).

---

## Quickstart

```bash
# 1. Validate
databricks bundle validate --strict -t dev -p <profile>

# 2. Deploy and start (this creates the app, schema and volume)
databricks bundle deploy -t dev -p <profile>

# 3. Watch it come up — the first boot builds LibreChat from source
databricks apps logs librechat-dev --follow -p <profile>
```

`lifecycle.started: true` is set on the app, so `bundle deploy` brings up compute and
returns a URL; you do not need a separate `bundle run`. (`databricks apps logs` requires
OAuth auth, not a PAT.)

While LibreChat is building, the app serves a live status page showing progress for PostgreSQL,
MongoDB, the RAG API and Unity Gateway. `GET /api/databricks/health` returns the same state
as JSON.

### Use `python`, not `python3`

The Apps runtime is **Python 3.11 on Ubuntu 22.04**. It installs `requirements.txt` into its
own virtualenv and exposes that interpreter on PATH as **`python`**. A command of `python3`
can instead resolve to Ubuntu's system Python 3.10, which has none of those packages, and the
app dies on its first third-party import:

```
ModuleNotFoundError: No module named 'aiohttp'
```

That error names `aiohttp` only because it is the first third-party import in
`start_app.py` -- it is an interpreter problem, not a missing requirement.
`start_app.py` now wraps those imports and prints the interpreter, version, `sys.prefix` and
`sys.base_prefix` before re-raising, so the cause is obvious. If `sys.prefix ==
sys.base_prefix`, the app is not running inside the virtualenv.

Python dependencies (`app/requirements.txt`, all pinned, all with 3.11 manylinux wheels):
`databricks-sdk`, `aiohttp`, `psycopg2-binary`, `PyYAML`, `uvicorn`. The runtime
pre-installs Dash, Streamlit, Gradio, Flask, FastAPI and Shiny, none of which this app uses.
Note that `psycopg2` is **not** pre-installed and must stay listed.

### Node toolchain

The Databricks Apps image ships **Node v22.16.0 / npm 10.9.2** (verified from app logs,
2026-10). LibreChat v0.8.8's build toolchain needs newer: `tsdown@0.22.2` declares
`engines: ^22.18.0 || >=24.0.0`, and `@babel/*@8.0.0-rc.6` wants `^22.18.0 || >=24.11.0`.
`npm ci` only *warns* about engine mismatches (`npm warn EBADENGINE`) and installs anyway,
so the failure surfaces later as:

```
ℹ tsdown v0.22.2 powered by rolldown v1.1.0
 ERROR  Error: Failed to import module "unrun". Please ensure it is installed.
npm error workspace librechat-data-provider@0.8.527
RuntimeError: Command npm run frontend exited with code 1
```

That is a red herring. `unrun` is an **optional peer dependency** of `tsdown`
(`"unrun": "*"` in its `peerDependencies`) and appears nowhere in LibreChat's
`package-lock.json`, so npm is right to skip it. On Node >= 22.18 `tsdown` loads its
TypeScript config natively and never needs `unrun`; on 22.16 it falls back to it and fails.

So `start_app.py` provisions its own Node before building: if the container's Node already
satisfies `MIN_BUILD_NODE` it is used as-is, otherwise the pinned official build
(`PINNED_NODE_VERSION`, currently `v24.22.0` LTS) is downloaded from `nodejs.org`,
**verified against a pinned SHA-256**, unpacked under `/tmp/node-toolchain`, and cached in
the UC Volume for later cold starts. That Node is then used for `npm ci`, the frontend build,
the embedded `mongod`, and the LibreChat server itself, so everything shares one version.

`node_modules` is tagged with the Node version that installed it
(`/tmp/librechat/.dbx-node-version`); if the build Node changes, dependencies are reinstalled
rather than reused against a different ABI.

To bump it, change `PINNED_NODE_VERSION` **and** `PINNED_NODE_SHA256` together — the digest
is enforced, so a stale pair fails the download rather than running unverified code. Get the
digest from `https://nodejs.org/dist/<version>/SHASUMS256.txt`.

### Cold starts are slow

LibreChat's `npm ci` plus Vite frontend build runs **in the container on every cold start**.
Measured on XLARGE, `npm ci` installs 2,988 packages in about 40 seconds; the frontend build
is the longer phase. Two things keep this tolerable:

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

#### "Unified" does not mean parameters are normalized

Unity Gateway exposes two unified, provider-agnostic APIs on the same `baseURL` --
`/chat/completions` and `/responses` -- and every `system.ai.claude-*` service lists both
in its `supported_api_types`. But *unified* describes the **request shape**, not the
**parameter surface**: the gateway still forwards sampling parameters to the provider,
and Anthropic rejects the ones OpenAI takes for granted.

LibreChat's conversation schema injects OpenAI sampling defaults on every request even
when nobody touches the parameters panel
([`schemas.ts`](https://github.com/danny-avila/LibreChat/blob/v0.8.8/packages/data-provider/src/schemas.ts)):
`temperature: 1`, `top_p: 1`, `presence_penalty: 0`, `frequency_penalty: 0`. Against a
Claude model service each of those is a hard `400`:

```
BAD_REQUEST: Model us.anthropic.claude-sonnet-5-5 does not support sampling parameters: top_p
BAD_REQUEST: Model us.anthropic.claude-sonnet-5-5 does not support the temperature parameter.
{"message":"presence_penalty: Extra inputs are not permitted"}
```

Switching to `/responses` does **not** help -- it returns the identical errors, and is in
fact *stricter* (it rejects even `temperature: 1`, which `/chat/completions` accepts).

The GPT services only appear to work because LibreChat already strips these parameters for
any model matching `/\b(o[13]|gpt-5)(?!\.|-chat)(?:-|$)/`
([`llm.ts`](https://github.com/danny-avila/LibreChat/blob/v0.8.8/packages/api/src/endpoints/openai/llm.ts))
— a pattern `claude-*` can never match. `system.ai.gpt-5-6-sol` matches it; every
`system.ai.claude-*` does not.

The fix is the `dropParams` list on the custom endpoint in `app/librechat.yaml`, which
`scripts/start_app.py` also injects so it cannot be lost. It extends that same stripping to
a non-OpenAI provider and tells the client to hide the corresponding sliders rather than
send values that fail. Measured against `system.ai.claude-sonnet-5-5`:

| Rejected by the gateway (dropped) | Accepted (kept) |
|---|---|
| `temperature` (except exactly `1`), `top_p`, `top_k`, `presence_penalty`, `frequency_penalty`, `logit_bias`, `logprobs`, `seed`, `user`, `parallel_tool_calls`, `response_format` | `max_tokens`, `max_completion_tokens`, `stop`, `n`, `stream`, `stream_options`, `reasoning_effort`, `tools`, `tool_choice`, `system` role |

Tool calling — what MCP depends on — works on Claude in both streaming and non-streaming
modes. `response_format` is dropped because the gateway reports *"Structured output is not
supported for this model"* for these Anthropic services, so it could only ever `400`.

If you add a model service that **does** support sampling (`llama-4-maverick`,
`gemini-*`), give it its own `custom` endpoint rather than widening this list, since
`dropParams` is per-endpoint and would needlessly strip controls that model supports.

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

#### The server key must not contain a dot

LibreChat names every model-facing tool `${toolName}_mcp_${serverName}`
(`Constants.mcp_delimiter`), where `serverName` is the **key** in the `mcpServers` map.
Anthropic validates tool names against `^[a-zA-Z0-9_-]{1,128}$` — no dots — while
LibreChat's own `normalizeServerName` allows `[a-zA-Z0-9_.-]` and passes a dotted name
straight through. Using a three-level FQN as the key therefore sends Claude
`web_search_mcp_system.ai.web_search`, and *every* tool-enabled request fails:

```json
{"error_code":"BAD_REQUEST",
 "message":"{\"message\":\"tools.0.custom.name: String should match pattern '^[a-zA-Z0-9_-]{1,128}$'\"}"}
```

Note this is a **model-side** failure, not an MCP one: the MCP handshake succeeds and the
tools load, then the chat request carrying them is rejected. So it looks like "MCP is
broken" when the tool *name* is what's wrong.

So the key is the underscored form (`system_ai_web_search`) and the three-level namespace
lives in `url` — which is what actually routes to the service — and in `description`.
Governance is unchanged; only LibreChat's internal identifier differs.
`scripts/start_app.py` (`safe_mcp_server_key`) applies the same mapping to anything named
in `unity_gateway_mcp_services`, so you can still write FQNs there. It also:

- caps the key at 60 characters with a short digest, since Unity Catalog permits 63
  characters *per level* (a ~191-character FQN would otherwise blow Anthropic's 128-char
  budget once the tool name and `_mcp_` are added), and
- disambiguates the case where two FQNs sanitize to the same key (`a.b.c` and `a_b.c`)
  instead of silently dropping one.

#### MCP tools with GPT and Gemini

Claude handles tools on `/chat/completions` as-is. The other providers each break a
different way behind Unity Gateway, all surfacing in LibreChat as
`The model provider could not complete this request (status 400)`:

| Models | Gateway error | Fix |
|--------|---------------|-----|
| GPT-5.6, GPT-6 (`sol`/`terra`/`luna`/`astra`), `gpt-5-5-pro` | `Function tools with reasoning_effort are not supported ... in /v1/chat/completions` (`gpt-5-5-pro`: Responses-only) | `start_app.py` moves them to a second endpoint, **Databricks Unity Gateway Responses API**, with `useResponsesApi: true` (`RESPONSES_API_MODELS`). Its titles use `UNITY_GATEWAY_RESPONSES_TITLE_MODEL` (default `system.ai.gpt-5-4-nano`). |
| Gemini | `Unknown name "stream_options"`, sent by LibreChat on every streamed request | The preload hook deletes `stream_options` for `gemini-*`. |
| Gemini 3 | `Function call is missing a thought_signature` on the turn after a tool call | The preload hook remembers each tool call's `thoughtSignature` and re-attaches it, falling back to Google's `skip_thought_signature_validator` if it is unknown (e.g. after a restart). |

LibreChat only routes GPT-5.6/GPT-6 to the Responses API on its own for `api.openai.com`,
and its OpenAI message conversion drops `thoughtSignature`, so neither can be fixed in
`librechat.yaml` alone. Rewrites are logged as `[UnityGateway-Compat]`.

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
│   ├── librechat_state.schema.yml        # UC schema for cached runtime artifacts
│   ├── librechat_state.volume.yml        # UC Volume caching the mongod binary
│   └── librechat.app.yml                 # App resource, OBO scopes, runtime config, bindings
└── app/
    ├── app.yaml                          # Literal runtime config for UI-driven deploys
    ├── librechat.yaml                    # LibreChat config template (rendered at startup)
    ├── requirements.txt                  # Pinned Python dependencies
    └── scripts/
        ├── start_app.py                  # Orchestrator: ingress proxy, Postgres, Mongo, RAG, LibreChat
        ├── start_mongodb.cjs             # Embedded ephemeral MongoDB launcher
        └── databricks_obo_preload.cjs    # Node preload hook injecting per-user OBO tokens
```

---

## How it works

### 1. Zero changes to LibreChat

`start_app.py` clones `LibreChat-AI/LibreChat` at `LIBRECHAT_VERSION` and runs it as-is.
Integration is entirely via environment variables (`MONGO_URI`, `CONFIG_PATH`, and the
RAG API's `DB_HOST` / `POSTGRES_*`), a rendered `/tmp/librechat.yaml`, and
`NODE_OPTIONS="--require scripts/databricks_obo_preload.cjs"`.

### 2. PostgreSQL for the RAG API

`POSTGRES_URI` follows the same contract as `MONGO_URI`. The orchestrator parses it into the
discrete settings the RAG API reads (`DB_HOST`, `DB_PORT`, `POSTGRES_DB`, `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `PGSSLMODE`), then creates the `librechat` schema and `vector`
extension.

With a loopback host (the default, `postgresql://librechat@127.0.0.1:5432/librechat`) it
runs an **ephemeral PostgreSQL 16.2 + pgvector 0.6.2** as a separate process in the app
container:

- The binaries come from the [`pgserver`](https://pypi.org/project/pgserver/) wheel in
  `requirements.txt`, so the platform's normal `pip install` provides them. No apt, no root,
  and no runtime download (unlike `mongod`, which needs `fastdl.mongodb.org`). Only its
  bundled `initdb` / `postgres` binaries are used, because pgserver's own launcher listens
  on a unix socket only and the RAG API needs TCP.
- Data lives in `/tmp/librechat-postgres`. TCP is bound to the loopback host only and needs
  a password (`scram-sha-256`); the password is taken from the URI or generated each boot.
  Trust auth applies only over the unix socket inside the `0700` data directory.
- PostgreSQL refuses to run as root, and the wheel ships `x86_64` Linux binaries only.
- A failure here is **non-fatal**: RAG / file search is disabled, and LibreChat still starts.

Databricks Apps has no sidecar containers, so "separate container" really means a separate
process on the same compute, exactly as with the embedded `mongod`.

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

### 3a. Single sign-on: why the proxy holds the LibreChat session

**Databricks Apps terminates `Authorization` itself and never forwards it to the app.**
Verified against the running app:

| Request to the app URL | Response | Meaning |
|---|---|---|
| Valid Databricks token in `Authorization` | `401 {"message":"No auth token"}` | Reached LibreChat, which saw **no** auth header — the platform stripped it |
| Garbage bearer | `401 {}` | Platform-generated — the platform validates and consumes the header |

LibreChat's `jwtStrategy` reads auth *only* from `Authorization: Bearer`
(`ExtractJwt.fromAuthHeaderAsBearerToken()`), so a browser can **never** authenticate to
LibreChat through Databricks Apps. Cookie-based `/api/auth/refresh` succeeds, every
`Authorization`-based route returns `401 {"message":"No auth token"}` (27 bytes), and the UI
falls back to its "create an account" screen.

So the ingress proxy holds the session instead. For every `/api/*` request (except
`/api/auth/*`) it attaches `Authorization: Bearer <librechat-jwt>` for the user named by the
proxy-verified `x-forwarded-email`, provisioning the account on first use. The browser never
holds a credential, and the Databricks identity *is* the LibreChat session — which is both
the SSO behaviour and a stronger trust model than a client-held token.

Four things this has to get right:

- **Single-flight.** A cold page load fans out ~15 parallel API calls. Each would otherwise
  drive its own register+login and trip LibreChat's limiters. Logins are de-duplicated per
  email behind an `asyncio.Lock`, so a 15-way fan-out performs exactly **one** login.
- **Rate limits.** `LOGIN_MAX` (default 7 / 5 min) and `REGISTER_MAX` (default 5 / 60 min)
  are **per IP**, and every login originates from the proxy at `127.0.0.1` — so all users
  share one bucket and the defaults lock out the 6th person to sign in. Both ceilings are
  raised. Only the proxy can reach these routes, and it rejects a registration whose email
  does not match the verified identity.
- **A stable password secret.** Each user's LibreChat password is derived via HMAC from
  `LIBRECHAT_SSO_PASSWORD_SECRET`, else the platform-injected `DATABRICKS_CLIENT_SECRET` —
  **never** the per-boot `JWT_SECRET`. Keying it to a per-boot value would be invisible with
  an ephemeral MongoDB (both reset together) but would permanently lock every existing user
  out after a restart once `MONGO_URI` is external.
- **Display names.** LibreChat requires `name` to be >= 3 characters. The IdP's
  `x-forwarded-user` is an opaque numeric id, so `x-forwarded-preferred-username` is
  preferred, falling back to a humanized email local part
  (`justin.monaldo` -> `Justin Monaldo`). `ALLOW_UNVERIFIED_EMAIL_LOGIN=true` is set because
  this container can never send a verification mail.

Session expiry is invisible: a 401 on an injected request mints a fresh token and retries
once, and concurrent 401s collapse into a single re-login.

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

> **Do not add IAM scopes to `user_api_scopes`.** `iam.current-user:read` and
> `iam.access-control:read` are assigned implicitly by the platform and are rejected if you
> request them:
> `The specified scope iam.current-user:read is not a valid scope (400 INVALID_PARAMETER_VALUE)`.
> The caller's identity already arrives on every request as `x-forwarded-email` /
> `x-forwarded-preferred-username` next to the OBO token. Only service-level scopes belong
> here — `ai-functions`, `ai-gateway`, `apps`, `files`, `genie`, `model-serving`, `postgres`,
> `sql`, `vector-search`, `sql:restricted-query`, plus the `catalog.*` / `workspace.workspace`
> SDK scopes.

One documented exception: embeddings. The RAG API calls Unity Gateway through a loopback
bridge on `127.0.0.1:3081` and carries no user identity, so **embeddings run as the app's
service principal and are not user-attributed**.

---

## Two things the proxy and SSRF guard must get right

### Compressed responses pass through untouched

LibreChat serves Brotli when the browser advertises it. The ingress proxy's aiohttp session
therefore runs with **`auto_decompress=False`** and forwards `Content-Encoding` intact.
Without that, aiohttp tries to decode the body, has no Brotli codec, and every page load
500s:

```
aiohttp.client_exceptions.ClientResponseError: 400,
  message='Can not decode content-encoding: brotli (br). Please install `Brotli`',
  url='http://127.0.0.1:3080/'
```

**Do not "fix" this by adding `brotli` to `requirements.txt`.** A reverse proxy has no reason
to decode at all; passing the compressed bytes straight through is correct, cheaper, and keeps
compression end-to-end for the browser. The only requests made with `Accept-Encoding: identity`
are the handful whose body the proxy itself parses (the `/api/auth/refresh` interception, the
SSO auto-login, and the loopback embeddings bridge).

### The workspace host needs an SSRF exemption

Inside the Databricks Apps VPC the workspace hostname resolves to a **private** IP. LibreChat's
SSRF guard (`resolveHostnameSSRF`) therefore rejects it and every MCP server fails to
initialize:

```
[MCPServersRegistry] Failed to inspect server "system.ai.dbsql":
  Domain "https://<workspace-host>" is not allowed
```

`start_app.py` renders three entries to cover this, using the live host:

| Config key | Value | Why |
|---|---|---|
| `mcpSettings.allowedDomains` | `https://<host>` | A non-empty allowlist short-circuits the MCP SSRF check entirely |
| `mcpSettings.allowedAddresses` | `<host>:443` | The documented exemption for a hostname that resolves to a private IP |
| `endpoints.allowedAddresses` | `<host>:443` | Same, for the Unity Gateway model endpoint |

Re-rendering is idempotent, so these never accumulate duplicates.

## Gotchas worth knowing

- **`sys.executable` is relative.** The Apps runtime launches the app as `.venv/bin/python`,
  so `sys.executable` only resolves against the startup working directory. Child processes
  spawned with `cwd=` elsewhere fail with
  `[Errno 2] No such file or directory: '.venv/bin/python'` — which is how the RAG API
  silently degraded to `rag_api: warning`. `PYTHON_EXECUTABLE` is resolved once at import,
  while the cwd is still the app root, and used for every child process.
- **Check `/api/databricks/health` first.** It reports PostgreSQL, MongoDB, RAG API and OBO
  state in one place, and surfaces partial failures (like the one above) that the UI hides.

## Known limitations

- **Streaming is capped at 120 seconds.** The Databricks Apps reverse proxy enforces a
  non-configurable 120-second per-request timeout and may buffer SSE. LibreChat streams chat
  over SSE, so a single generation longer than 120s returns a 504 **generated at the proxy,
  with nothing in the app logs**. Long agent/tool loops over the MCP servers are the most
  likely way to hit this.
- **One instance only** while MongoDB or PostgreSQL is embedded. Each replica would have its own database
  and its own per-boot JWT secret, so sessions and history would break at random. Pinned via
  `compute_min_instances: 1` / `compute_max_instances: 1`.
- **Session secrets are regenerated on every boot.** Correct while MongoDB is ephemeral
  (there is no persisted data to decrypt), but when you move to an external MongoDB, set
  `JWT_SECRET`, `JWT_REFRESH_SECRET`, `CREDS_KEY` and `CREDS_IV` from a Databricks secret
  scope so sessions and stored credentials survive restarts.
- **Meilisearch is disabled** (`SEARCH=false`); LibreChat's full-text conversation search is
  unavailable.
- **`SCHEDULES_SINGLE_PROCESS=true`** is set, asserting this deployment runs exactly one
  replica — which the pinned `compute_min_instances`/`compute_max_instances` of 1 guarantees.
  Without it LibreChat refuses all schedule writes with a 503. **Revisit this if the instance
  count is ever raised**, which also requires an external MongoDB.
- **OBO scopes can be wiped by a destructive app update.** Re-check `user_api_scopes` after
  any out-of-band change to the app.

## Health & diagnostics

```bash
curl -s https://<app-url>/api/databricks/health | jq
```

Reports `ready`/`phase`, PostgreSQL status, host and whether it is embedded, MongoDB status (`running (ephemeral)` vs
`external`), RAG API status, and the resolved Unity Gateway host, models, embedding model,
MCP services, and whether an OBO user token was seen on the request.
