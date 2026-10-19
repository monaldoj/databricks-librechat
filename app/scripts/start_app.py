#!/usr/bin/env python3
"""
Databricks App Orchestrator for Unmodified LibreChat
====================================================
Runs inside a Databricks App container and coordinates:
1. Immediate HTTP/SSE/WebSocket ingress on `DATABRICKS_APP_PORT` (default 8000)
   that captures `x-forwarded-access-token` and `x-forwarded-email` for
   On-Behalf-Of (OBO) authentication to Databricks Unity Gateway.
2. Lakebase Autoscaling PostgreSQL connection string generation (`PGHOST`, `PGPORT`,
   `PGDATABASE`, `PGUSER`, `PGSSLMODE=require`, OAuth credential via `LAKEBASE_ENDPOINT`)
   and `pgvector` + schema initialization.
3. Embedded ephemeral MongoDB (`mongod` on `127.0.0.1:27017/LibreChat`) running on
   the same Databricks App compute (`MONGO_URI=mongodb://127.0.0.1:27017/LibreChat`).
4. Dynamic rendering of `/tmp/librechat.yaml` with 3-level Unity Catalog namespace
   Model Services (`catalog.schema.model_service_name`) and MCP Services
   (`catalog.schema.mcp_service_name`) routed through Databricks Unity Gateway.
5. Unmodified `LibreChat-AI/LibreChat` (and `LibreChat-AI/rag-api` backed by Lakebase
   PostgreSQL `pgvector`), with only connection strings and `CONFIG_PATH` configured
   through environment variables.
"""

from __future__ import annotations

import asyncio
import collections
import hashlib
import hmac
import html
import json
import logging
import os
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

import aiohttp
from aiohttp import web
import psycopg2
import yaml
from databricks.sdk import WorkspaceClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("databricks-librechat")

APP_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = APP_ROOT / "scripts"

INGRESS_PORT = int(os.environ.get("DATABRICKS_APP_PORT") or os.environ.get("PORT") or "8000")
LIBRECHAT_INTERNAL_PORT = int(os.environ.get("LIBRECHAT_INTERNAL_PORT", "3080"))
OBO_BRIDGE_PORT = int(os.environ.get("OBO_BRIDGE_PORT", "3081"))
RAG_API_PORT = int(os.environ.get("RAG_PORT", "8001"))

LIBRECHAT_DIR = Path(os.environ.get("LIBRECHAT_Runtime_DIR", "/tmp/librechat"))
RAG_API_DIR = Path(os.environ.get("RAG_API_RUNTIME_DIR", "/tmp/librechat-rag-api"))
RENDERED_CONFIG_PATH = Path("/tmp/librechat.yaml")
OBO_CACHE_FILE = Path("/tmp/databricks_obo_tokens.json")
SP_TOKEN_FILE = Path("/tmp/databricks_sp_token")

# Shared cryptographic secrets generated once per app boot if not provided
JWT_SECRET = os.environ.get("JWT_SECRET") or secrets.token_hex(32)
JWT_REFRESH_SECRET = os.environ.get("JWT_REFRESH_SECRET") or secrets.token_hex(32)
CREDS_KEY = os.environ.get("CREDS_KEY") or secrets.token_hex(32)
CREDS_IV = os.environ.get("CREDS_IV") or secrets.token_hex(16)


class AppState:
    def __init__(self) -> None:
        self.phase: str = "Initializing Databricks App environment..."
        self.ready: bool = False
        self.error: Optional[str] = None
        self.logs: Deque[str] = collections.deque(maxlen=250)

        # Component statuses
        self.lakebase_status: str = "pending"
        self.lakebase_host: str = os.environ.get("PGHOST", "")
        self.lakebase_db: str = os.environ.get("PGDATABASE", "databricks_postgres")
        self.lakebase_endpoint: str = os.environ.get("LAKEBASE_ENDPOINT", "")
        self.lakebase_token: str = ""
        self.database_url: str = ""

        self.mongodb_status: str = "pending"
        self.mongo_uri: str = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017/LibreChat")

        self.rag_api_status: str = "pending"
        self.librechat_status: str = "pending"

        self.databricks_host: str = ""
        self.sp_token: str = ""

        # OBO token store: email -> access token
        self.obo_tokens_by_email: Dict[str, str] = {}
        self.latest_obo_token: Optional[str] = None
        self.latest_obo_email: Optional[str] = None
        self.obo_lock = threading.Lock()

        # Subprocesses
        self.mongo_proc: Optional[subprocess.Popen] = None
        self.rag_proc: Optional[subprocess.Popen] = None
        self.librechat_proc: Optional[subprocess.Popen] = None

    def add_log(self, msg: str) -> None:
        timestamp = time.strftime("%H:%M:%S")
        line = f"[{timestamp}] {msg}"
        self.logs.append(line)
        logger.info(msg)

    def record_obo_token(self, token: Optional[str], email: Optional[str]) -> None:
        if not token or not token.strip():
            return
        clean_tok = token.strip()
        clean_email = email.strip().lower() if email and email.strip() else None
        with self.obo_lock:
            self.latest_obo_token = clean_tok
            if clean_email:
                self.latest_obo_email = clean_email
                self.obo_tokens_by_email[clean_email] = clean_tok
            payload = {
                "latest_token": self.latest_obo_token,
                "latest_email": self.latest_obo_email,
                "by_email": dict(self.obo_tokens_by_email),
                "updated_at": time.time(),
            }
            try:
                tmp_path = OBO_CACHE_FILE.with_suffix(".tmp")
                tmp_path.write_text(json.dumps(payload), encoding="utf-8")
                tmp_path.replace(OBO_CACHE_FILE)
            except Exception as exc:
                logger.debug("Failed writing OBO token cache: %s", exc)

    def resolve_obo_or_sp_token(self, email: Optional[str] = None) -> tuple[str, str]:
        with self.obo_lock:
            if email:
                clean = email.strip().lower()
                if clean in self.obo_tokens_by_email:
                    return self.obo_tokens_by_email[clean], f"OBO({clean})"
            if self.latest_obo_token:
                return self.latest_obo_token, f"OBO({self.latest_obo_email or 'active-user'})"
        return self.sp_token, "SP-fallback"


STATE = AppState()


# =============================================================================
# 1. Databricks Workspace, Unity Gateway & Lakebase Credential Management
# =============================================================================
def normalize_host(raw: str) -> str:
    h = (raw or "").strip()
    h = h.removeprefix("https://").removeprefix("http://").rstrip("/")
    return h


def init_databricks_workspace_and_sp_token(w: WorkspaceClient) -> None:
    host = normalize_host(os.environ.get("DATABRICKS_HOST") or w.config.host or "")
    STATE.databricks_host = host
    os.environ["DATABRICKS_HOST"] = host

    headers = w.config.authenticate()
    auth_header = headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        sp_tok = auth_header.removeprefix("Bearer ").strip()
        STATE.sp_token = sp_tok
        os.environ["DATABRICKS_TOKEN"] = sp_tok
        try:
            SP_TOKEN_FILE.write_text(sp_tok, encoding="utf-8")
        except Exception:
            pass
    STATE.add_log(f"Authenticated Databricks WorkspaceClient for host: {host}")


def refresh_lakebase_connection_string(w: WorkspaceClient, initialize_schema: bool = False) -> None:
    """
    Mints a fresh Lakebase Autoscaling OAuth token and builds the standard
    PostgreSQL connection string for LibreChat and its RAG API.
    """
    pg_host = os.environ.get("PGHOST", "").strip()
    pg_port = os.environ.get("PGPORT", "5432").strip()
    pg_db = os.environ.get("PGDATABASE", "databricks_postgres").strip()
    pg_user = os.environ.get("PGUSER", "").strip()
    endpoint = os.environ.get("LAKEBASE_ENDPOINT", "").strip()

    STATE.lakebase_host = pg_host
    STATE.lakebase_db = pg_db
    STATE.lakebase_endpoint = endpoint

    if not pg_host:
        STATE.lakebase_status = "not_configured"
        STATE.add_log("PGHOST not found in environment; skipping Lakebase initialization.")
        return

    token = os.environ.get("PGPASSWORD", "").strip()
    if endpoint:
        STATE.add_log(f"Generating Lakebase OAuth credential for endpoint: {endpoint}")
        cred = w.postgres.generate_database_credential(endpoint=endpoint)
        token = cred.token
    elif not token:
        # Fallback to workspace OAuth token if LAKEBASE_ENDPOINT was not explicitly set
        token = STATE.sp_token

    if not pg_user:
        pg_user = os.environ.get("DATABRICKS_CLIENT_ID") or w.current_user.me().user_name

    STATE.lakebase_token = token
    quoted_user = urllib.parse.quote_plus(pg_user)
    quoted_pw = urllib.parse.quote_plus(token)
    quoted_db = urllib.parse.quote_plus(pg_db)

    # The ONLY thing that changes from LibreChat's perspective for PostgreSQL
    # is the connection string pointing to the Lakebase Autoscaling project:
    conn_str = f"postgresql://{quoted_user}:{quoted_pw}@{pg_host}:{pg_port}/{quoted_db}?sslmode=require"
    STATE.database_url = conn_str

    os.environ["DATABASE_URL"] = conn_str
    os.environ["POSTGRES_URI"] = conn_str
    os.environ["DB_HOST"] = pg_host
    os.environ["DB_PORT"] = pg_port
    os.environ["POSTGRES_DB"] = pg_db
    os.environ["POSTGRES_USER"] = pg_user
    os.environ["POSTGRES_PASSWORD"] = token
    os.environ["PGSSLMODE"] = "require"

    if initialize_schema:
        STATE.add_log(f"Connecting to Lakebase PostgreSQL at {pg_host}:{pg_port}/{pg_db} (sslmode=require)...")
        conn = psycopg2.connect(
            host=pg_host,
            port=int(pg_port),
            dbname=pg_db,
            user=pg_user,
            password=token,
            sslmode="require",
            connect_timeout=15,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            ver = cur.fetchone()[0]
            STATE.add_log(f"Connected to Lakebase PostgreSQL: {ver.split(',')[0]}")
            cur.execute("CREATE SCHEMA IF NOT EXISTS librechat;")
            STATE.add_log("Ensured PostgreSQL schema `librechat` exists in Lakebase.")
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                STATE.add_log("Ensured PostgreSQL `vector` (pgvector) extension is enabled in Lakebase.")
            except Exception as ext_err:
                STATE.add_log(f"Note on pgvector extension check: {ext_err}")
        conn.close()
        STATE.lakebase_status = "connected"


def credential_refresh_loop() -> None:
    """Refreshes Service Principal token and Lakebase 1-hour OAuth token every 40 minutes."""
    while True:
        time.sleep(40 * 60)
        try:
            w = WorkspaceClient()
            init_databricks_workspace_and_sp_token(w)
            refresh_lakebase_connection_string(w, initialize_schema=False)
            STATE.add_log("Refreshed Databricks Service Principal & Lakebase OAuth tokens.")
        except Exception as exc:
            logger.warning("Periodic token refresh warning: %s", exc)


# =============================================================================
# 2. Render `/tmp/librechat.yaml` with 3-Level Namespace Models & MCPs
# =============================================================================
def parse_csv_env(name: str, default: str) -> List[str]:
    raw = os.environ.get(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def render_librechat_yaml() -> Path:
    template_path = APP_ROOT / "librechat.yaml"
    config = yaml.safe_load(template_path.read_text(encoding="utf-8"))

    default_model = os.environ.get("UNITY_GATEWAY_DEFAULT_MODEL", "system.ai.claude-sonnet-4-5").strip()
    models = parse_csv_env(
        "UNITY_GATEWAY_MODELS",
        "system.ai.claude-sonnet-4-5,system.ai.claude-opus-4-5,system.ai.gpt-5-6-sol",
    )
    if default_model and default_model not in models:
        models.insert(0, default_model)

    mcp_services = parse_csv_env(
        "UNITY_GATEWAY_MCP_SERVICES",
        "system.ai.dbsql,system.ai.genie_one_mcp,system.ai.web_search",
    )

    # 1. Configure 3-level namespace Model Services via Unity Gateway (/ai-gateway/mlflow/v1)
    custom_endpoints = config.setdefault("endpoints", {}).setdefault("custom", [])
    if custom_endpoints:
        ugw_endpoint = custom_endpoints[0]
        ugw_endpoint["baseURL"] = f"https://{STATE.databricks_host}/ai-gateway/mlflow/v1"
        ugw_endpoint["models"] = {
            "default": models,
            "fetch": False,
        }
        ugw_endpoint["titleModel"] = default_model

    # 2. Configure 3-level namespace MCP Services via Unity Gateway (/ai-gateway/mcp-services/<catalog>.<schema>.<service>)
    mcp_servers_cfg = config.get("mcpServers") or {}
    rendered_mcps: Dict[str, Any] = {}
    for mcp_fqn in mcp_services:
        existing = mcp_servers_cfg.get(mcp_fqn, {})
        rendered_mcps[mcp_fqn] = {
            "type": existing.get("type", "streamable-http"),
            "title": existing.get("title", f"Unity Gateway MCP ({mcp_fqn})"),
            "description": existing.get(
                "description",
                f"Databricks Unity Gateway governed MCP service {mcp_fqn} (OBO authenticated)",
            ),
            "url": f"https://{STATE.databricks_host}/ai-gateway/mcp-services/{mcp_fqn}",
            "timeout": existing.get("timeout", 120000),
            "initTimeout": existing.get("initTimeout", 30000),
            "chatMenu": True,
            "startup": False,
            "headers": {
                "Authorization": "Bearer ${DATABRICKS_TOKEN}",
                "x-librechat-user-email": "{{LIBRECHAT_USER_EMAIL}}",
                "x-librechat-user-id": "{{LIBRECHAT_USER_ID}}",
            },
        }
    config["mcpServers"] = rendered_mcps

    RENDERED_CONFIG_PATH.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    STATE.add_log(
        f"Rendered {RENDERED_CONFIG_PATH} with Unity Gateway 3-level models={models} and MCPs={mcp_services}"
    )
    return RENDERED_CONFIG_PATH


# =============================================================================
# 3. Bootstrap Unmodified LibreChat, Embedded MongoDB, and RAG API
# =============================================================================
def run_cmd_logged(cmd: List[str], cwd: Path, env: Optional[Dict[str, str]] = None, prefix: str = "cmd") -> None:
    STATE.add_log(f"[{prefix}] Running: {' '.join(cmd)} (in {cwd})")
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        env=env or os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        stripped = line.rstrip()
        if stripped:
            STATE.add_log(f"[{prefix}] {stripped}")
    rc = proc.wait()
    if rc != 0:
        raise RuntimeError(f"Command {' '.join(cmd)} exited with code {rc}")


def stream_process_logs(proc: subprocess.Popen, prefix: str) -> None:
    def _reader() -> None:
        if proc.stdout is None:
            return
        for line in proc.stdout:
            stripped = line.rstrip()
            if stripped:
                STATE.add_log(f"[{prefix}] {stripped}")

    t = threading.Thread(target=_reader, daemon=True)
    t.start()


def wait_for_tcp_port(host: str, port: int, timeout_sec: float = 120.0) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.5):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def ensure_unmodified_librechat_repo() -> None:
    version = os.environ.get("LIBRECHAT_VERSION", "v0.8.8").strip()
    if not (LIBRECHAT_DIR / "package.json").exists():
        STATE.phase = f"Cloning unmodified LibreChat ({version}) from GitHub..."
        if LIBRECHAT_DIR.exists():
            shutil.rmtree(LIBRECHAT_DIR)
        run_cmd_logged(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                version,
                "https://github.com/LibreChat-AI/LibreChat.git",
                str(LIBRECHAT_DIR),
            ],
            cwd= Path("/tmp"),
            prefix="git-librechat",
        )

    if not (LIBRECHAT_DIR / "node_modules").exists():
        STATE.phase = "Installing LibreChat npm dependencies (npm ci)..."
        env = os.environ.copy()
        env["NODE_OPTIONS"] = "--max-old-space-size=4096"
        run_cmd_logged(
            ["npm", "ci", "--no-audit"],
            cwd=LIBRECHAT_DIR,
            env=env,
            prefix="npm-ci",
        )

    if not (LIBRECHAT_DIR / "client" / "dist" / "index.html").exists():
        STATE.phase = "Building unmodified LibreChat frontend & workspace packages..."
        env = os.environ.copy()
        env["NODE_OPTIONS"] = "--max-old-space-size=4096"
        run_cmd_logged(
            ["npm", "run", "frontend"],
            cwd=LIBRECHAT_DIR,
            env=env,
            prefix="npm-frontend",
        )


def start_embedded_mongodb() -> None:
    STATE.phase = "Starting embedded ephemeral MongoDB on 127.0.0.1:27017..."
    STATE.mongodb_status = "starting"

    if wait_for_tcp_port("127.0.0.1", 27017, timeout_sec=1.0):
        STATE.mongodb_status = "running"
        STATE.add_log("Embedded MongoDB is already listening on 127.0.0.1:27017")
        return

    env = os.environ.copy()
    proc = subprocess.Popen(
        ["node", str(SCRIPTS_DIR / "start_mongodb.cjs")],
        cwd=str(LIBRECHAT_DIR if LIBRECHAT_DIR.exists() else APP_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    STATE.mongo_proc = proc
    stream_process_logs(proc, "mongodb")

    if not wait_for_tcp_port("127.0.0.1", 27017, timeout_sec=120.0):
        raise RuntimeError("Embedded MongoDB failed to bind 127.0.0.1:27017 within 120s")

    STATE.mongodb_status = "running"
    STATE.add_log(f"Embedded MongoDB ready at MONGO_URI={STATE.mongo_uri}")


def start_rag_api_with_lakebase() -> None:
    """
    Starts LibreChat's official companion `rag_api` connected to the Lakebase
    Autoscaling PostgreSQL instance (`DB_HOST`, `POSTGRES_DB`, `POSTGRES_USER`,
    `POSTGRES_PASSWORD`, `PGSSLMODE=require`) and Unity Gateway embeddings.
    """
    if STATE.lakebase_status != "connected":
        STATE.rag_api_status = "skipped (Lakebase not configured)"
        return

    try:
        STATE.phase = "Starting LibreChat RAG API connected to Lakebase Autoscaling PostgreSQL..."
        STATE.rag_api_status = "starting"

        if not (RAG_API_DIR / "main.py").exists():
            if RAG_API_DIR.exists():
                shutil.rmtree(RAG_API_DIR)
            run_cmd_logged(
                [
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    "https://github.com/LibreChat-AI/rag-api.git",
                    str(RAG_API_DIR),
                ],
                cwd=Path("/tmp"),
                prefix="git-rag-api",
            )
            run_cmd_logged(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--quiet",
                    "-r",
                    str(RAG_API_DIR / "requirements.lite.txt"),
                ],
                cwd=RAG_API_DIR,
                prefix="pip-rag-api",
            )

        rag_env = os.environ.copy()
        rag_env.update(
            {
                "DB_HOST": os.environ["DB_HOST"],
                "DB_PORT": os.environ.get("DB_PORT", "5432"),
                "POSTGRES_DB": os.environ["POSTGRES_DB"],
                "POSTGRES_USER": os.environ["POSTGRES_USER"],
                "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
                "POSTGRES_SCHEMA": os.environ.get("POSTGRES_SCHEMA", "librechat,public"),
                "PGSSLMODE": "require",
                "PGVECTOR_CREATE_EXTENSION": "false",
                "RAG_PORT": str(RAG_API_PORT),
                "JWT_SECRET": JWT_SECRET,
                "EMBEDDINGS_PROVIDER": "openai",
                "EMBEDDINGS_MODEL": os.environ.get(
                    "UNITY_GATEWAY_EMBEDDING_MODEL", "system.ai.databricks-gte-large-en"
                ),
                "OPENAI_BASEURL": f"http://127.0.0.1:{OBO_BRIDGE_PORT}/ai-gateway/mlflow/v1",
                "RAG_OPENAI_API_KEY": STATE.sp_token or "databricks-obo",
            }
        )

        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(RAG_API_PORT),
            ],
            cwd=str(RAG_API_DIR),
            env=rag_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        STATE.rag_proc = proc
        stream_process_logs(proc, "rag-api")

        if wait_for_tcp_port("127.0.0.1", RAG_API_PORT, timeout_sec=45.0):
            STATE.rag_api_status = "running"
            os.environ["RAG_API_URL"] = f"http://127.0.0.1:{RAG_API_PORT}"
            os.environ["RAG_PORT"] = str(RAG_API_PORT)
            STATE.add_log(
                f"LibreChat RAG API listening on http://127.0.0.1:{RAG_API_PORT} (backed by Lakebase PostgreSQL)"
            )
        else:
            STATE.rag_api_status = "warning (startup timeout)"
            STATE.add_log("RAG API did not bind within 45s; continuing with core LibreChat startup.")
    except Exception as exc:
        STATE.rag_api_status = f"warning ({exc})"
        STATE.add_log(f"RAG API setup note (non-fatal): {exc}")


def start_librechat_backend(config_path: Path) -> None:
    STATE.phase = "Starting unmodified LibreChat backend server..."
    STATE.librechat_status = "starting"

    preload_script = SCRIPTS_DIR / "databricks_obo_preload.cjs"
    existing_node_opts = os.environ.get("NODE_OPTIONS", "")
    node_options = f"--max-old-space-size=4096 --require {preload_script} {existing_node_opts}".strip()

    env = os.environ.copy()
    env.update(
        {
            "NODE_ENV": "production",
            "HOST": "127.0.0.1",
            "PORT": str(LIBRECHAT_INTERNAL_PORT),
            # 1. Embedded ephemeral MongoDB connection string
            "MONGO_URI": STATE.mongo_uri,
            # 2. Lakebase Autoscaling PostgreSQL connection string
            "DATABASE_URL": STATE.database_url,
            "POSTGRES_URI": STATE.database_url,
            # 3. Unity Gateway 3-level namespace configuration
            "CONFIG_PATH": str(config_path),
            "DATABRICKS_HOST": STATE.databricks_host,
            "DATABRICKS_TOKEN": STATE.sp_token or "databricks-obo",
            "DATABRICKS_OBO_CACHE_FILE": str(OBO_CACHE_FILE),
            "DATABRICKS_SP_TOKEN_FILE": str(SP_TOKEN_FILE),
            "NODE_OPTIONS": node_options,
            # Standard LibreChat runtime settings
            "APP_TITLE": "LibreChat on Databricks",
            "ALLOW_REGISTRATION": "true",
            "ALLOW_EMAIL_LOGIN": "true",
            "ALLOW_SOCIAL_LOGIN": "false",
            "JWT_SECRET": JWT_SECRET,
            "JWT_REFRESH_SECRET": JWT_REFRESH_SECRET,
            "CREDS_KEY": CREDS_KEY,
            "CREDS_IV": CREDS_IV,
            "SEARCH": "false",
        }
    )

    proc = subprocess.Popen(
        ["node", "api/server/index.js"],
        cwd=str(LIBRECHAT_DIR),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    STATE.librechat_proc = proc
    stream_process_logs(proc, "librechat")

    if not wait_for_tcp_port("127.0.0.1", LIBRECHAT_INTERNAL_PORT, timeout_sec=120.0):
        raise RuntimeError(
            f"LibreChat backend failed to bind 127.0.0.1:{LIBRECHAT_INTERNAL_PORT} within 120s"
        )

    STATE.librechat_status = "running"
    STATE.ready = True
    STATE.phase = "LibreChat is running and ready."
    STATE.add_log(
        f"LibreChat is READY on internal port {LIBRECHAT_INTERNAL_PORT} (proxied on Databricks App port {INGRESS_PORT})"
    )


def bootstrap_worker() -> None:
    try:
        STATE.phase = "Authenticating with Databricks Workspace & Lakebase Autoscaling..."
        w = WorkspaceClient()
        init_databricks_workspace_and_sp_token(w)
        refresh_lakebase_connection_string(w, initialize_schema=True)

        # Start background token refresh thread for Lakebase 1-hour OAuth credentials
        threading.Thread(target=credential_refresh_loop, daemon=True).start()

        config_path = render_librechat_yaml()
        ensure_unmodified_librechat_repo()
        start_embedded_mongodb()
        start_rag_api_with_lakebase()
        start_librechat_backend(config_path)
    except Exception as exc:
        logger.exception("Bootstrap error")
        STATE.error = str(exc)
        STATE.phase = f"Startup failed: {exc}"
        STATE.add_log(f"ERROR: {exc}")


# =============================================================================
# 4. Seamless Databricks SSO User Provisioning for Ephemeral MongoDB
# =============================================================================
def derive_sso_password(email: str) -> str:
    digest = hmac.new(
        JWT_SECRET.encode("utf-8"),
        f"databricks-sso:{email.lower()}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"Dbx!{digest[:28]}9aA"


async def auto_login_databricks_sso_user(
    session: aiohttp.ClientSession,
    email: str,
    username: Optional[str],
    obo_headers: Dict[str, str],
) -> Optional[aiohttp.ClientResponse]:
    """
    Because the embedded MongoDB is ephemeral when the Databricks App spins down,
    this helper automatically provisions and signs in the Databricks SSO user
    (`x-forwarded-email`) via LibreChat's standard `/api/auth/register` and
    `/api/auth/login` endpoints whenever `/api/auth/refresh` has no active session.
    This guarantees `{{LIBRECHAT_USER_EMAIL}}` matches `x-forwarded-email` with
    zero changes to LibreChat's core code.
    """
    clean_email = email.strip().lower()
    display_name = (username or clean_email.split("@")[0]).strip()
    safe_username = "".join(c if c.isalnum() or c in "_-" else "_" for c in clean_email.split("@")[0])
    if len(safe_username) < 3:
        safe_username = f"dbx_{safe_username}"
    password = derive_sso_password(clean_email)
    base = f"http://127.0.0.1:{LIBRECHAT_INTERNAL_PORT}"

    headers = {
        "Content-Type": "application/json",
        "Origin": base,
        "Referer": f"{base}/login",
        **obo_headers,
    }

    try:
        reg_payload = {
            "name": display_name,
            "username": safe_username,
            "email": clean_email,
            "password": password,
            "confirm_password": password,
        }
        async with session.post(f"{base}/api/auth/register", json=reg_payload, headers=headers) as reg_resp:
            await reg_resp.read()

        login_payload = {
            "email": clean_email,
            "password": password,
        }
        login_resp = await session.post(f"{base}/api/auth/login", json=login_payload, headers=headers)
        if login_resp.status == 200:
            STATE.add_log(f"Auto-authenticated Databricks SSO user into LibreChat: {clean_email}")
            return login_resp
        await login_resp.read()
        login_resp.close()
    except Exception as exc:
        logger.warning("Databricks SSO auto-login note for %s: %s", clean_email, exc)
    return None


# =============================================================================
# 5. Ingress Reverse Proxy (`DATABRICKS_APP_PORT`) & Local OBO Bridge (`3081`)
# =============================================================================
def build_loading_html() -> str:
    logs_text = html.escape("\n".join(STATE.logs))
    err_block = (
        f'<div style="background:#451a1a;border:1px solid #ef4444;padding:12px;border-radius:8px;margin-bottom:16px;color:#fca5a5;">'
        f"<strong>Error:</strong> {html.escape(STATE.error)}</div>"
        if STATE.error
        else ""
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>LibreChat on Databricks Apps — Starting</title>
  <meta http-equiv="refresh" content="3" />
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #0f172a;
      color: #f8fafc;
      margin: 0;
      padding: 32px;
      display: flex;
      justify-content: center;
    }}
    .card {{
      max-width: 880px;
      width: 100%;
      background: #1e293b;
      border: 1px solid #334155;
      border-radius: 12px;
      padding: 24px 28px;
      box-shadow: 0 10px 25px rgba(0,0,0,0.4);
    }}
    .badges {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 12px;
      margin: 16px 0;
    }}
    .badge {{
      background: #0f172a;
      border: 1px solid #334155;
      border-radius: 8px;
      padding: 10px 14px;
      font-size: 13px;
    }}
    .badge strong {{ display: block; color: #94a3b8; margin-bottom: 4px; }}
    pre {{
      background: #020617;
      border: 1px solid #1e293b;
      border-radius: 8px;
      padding: 14px;
      font-size: 12px;
      line-height: 1.45;
      max-height: 380px;
      overflow-y: auto;
      color: #cbd5e1;
      white-space: pre-wrap;
    }}
  </style>
</head>
<body>
  <div class="card">
    <h2 style="margin-top:0;">LibreChat on Databricks Apps</h2>
    <p style="color:#94a3b8;margin-top:4px;">Phase: <strong>{html.escape(STATE.phase)}</strong></p>
    {err_block}
    <div class="badges">
      <div class="badge">
        <strong>Lakebase PostgreSQL</strong>
        {html.escape(STATE.lakebase_status)} ({html.escape(STATE.lakebase_host or 'binding')})
      </div>
      <div class="badge">
        <strong>Embedded MongoDB</strong>
        {html.escape(STATE.mongodb_status)} (127.0.0.1:27017)
      </div>
      <div class="badge">
        <strong>Unity Gateway Host</strong>
        {html.escape(STATE.databricks_host or 'initializing')}
      </div>
      <div class="badge">
        <strong>LibreChat Server</strong>
        {html.escape(STATE.librechat_status)}
      </div>
    </div>
    <pre id="logs">{logs_text}</pre>
  </div>
  <script>
    const pre = document.getElementById('logs');
    if (pre) pre.scrollTop = pre.scrollHeight;
  </script>
</body>
</html>"""


async def handle_databricks_health(request: web.Request) -> web.Response:
    obo_tok = request.headers.get("x-forwarded-access-token")
    obo_email = request.headers.get("x-forwarded-email") or request.headers.get("x-forwarded-user")
    STATE.record_obo_token(obo_tok, obo_email)
    return web.json_response(
        {
            "ready": STATE.ready,
            "phase": STATE.phase,
            "error": STATE.error,
            "lakebase": {
                "status": STATE.lakebase_status,
                "host": STATE.lakebase_host,
                "database": STATE.lakebase_db,
                "endpoint": STATE.lakebase_endpoint,
            },
            "mongodb": {
                "status": STATE.mongodb_status,
                "uri": STATE.mongo_uri,
            },
            "rag_api": {
                "status": STATE.rag_api_status,
            },
            "unity_gateway": {
                "host": STATE.databricks_host,
                "obo_user_detected": bool(obo_tok or STATE.latest_obo_token),
                "obo_user_email": obo_email or STATE.latest_obo_email,
                "models": parse_csv_env("UNITY_GATEWAY_MODELS", ""),
                "mcp_services": parse_csv_env("UNITY_GATEWAY_MCP_SERVICES", ""),
            },
        }
    )


async def handle_ingress_proxy(request: web.Request) -> web.StreamResponse:
    # 1. Capture Databricks Apps OBO headers on every incoming request
    obo_tok = request.headers.get("x-forwarded-access-token")
    obo_email = (
        request.headers.get("x-forwarded-email")
        or request.headers.get("x-forwarded-preferred-username")
        or request.headers.get("x-forwarded-user")
    )
    obo_user = request.headers.get("x-forwarded-user")
    STATE.record_obo_token(obo_tok, obo_email)

    if request.path == "/api/databricks/health":
        return await handle_databricks_health(request)

    if not STATE.ready:
        return web.Response(text=build_loading_html(), content_type="text/html", status=200)

    # 2. WebSocket upgrade passthrough
    if (
        request.headers.get("Upgrade", "").lower() == "websocket"
        and "upgrade" in request.headers.get("Connection", "").lower()
    ):
        ws_server = web.WebSocketResponse()
        await ws_server.prepare(request)
        target_ws_url = f"ws://127.0.0.1:{LIBRECHAT_INTERNAL_PORT}{request.rel_url}"
        session: aiohttp.ClientSession = request.app["http_session"]
        async with session.ws_connect(target_ws_url, headers=dict(request.headers)) as ws_client:
            async def ws_forward(src, dst):
                async for msg in src:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await dst.send_str(msg.data)
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        await dst.send_bytes(msg.data)
                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        await dst.close()

            await asyncio.gather(
                ws_forward(ws_server, ws_client),
                ws_forward(ws_client, ws_server),
                return_exceptions=True,
            )
        return ws_server

    # 3. Forward HTTP / SSE request to unmodified LibreChat on 127.0.0.1:3080
    target_url = f"http://127.0.0.1:{LIBRECHAT_INTERNAL_PORT}{request.rel_url}"
    req_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length", "transfer-encoding")
    }
    req_headers["Host"] = f"127.0.0.1:{LIBRECHAT_INTERNAL_PORT}"

    body = await request.read()
    session = request.app["http_session"]

    # Seamless Databricks SSO auto-provisioning on /api/auth/refresh when MongoDB is ephemeral
    if request.method == "POST" and request.path == "/api/auth/refresh" and obo_email:
        async with session.request(
            request.method,
            target_url,
            headers=req_headers,
            data=body,
            allow_redirects=False,
        ) as upstream_resp:
            resp_bytes = await upstream_resp.read()
            has_valid_token = False
            if upstream_resp.status == 200:
                try:
                    parsed = json.loads(resp_bytes.decode("utf-8"))
                    has_valid_token = bool(isinstance(parsed, dict) and parsed.get("token"))
                except Exception:
                    has_valid_token = False

            if has_valid_token:
                out = web.Response(status=upstream_resp.status, body=resp_bytes)
                for k, v in upstream_resp.headers.items():
                    if k.lower() not in ("content-length", "transfer-encoding", "content-encoding"):
                        out.headers.add(k, v)
                return out

        # No valid local session yet in ephemeral MongoDB -> auto-register & login SSO user
        obo_hdrs = {}
        if obo_tok:
            obo_hdrs["x-forwarded-access-token"] = obo_tok
        if obo_email:
            obo_hdrs["x-forwarded-email"] = obo_email
        login_resp = await auto_login_databricks_sso_user(session, obo_email, obo_user, obo_hdrs)
        if login_resp is not None:
            try:
                login_bytes = await login_resp.read()
                out = web.Response(status=login_resp.status, body=login_bytes)
                for k, v in login_resp.headers.items():
                    if k.lower() not in ("content-length", "transfer-encoding", "content-encoding"):
                        out.headers.add(k, v)
                return out
            finally:
                login_resp.close()

    async with session.request(
        request.method,
        target_url,
        headers=req_headers,
        data=body,
        allow_redirects=False,
    ) as upstream_resp:
        content_type = upstream_resp.headers.get("Content-Type", "")
        is_sse = "text/event-stream" in content_type.lower()

        if is_sse:
            stream_resp = web.StreamResponse(status=upstream_resp.status)
            for k, v in upstream_resp.headers.items():
                if k.lower() not in ("content-length", "transfer-encoding", "content-encoding"):
                    stream_resp.headers.add(k, v)
            await stream_resp.prepare(request)
            async for chunk in upstream_resp.content.iter_any():
                await stream_resp.write(chunk)
            await stream_resp.write_eof()
            return stream_resp

        resp_bytes = await upstream_resp.read()
        out = web.Response(status=upstream_resp.status, body=resp_bytes)
        for k, v in upstream_resp.headers.items():
            if k.lower() not in ("content-length", "transfer-encoding", "content-encoding"):
                out.headers.add(k, v)
        return out


async def handle_obo_bridge(request: web.Request) -> web.StreamResponse:
    """
    Local Unity Gateway OBO proxy on 127.0.0.1:3081.
    Used by companion processes (such as Python `rag_api` calling Unity Gateway
    3-level embedding model services) to automatically inject the caller's
    `x-forwarded-access-token` (or Service Principal fallback token).
    """
    explicit_email = request.headers.get("x-librechat-user-email")
    token, source = STATE.resolve_obo_or_sp_token(explicit_email)
    target_url = f"https://{STATE.databricks_host}{request.rel_url}"

    fwd_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower()
        not in (
            "host",
            "authorization",
            "x-librechat-user-email",
            "x-librechat-user-id",
            "content-length",
            "transfer-encoding",
        )
    }
    fwd_headers["Authorization"] = f"Bearer {token}"
    logger.info("[UnityGateway-OBO:Bridge] %s %s -> auth=%s", request.method, target_url, source)

    body = await request.read()
    session: aiohttp.ClientSession = request.app["http_session"]
    async with session.request(
        request.method,
        target_url,
        headers=fwd_headers,
        data=body,
        allow_redirects=False,
    ) as resp:
        resp_bytes = await resp.read()
        out = web.Response(status=resp.status, body=resp_bytes)
        for k, v in resp.headers.items():
            if k.lower() not in ("content-length", "transfer-encoding", "content-encoding"):
                out.headers.add(k, v)
        return out


async def start_servers() -> None:
    timeout = aiohttp.ClientTimeout(total=900, connect=30, sock_read=600)
    http_session = aiohttp.ClientSession(timeout=timeout)

    # 1. Start Ingress Server on 0.0.0.0:${DATABRICKS_APP_PORT}
    ingress_app = web.Application(client_max_size=100 * 1024 * 1024)
    ingress_app["http_session"] = http_session
    ingress_app.router.add_route("*", "/{tail:.*}", handle_ingress_proxy)

    ingress_runner = web.AppRunner(ingress_app, access_log=None)
    await ingress_runner.setup()
    ingress_site = web.TCPSite(ingress_runner, "0.0.0.0", INGRESS_PORT)
    await ingress_site.start()
    STATE.add_log(f"Ingress OBO proxy listening on 0.0.0.0:{INGRESS_PORT}")

    # 2. Start Local Unity Gateway OBO Bridge on 127.0.0.1:3081
    bridge_app = web.Application(client_max_size=50 * 1024 * 1024)
    bridge_app["http_session"] = http_session
    bridge_app.router.add_route("*", "/{tail:.*}", handle_obo_bridge)

    bridge_runner = web.AppRunner(bridge_app, access_log=None)
    await bridge_runner.setup()
    bridge_site = web.TCPSite(bridge_runner, "127.0.0.1", OBO_BRIDGE_PORT)
    await bridge_site.start()
    STATE.add_log(f"Local Unity Gateway OBO bridge listening on 127.0.0.1:{OBO_BRIDGE_PORT}")

    # 3. Launch background bootstrap thread (Lakebase + MongoDB + LibreChat)
    threading.Thread(target=bootstrap_worker, daemon=True).start()

    # Keep running
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(start_servers())
    except KeyboardInterrupt:
        logger.info("Shutting down Databricks LibreChat App...")
