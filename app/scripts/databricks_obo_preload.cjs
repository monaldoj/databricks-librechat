/**
 * Databricks Unity Gateway On-Behalf-Of (OBO) Preload Hook for LibreChat
 *
 * Loaded via NODE_OPTIONS="--require /path/to/databricks_obo_preload.cjs"
 * so that ZERO files in the upstream LibreChat repository are modified.
 *
 * Responsibilities:
 * 1. Captures incoming Databricks Apps reverse-proxy headers (`x-forwarded-access-token`,
 *    `x-forwarded-email`, `x-forwarded-user`) per request using Node's AsyncLocalStorage
 *    and caches tokens per user email.
 * 2. Intercepts outbound requests from LibreChat to Databricks Unity Gateway:
 *    - Model Services: `https://<workspace>/ai-gateway/mlflow/v1/*` (3-level namespace models)
 *    - MCP Services:   `https://<workspace>/ai-gateway/mcp-services/<catalog>.<schema>.<mcp_service>`
 *    across `globalThis.fetch`, `undici` (Agent/Client/ProxyAgent/fetch), and `http`/`https`,
 *    replacing the `Authorization` header with `Bearer <x-forwarded-access-token>` for the
 *    authenticated Databricks user (with fallback to the App Service Principal token during
 *    non-user background/startup checks).
 */

'use strict';

const fs = require('node:fs');
const http = require('node:http');
const https = require('node:https');
const Module = require('node:module');
const { AsyncLocalStorage } = require('node:async_hooks');

const oboStorage = new AsyncLocalStorage();
const tokensByEmail = new Map();
let latestUserContext = {
  token: null,
  email: null,
  user: null,
  updatedAt: 0,
};

const OBO_CACHE_FILE = process.env.DATABRICKS_OBO_CACHE_FILE || '/tmp/databricks_obo_tokens.json';
const SP_TOKEN_FILE = process.env.DATABRICKS_SP_TOKEN_FILE || '/tmp/databricks_sp_token';

function normalizeHost(raw) {
  if (!raw) return '';
  return String(raw)
    .trim()
    .replace(/^https?:\/\//i, '')
    .replace(/\/+$/, '')
    .toLowerCase();
}

function getDatabricksHost() {
  return normalizeHost(process.env.DATABRICKS_HOST);
}

function readSharedOboToken(email) {
  try {
    if (!fs.existsSync(OBO_CACHE_FILE)) return null;
    const raw = fs.readFileSync(OBO_CACHE_FILE, 'utf8');
    const data = JSON.parse(raw);
    if (email && data.by_email && data.by_email[email.toLowerCase()]) {
      return {
        token: data.by_email[email.toLowerCase()],
        source: `OBO(shared:${email.toLowerCase()})`,
      };
    }
    if (data.latest_token) {
      return {
        token: data.latest_token,
        source: `OBO(shared-latest:${data.latest_email || 'user'})`,
      };
    }
  } catch (_) {
    // ignore transient file read errors
  }
  return null;
}

function readServicePrincipalFallbackToken() {
  try {
    if (fs.existsSync(SP_TOKEN_FILE)) {
      const tok = fs.readFileSync(SP_TOKEN_FILE, 'utf8').trim();
      if (tok) return tok;
    }
  } catch (_) {
    // ignore
  }
  return process.env.DATABRICKS_TOKEN || '';
}

function resolveUnityGatewayBearer(explicitEmail) {
  const store = oboStorage.getStore();
  if (store && store.token) {
    return {
      token: store.token,
      source: `OBO(async-context:${store.email || 'active-request'})`,
    };
  }

  const emailKey = (explicitEmail || (store && store.email) || '').trim().toLowerCase();
  if (emailKey && !emailKey.includes('{{')) {
    const memToken = tokensByEmail.get(emailKey);
    if (memToken) {
      return {
        token: memToken,
        source: `OBO(user-map:${emailKey})`,
      };
    }
    const shared = readSharedOboToken(emailKey);
    if (shared && shared.token) {
      return shared;
    }
  }

  const sharedLatest = readSharedOboToken(null);
  if (sharedLatest && sharedLatest.token) {
    return sharedLatest;
  }

  if (latestUserContext.token) {
    return {
      token: latestUserContext.token,
      source: `OBO(latest-session:${latestUserContext.email || 'user'})`,
    };
  }

  const spToken = readServicePrincipalFallbackToken();
  if (spToken) {
    return {
      token: spToken,
      source: 'SP-fallback(app-service-principal)',
    };
  }

  return null;
}

function isUnityGatewayUrl(urlStr) {
  if (!urlStr) return false;
  try {
    const parsed = new URL(urlStr);
    const dbHost = getDatabricksHost();
    const hostMatch = dbHost && parsed.hostname.toLowerCase() === dbHost;
    const pathMatch =
      parsed.pathname.startsWith('/ai-gateway/') ||
      parsed.pathname.startsWith('/serving-endpoints/') ||
      parsed.pathname.startsWith('/api/2.0/mcp/');
    const localBridgeMatch =
      (parsed.hostname === '127.0.0.1' || parsed.hostname === 'localhost') &&
      parsed.pathname.startsWith('/ai-gateway/');
    return Boolean((hostMatch && pathMatch) || localBridgeMatch);
  } catch (_) {
    return false;
  }
}

// -----------------------------------------------------------------------------
// 1. Hook incoming HTTP requests to LibreChat (`http.Server.prototype.emit`)
// -----------------------------------------------------------------------------
const originalHttpEmit = http.Server.prototype.emit;
http.Server.prototype.emit = function patchedHttpEmit(event, req, res) {
  if (event === 'request' && req && req.headers) {
    const oboToken = req.headers['x-forwarded-access-token'];
    const rawEmail =
      req.headers['x-forwarded-email'] ||
      req.headers['x-forwarded-preferred-username'] ||
      req.headers['x-forwarded-user'];
    const email = typeof rawEmail === 'string' ? rawEmail.trim().toLowerCase() : null;

    if (typeof oboToken === 'string' && oboToken.trim().length > 0) {
      const cleanToken = oboToken.trim();
      latestUserContext = {
        token: cleanToken,
        email: email || latestUserContext.email,
        user: req.headers['x-forwarded-user'] || null,
        updatedAt: Date.now(),
      };
      if (email) {
        tokensByEmail.set(email, cleanToken);
      }
    }

    return oboStorage.run(
      {
        token: typeof oboToken === 'string' ? oboToken.trim() : null,
        email,
      },
      () => originalHttpEmit.apply(this, arguments),
    );
  }
  return originalHttpEmit.apply(this, arguments);
};

// -----------------------------------------------------------------------------
// 2. Helper to rewrite Headers for Fetch / Undici
// -----------------------------------------------------------------------------
function collectHeaderEntries(hdrs, target) {
  if (!hdrs) {
    return;
  }
  if (typeof hdrs.entries === 'function') {
    for (const [k, v] of hdrs.entries()) {
      target.push([k, v]);
    }
  } else if (Array.isArray(hdrs)) {
    for (const pair of hdrs) {
      if (Array.isArray(pair) && pair.length >= 2) {
        target.push([pair[0], pair[1]]);
      }
    }
  } else if (typeof hdrs === 'object') {
    for (const [k, v] of Object.entries(hdrs)) {
      if (v !== undefined) {
        target.push([k, String(v)]);
      }
    }
  }
}

function extractAndRewriteFetchHeaders(initHeaders, fallbackRequestHeaders) {
  const headerEntries = [];
  let explicitEmail = null;

  collectHeaderEntries(fallbackRequestHeaders, headerEntries);
  collectHeaderEntries(initHeaders, headerEntries);

  const filtered = {};
  for (const [k, v] of headerEntries) {
    const lower = String(k).toLowerCase();
    if (lower === 'x-librechat-user-email') {
      explicitEmail = String(v);
      continue;
    }
    if (lower === 'x-librechat-user-id') {
      continue;
    }
    filtered[k] = v;
  }

  const resolved = resolveUnityGatewayBearer(explicitEmail);
  if (resolved && resolved.token) {
    // Remove any existing case-variant Authorization key
    for (const key of Object.keys(filtered)) {
      if (key.toLowerCase() === 'authorization') {
        delete filtered[key];
      }
    }
    filtered['Authorization'] = `Bearer ${resolved.token}`;
  }

  return { headers: filtered, resolved };
}

function wrapFetchImplementation(originalFetch, label) {
  if (typeof originalFetch !== 'function' || originalFetch.__databricksOboWrapped) {
    return originalFetch;
  }

  const wrapped = async function databricksOboFetch(input, init) {
    let urlString = '';
    try {
      if (typeof input === 'string') {
        urlString = input;
      } else if (input instanceof URL) {
        urlString = input.toString();
      } else if (input && typeof input.url === 'string') {
        urlString = input.url;
      }
    } catch (_) {
      // ignore
    }

    if (isUnityGatewayUrl(urlString)) {
      const mergedInit = Object.assign({}, init || {});
      const reqHeaders = input && typeof input === 'object' ? input.headers : null;
      const { headers, resolved } = extractAndRewriteFetchHeaders(mergedInit.headers, reqHeaders);
      mergedInit.headers = headers;
      const method = (mergedInit.method || (input && input.method) || 'POST').toUpperCase();
      console.log(
        `[UnityGateway-OBO:${label}] ${method} ${urlString} -> auth=${resolved ? resolved.source : 'none'}`,
      );
      return originalFetch.call(this, input, mergedInit);
    }

    return originalFetch.apply(this, arguments);
  };

  wrapped.__databricksOboWrapped = true;
  return wrapped;
}

if (typeof globalThis.fetch === 'function') {
  globalThis.fetch = wrapFetchImplementation(globalThis.fetch, 'global-fetch');
}

// -----------------------------------------------------------------------------
// 3. Patch `undici` Dispatcher.dispatch + undici.fetch (used by MCP & OpenAI)
// -----------------------------------------------------------------------------
function patchUndiciDispatcherProto(proto, protoName) {
  if (!proto || typeof proto.dispatch !== 'function' || proto.dispatch.__databricksOboWrapped) {
    return;
  }
  const origDispatch = proto.dispatch;
  const wrappedDispatch = function databricksOboDispatch(options, handler) {
    try {
      if (options && options.path) {
        const origin = options.origin ? String(options.origin) : '';
        const fullUrl = origin ? `${origin.replace(/\/+$/, '')}${options.path}` : '';
        if (isUnityGatewayUrl(fullUrl)) {
          let explicitEmail = null;
          if (Array.isArray(options.headers)) {
            const newHeaders = [];
            for (let i = 0; i < options.headers.length; i += 2) {
              const k = String(options.headers[i] || '');
              const v = String(options.headers[i + 1] || '');
              const lower = k.toLowerCase();
              if (lower === 'x-librechat-user-email') {
                explicitEmail = v;
                continue;
              }
              if (lower === 'x-librechat-user-id' || lower === 'authorization') {
                continue;
              }
              newHeaders.push(k, v);
            }
            const resolved = resolveUnityGatewayBearer(explicitEmail);
            if (resolved && resolved.token) {
              newHeaders.push('authorization', `Bearer ${resolved.token}`);
              console.log(
                `[UnityGateway-OBO:${protoName}] ${options.method || 'POST'} ${fullUrl} -> auth=${resolved.source}`,
              );
            }
            options.headers = newHeaders;
          } else if (options.headers && typeof options.headers === 'object') {
            const { headers, resolved } = extractAndRewriteFetchHeaders(options.headers);
            options.headers = headers;
            if (resolved) {
              console.log(
                `[UnityGateway-OBO:${protoName}] ${options.method || 'POST'} ${fullUrl} -> auth=${resolved.source}`,
              );
            }
          }
        }
      }
    } catch (e) {
      console.warn('[UnityGateway-OBO] Dispatcher header rewrite warning:', e.message);
    }
    return origDispatch.call(this, options, handler);
  };
  wrappedDispatch.__databricksOboWrapped = true;
  proto.dispatch = wrappedDispatch;
}

function patchUndiciModule(undiciMod) {
  if (!undiciMod || undiciMod.__databricksOboPatched) {
    return undiciMod;
  }
  try {
    if (typeof undiciMod.fetch === 'function') {
      undiciMod.fetch = wrapFetchImplementation(undiciMod.fetch, 'undici-fetch');
    }
    for (const clsName of ['Agent', 'Client', 'Pool', 'ProxyAgent', 'Dispatcher']) {
      if (undiciMod[clsName] && undiciMod[clsName].prototype) {
        patchUndiciDispatcherProto(undiciMod[clsName].prototype, `undici-${clsName}`);
      }
    }
    undiciMod.__databricksOboPatched = true;
  } catch (e) {
    console.warn('[UnityGateway-OBO] Warning patching undici module:', e.message);
  }
  return undiciMod;
}

const origLoad = Module._load;
Module._load = function patchedModuleLoad(request, parent, isMain) {
  const loaded = origLoad.apply(this, arguments);
  if (request === 'undici' || (typeof request === 'string' && request.startsWith('undici/'))) {
    return patchUndiciModule(loaded);
  }
  return loaded;
};

// -----------------------------------------------------------------------------
// 4. Patch `https.request` / `http.request` as defense-in-depth for SDKs
// -----------------------------------------------------------------------------
function patchNodeHttpRequest(mod, defaultProtocol) {
  const origRequest = mod.request;
  if (origRequest.__databricksOboWrapped) return;

  const wrappedRequest = function databricksOboNodeRequest(input, options, cb) {
    try {
      let targetUrl = '';
      let optsObj = null;
      if (typeof input === 'string' || input instanceof URL) {
        targetUrl = String(input);
        optsObj = typeof options === 'object' && options !== null ? options : null;
      } else if (typeof input === 'object' && input !== null) {
        optsObj = input;
        const host = optsObj.hostname || optsObj.host || '';
        const path = optsObj.path || '/';
        const proto = optsObj.protocol || defaultProtocol;
        if (host) {
          targetUrl = `${proto}//${host}${path}`;
        }
      }

      if (isUnityGatewayUrl(targetUrl) && optsObj) {
        const { headers, resolved } = extractAndRewriteFetchHeaders(optsObj.headers || {});
        optsObj.headers = headers;
        if (resolved) {
          console.log(
            `[UnityGateway-OBO:node-${defaultProtocol.replace(':', '')}] ${optsObj.method || 'POST'} ${targetUrl} -> auth=${resolved.source}`,
          );
        }
      }
    } catch (_) {
      // ignore
    }
    return origRequest.apply(this, arguments);
  };

  wrappedRequest.__databricksOboWrapped = true;
  mod.request = wrappedRequest;
}

patchNodeHttpRequest(https, 'https:');
patchNodeHttpRequest(http, 'http:');

console.log('[UnityGateway-OBO] Preload hook initialized for Databricks Unity Gateway OBO auth');
