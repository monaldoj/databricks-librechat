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
 *    authenticated Databricks user.
 *
 * Fails CLOSED: a token is only ever resolved for the user who made the request.
 * There is deliberately no "most recent user" fallback -- that would let one
 * user's prompt be authorized, billed and audited as another user, which defeats
 * the entire point of per-user Unity Gateway governance. If no token can be
 * resolved for the caller, the Authorization header is removed so the call fails
 * visibly with a 401 instead of silently running as the app Service Principal.
 * Set DATABRICKS_OBO_ALLOW_SP_FALLBACK=true to accept that privilege downgrade.
 */

'use strict';

const fs = require('node:fs');
const http = require('node:http');
const https = require('node:https');
const Module = require('node:module');
const { AsyncLocalStorage } = require('node:async_hooks');
const { randomUUID } = require('node:crypto');

const oboStorage = new AsyncLocalStorage();
const tokensByEmail = new Map();

const OBO_CACHE_FILE = process.env.DATABRICKS_OBO_CACHE_FILE || '/tmp/databricks_obo_tokens.json';
const SP_TOKEN_FILE = process.env.DATABRICKS_SP_TOKEN_FILE || '/tmp/databricks_sp_token';
const ALLOW_SP_FALLBACK =
  String(process.env.DATABRICKS_OBO_ALLOW_SP_FALLBACK || 'false').toLowerCase() === 'true';

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
  // Email-scoped only. The shared cache no longer carries a "latest token", so
  // there is no way for this to hand back a different user's credential.
  if (!email) return null;
  try {
    if (!fs.existsSync(OBO_CACHE_FILE)) return null;
    const data = JSON.parse(fs.readFileSync(OBO_CACHE_FILE, 'utf8'));
    const key = email.toLowerCase();
    if (data.by_email && data.by_email[key]) {
      return { token: data.by_email[key], source: `OBO(shared:${key})` };
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
  // 1. The token carried by the in-flight request -- the normal path.
  const store = oboStorage.getStore();
  if (store && store.token) {
    return {
      token: store.token,
      source: `OBO(async-context:${store.email || 'active-request'})`,
    };
  }

  // 2. A token cached for THIS caller's verified email, for calls made outside
  //    the request's async context.
  const emailKey = (explicitEmail || (store && store.email) || '').trim().toLowerCase();
  if (emailKey && !emailKey.includes('{{')) {
    const memToken = tokensByEmail.get(emailKey);
    if (memToken) {
      return { token: memToken, source: `OBO(user-map:${emailKey})` };
    }
    const shared = readSharedOboToken(emailKey);
    if (shared && shared.token) {
      return shared;
    }
  }

  // 3. Opt-in only: run as the app Service Principal. This is a privilege
  //    change, not a retry, so it is off by default.
  if (ALLOW_SP_FALLBACK) {
    const spToken = readServicePrincipalFallbackToken();
    if (spToken) {
      return { token: spToken, source: 'SP-fallback(app-service-principal)' };
    }
  }

  // 4. Fail closed.
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
// 0. Provider compatibility for Unity Gateway `/chat/completions`
// -----------------------------------------------------------------------------
// Every model shares one LibreChat custom endpoint, so LibreChat shapes every
// request the same (OpenAI) way. Three provider rules on the gateway then break
// MCP tool use for everything except Claude. Each was reproduced directly
// against POST /ai-gateway/mlflow/v1/chat/completions:
//
// 1. GPT-5.6 / GPT-6 (sol, terra, luna, astra) reason by default and reject
//    function tools on Chat Completions while reasoning:
//      Function tools with reasoning_effort are not supported for gpt-5.6-sol
//      in /v1/chat/completions. To use function tools, use /v1/responses or
//      set reasoning_effort to 'none'.
//    start_app.py already puts these models on a separate LibreChat endpoint
//    that uses the Responses API (RESPONSES_API_MODELS), so this is only a
//    backstop for one served over Chat Completions anyway: when tools are
//    attached, send `reasoning_effort: "none"`. That cannot rescue
//    gpt-6-1-sol / gpt-6-astra, which reject 'none'.
// 2. Gemini rejects `stream_options`, which LibreChat sends on every streamed
//    request, tools or not:
//      Invalid JSON payload received. Unknown name "stream_options" at
//      'generation_config': Cannot find field.
// 3. Gemini 3 requires the `thoughtSignature` it attached to each tool call to
//    be sent back with that call on the next turn:
//      Function call is missing a thought_signature in functionCall parts.
//    LibreChat's OpenAI message conversion drops the field. Fix: remember the
//    signatures from responses and re-attach them. If one is unknown (e.g. the
//    conversation predates a restart), use Google's documented
//    validator-skip value instead.
// 4. Gemini uses the function name as the tool call id, so calling the same
//    tool twice in one turn (e.g. `genie_poll_response` while Genie is still
//    working) yields two calls with the same id. LibreChat's ToolNode skips
//    any call whose id already has a tool result, so the repeat never runs,
//    and the next request ends on an unanswered call:
//      Requests ending with a model turn are not supported.
//    Fix: give each Gemini tool call a unique id in the response before
//    LibreChat sees it. The gateway accepts any id on the way back.
const GPT_TOOLS_REQUIRE_NO_REASONING = /(?:^|[./])gpt-(?:5-6|6(?:-\d+)?)(?:-|$)/i;
const GEMINI_MODEL = /gemini-/i;
const GEMINI_SKIP_SIGNATURE = 'skip_thought_signature_validator';
const GEMINI_SIGNATURE_CACHE_MAX = 5000;
const geminiSignatures = new Map();

function isChatCompletionsUrl(urlStr) {
  try {
    return new URL(urlStr).pathname.endsWith('/chat/completions');
  } catch (_) {
    return false;
  }
}

// Gemini reuses the function name as the tool call id, so the id alone is not
// unique. Arguments are normalized because LibreChat re-serializes them.
function geminiToolCallKey(call) {
  const fn = (call && call.function) || {};
  let args = fn.arguments == null ? '' : String(fn.arguments);
  try {
    args = JSON.stringify(JSON.parse(args));
  } catch (_) {
    // keep the raw string
  }
  return `${call.id || ''}|${fn.name || ''}|${args}`;
}

function rememberGeminiSignature(call) {
  if (!call || typeof call.thoughtSignature !== 'string' || !call.thoughtSignature) return;
  const key = geminiToolCallKey(call);
  geminiSignatures.delete(key);
  geminiSignatures.set(key, call.thoughtSignature);
  if (geminiSignatures.size > GEMINI_SIGNATURE_CACHE_MAX) {
    geminiSignatures.delete(geminiSignatures.keys().next().value);
  }
}

function applyGatewayCompatibility(urlString, body) {
  const result = { body, changes: [], gemini: false, stream: false };
  if (typeof body !== 'string' || !isChatCompletionsUrl(urlString)) return result;
  let payload;
  try {
    payload = JSON.parse(body);
  } catch (_) {
    return result;
  }
  const model = typeof payload.model === 'string' ? payload.model : '';
  result.stream = payload.stream === true;

  const hasTools = Array.isArray(payload.tools) && payload.tools.length > 0;
  if (hasTools && GPT_TOOLS_REQUIRE_NO_REASONING.test(model) && payload.reasoning_effort !== 'none') {
    payload.reasoning_effort = 'none';
    result.changes.push('reasoning_effort=none');
  }

  if (GEMINI_MODEL.test(model)) {
    result.gemini = true;
    if ('stream_options' in payload) {
      delete payload.stream_options;
      result.changes.push('-stream_options');
    }
    let restored = 0;
    let skipped = 0;
    for (const msg of Array.isArray(payload.messages) ? payload.messages : []) {
      if (!msg || msg.role !== 'assistant' || !Array.isArray(msg.tool_calls)) continue;
      for (const call of msg.tool_calls) {
        if (!call || call.thoughtSignature) continue;
        const known = geminiSignatures.get(geminiToolCallKey(call));
        call.thoughtSignature = known || GEMINI_SKIP_SIGNATURE;
        if (known) restored += 1;
        else skipped += 1;
      }
    }
    if (restored) result.changes.push(`thoughtSignature restored x${restored}`);
    if (skipped) result.changes.push(`thoughtSignature skip-validator x${skipped}`);
  }

  if (result.changes.length) {
    result.body = JSON.stringify(payload);
    console.log(`[UnityGateway-Compat] ${model}: ${result.changes.join(', ')}`);
  }
  return result;
}

function uniqueGeminiToolCallId() {
  return `call_${randomUUID().replace(/-/g, '').slice(0, 24)}`;
}

// Rewrites one parsed chunk/completion in place: unique tool call ids, and
// signatures recorded under those ids. `slots` keeps one id per tool call
// across the chunks of a stream.
function rewriteGeminiToolCalls(data, slots, stream) {
  let changed = false;
  for (const choice of (data && data.choices) || []) {
    const holder = stream ? choice.delta : choice.message;
    for (const tc of (holder && holder.tool_calls) || []) {
      const slot = `${choice.index || 0}:${stream ? tc.index || 0 : tc.id}`;
      const entry = slots.get(slot) || {
        id: uniqueGeminiToolCallId(),
        function: { name: '', arguments: '' },
        thoughtSignature: '',
      };
      if (tc.id) {
        tc.id = entry.id;
        changed = true;
      }
      if (tc.function && tc.function.name) entry.function.name = tc.function.name;
      if (tc.function && tc.function.arguments) entry.function.arguments += tc.function.arguments;
      if (tc.thoughtSignature) entry.thoughtSignature = tc.thoughtSignature;
      slots.set(slot, entry);
    }
  }
  return changed;
}

function rewriteGeminiSseLine(line, slots) {
  const trimmed = line.trim();
  if (!trimmed.startsWith('data:')) return line;
  const data = trimmed.slice(5).trim();
  if (!data || data === '[DONE]' || !data.includes('tool_calls')) return line;
  try {
    const chunk = JSON.parse(data);
    return rewriteGeminiToolCalls(chunk, slots, true) ? `data: ${JSON.stringify(chunk)}` : line;
  } catch (_) {
    return line;
  }
}

// Returns a Response whose Gemini tool call ids are unique (see 4. above),
// recording each call's thoughtSignature (see 3.) as it passes through.
async function transformGeminiResponse(response, stream) {
  if (!response || !response.ok || !response.body) return response;
  const headers = new Headers(response.headers);
  headers.delete('content-length');
  headers.delete('content-encoding');
  const init = { status: response.status, statusText: response.statusText, headers };
  const slots = new Map();
  const finish = () => {
    for (const call of slots.values()) rememberGeminiSignature(call);
  };
  let body;
  if (stream) {
    const decoder = new TextDecoder();
    const encoder = new TextEncoder();
    let buffer = '';
    body = response.body.pipeThrough(
      new TransformStream({
        transform(piece, controller) {
          buffer += decoder.decode(piece, { stream: true });
          let newline;
          while ((newline = buffer.indexOf('\n')) >= 0) {
            const line = buffer.slice(0, newline);
            buffer = buffer.slice(newline + 1);
            controller.enqueue(encoder.encode(`${rewriteGeminiSseLine(line, slots)}\n`));
          }
        },
        flush(controller) {
          buffer += decoder.decode();
          if (buffer) controller.enqueue(encoder.encode(rewriteGeminiSseLine(buffer, slots)));
          finish();
        },
      }),
    );
  } else {
    const text = await response.text();
    body = text;
    try {
      const data = JSON.parse(text);
      if (rewriteGeminiToolCalls(data, slots, false)) body = JSON.stringify(data);
      finish();
    } catch (_) {
      // not JSON; pass through untouched
    }
  }
  const out = new Response(body, init);
  Object.defineProperty(out, 'url', { value: response.url });
  return out;
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

    // Only ever cached against a verified email. An unkeyed token would be
    // indistinguishable from another user's and could not be used safely.
    if (typeof oboToken === 'string' && oboToken.trim().length > 0 && email) {
      tokensByEmail.set(email, oboToken.trim());
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

  // Always drop the inbound Authorization header. librechat.yaml sets it to
  // `Bearer ${DATABRICKS_TOKEN}` (the app Service Principal), so leaving it in
  // place when no user token resolved would quietly run the call with the SP's
  // privileges instead of the caller's.
  for (const key of Object.keys(filtered)) {
    if (key.toLowerCase() === 'authorization') {
      delete filtered[key];
    }
  }
  if (resolved && resolved.token) {
    filtered['Authorization'] = `Bearer ${resolved.token}`;
  } else {
    console.warn(
      '[UnityGateway-OBO] No per-user OBO token for this request; sending it without an ' +
        'Authorization header so it fails visibly rather than running as the app service ' +
        'principal. Set DATABRICKS_OBO_ALLOW_SP_FALLBACK=true to allow that fallback.',
    );
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

      const compat = applyGatewayCompatibility(urlString, mergedInit.body);
      if (compat.changes.length) {
        mergedInit.body = compat.body;
        for (const key of Object.keys(headers)) {
          if (key.toLowerCase() === 'content-length') delete headers[key];
        }
      }
      const response = await originalFetch.call(this, input, mergedInit);
      if (compat.gemini) {
        try {
          return await transformGeminiResponse(response, compat.stream);
        } catch (e) {
          console.warn('[UnityGateway-Compat] Gemini response rewrite warning:', e.message);
        }
      }
      return response;
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
