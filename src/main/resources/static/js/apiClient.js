/*
 * ApiClient - fetch wrapper and shared visibility-aware poller. No dependencies.
 *
 *   ApiClient.get(path, {query})            -> Promise<json | null>
 *   ApiClient.post(path, body)              -> Promise<json | null>
 *   ApiClient.del(path)                     -> Promise<null>          (204 resolves to null)
 *   ApiClient.request(method, path, {query, body})
 *   ApiClient.ApiError                      Error subclass thrown on any non-2xx / network failure:
 *       .status (number, 0 for network failure), .errorCode (string|null), .detail (string),
 *       .correlationId (string|null), .errors ([{field,message}]), .title, .message (== detail)
 *   ApiClient.startPolling(fn, intervalMs)  -> { stop() }
 *       Calls fn immediately, then every intervalMs. Pauses while document.visibilityState !== "visible"
 *       and polls immediately on visibilitychange back to visible. Never overlaps calls (a slow fn is awaited).
 */
(function (global) {
  'use strict';

  class ApiError extends Error {
    constructor(init) {
      super(init.detail || init.title || 'Request failed');
      this.name = 'ApiError';
      this.status = init.status || 0;
      this.errorCode = init.errorCode || null;
      this.detail = init.detail || init.title || 'Request failed';
      this.correlationId = init.correlationId || null;
      this.errors = Array.isArray(init.errors) ? init.errors : [];
      this.title = init.title || null;
    }
  }

  function buildUrl(path, query) {
    if (!query) return path;
    const params = new URLSearchParams();
    Object.keys(query).forEach(function (k) {
      const v = query[k];
      if (v !== undefined && v !== null && v !== '') params.append(k, String(v));
    });
    const qs = params.toString();
    return qs ? path + (path.indexOf('?') >= 0 ? '&' : '?') + qs : path;
  }

  async function request(method, path, opts) {
    opts = opts || {};
    const init = { method: method, headers: { 'Accept': 'application/json, application/problem+json' } };
    if (opts.body !== undefined) {
      init.headers['Content-Type'] = 'application/json';
      init.body = JSON.stringify(opts.body);
    }
    let res;
    try {
      res = await fetch(buildUrl(path, opts.query), init);
    } catch (e) {
      throw new ApiError({ status: 0, errorCode: 'NETWORK_ERROR', detail: 'Cannot reach the Kafka Tester backend.' });
    }
    const correlationHeader = res.headers.get('X-Correlation-Id');
    if (res.ok) {
      if (res.status === 204) return null;
      const text = await res.text();
      return text ? JSON.parse(text) : null;
    }
    let problem = {};
    try {
      problem = JSON.parse(await res.text());
    } catch (e) { /* non-JSON error body */ }
    throw new ApiError({
      status: res.status,
      errorCode: problem.errorCode,
      detail: problem.detail || problem.title || (res.status + ' ' + res.statusText),
      title: problem.title,
      correlationId: problem.correlationId || correlationHeader,
      errors: problem.errors
    });
  }

  function startPolling(fn, intervalMs) {
    let stopped = false;
    let timer = null;
    let running = false;
    const visible = function () { return document.visibilityState === 'visible'; };

    async function tick() {
      if (stopped || running || !visible()) return;
      running = true;
      try {
        await fn();
      } catch (e) {
        if (global.console) console.warn('polling callback failed', e);
      } finally {
        running = false;
      }
    }
    function onVisibility() {
      if (visible()) tick();
    }

    document.addEventListener('visibilitychange', onVisibility);
    timer = setInterval(tick, intervalMs);
    tick();

    return {
      stop: function () {
        stopped = true;
        clearInterval(timer);
        document.removeEventListener('visibilitychange', onVisibility);
      }
    };
  }

  global.ApiClient = {
    ApiError: ApiError,
    request: request,
    get: function (path, opts) { return request('GET', path, opts); },
    post: function (path, body) { return request('POST', path, { body: body }); },
    del: function (path) { return request('DELETE', path); },
    startPolling: startPolling
  };
})(window);
