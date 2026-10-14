/*
 * App - UI shell: hash router, screen lifecycle, toasts/banner, modal helper, DOM helper.
 *
 * ==================== EXTENSION POINTS (Groups 9-11) ====================
 *
 * 1. Register a screen (one IIFE per screen file, loaded after app.js):
 *
 *      App.registerScreen('consumers', {            // route names: 'producer' | 'consumers' | 'catalog'
 *        mount(root, ctx) { ... },                  // root = <main id="screen-root"> (already emptied)
 *        unmount() { ... }                          // optional; called when the user leaves the screen
 *      });
 *
 *    ctx.poll(fn, ms)   -> handle {stop()}  starts a visibility-aware poller (ApiClient.startPolling) that the
 *                          shell stops automatically on screen exit. Use this for every polling loop.
 *    ctx.isActive()     -> false once the screen was left (guard async continuations before touching the DOM).
 *    ctx.navigate(name) -> change route.
 *    After unmount the shell also empties root, so screens need not clean up their DOM.
 *    Screen state that must survive navigation (e.g. running consumer sessions) must be rebuilt from the
 *    server on mount (GET /api/consumers).
 *
 * 2. Screen scripts: every screen script (producer, consumers, catalog, registration) is a static
 *    <script> tag in index.html, loaded after app.js. There is no dynamic loader (the page CSP is
 *    script-src 'self'), so a new screen needs a new tag there.
 *
 * 3. Catalog -> Registration modal event bus (plain DOM CustomEvent on document):
 *      document.dispatchEvent(new CustomEvent('register-schema', { detail: { eventType: 'PingEvent', format: 'AVRO' } }));
 *    registration.js (Group 11) listens for it and opens the modal. catalog.js dispatches it.
 *
 * 4. Helpers: App.h(tag, attrs, ...children) builds DOM safely (text children are escaped; attrs: class, on*,
 *    dataset via 'data-x', boolean props); App.toast(message, kind?, detail?) kind = 'info'|'success'|'error';
 *    App.showError(err, prefix?) toasts an ApiError with errorCode+correlationId; App.showBanner(msg, kind?)
 *    / App.clearBanner() for the page-level banner; App.renderProblem(err) returns a result-panel Node for an
 *    ApiError (errorCode, detail, correlation id, field errors); App.openModal({title, danger, body, footer})
 *    -> {el, body, footer, close()} rendering .modal-backdrop > .modal into #modal-root (Esc / backdrop closes).
 *    App.modalRoot is the raw slot for custom markup.
 * ========================================================================
 */
(function (global) {
  'use strict';

  const ROUTES = ['producer', 'consumers', 'catalog'];
  const DEFAULT_ROUTE = 'producer';
  const TITLES = { producer: 'Producer', consumers: 'Consumers', catalog: 'Event Catalog' };

  const screens = {};
  let current = null; // { name, screen, ctx, pollers[], active }

  /* ---------- DOM helper ---------- */
  function h(tag, attrs) {
    const el = document.createElement(tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (k) {
        const v = attrs[k];
        if (v === undefined || v === null || v === false) return;
        if (k.slice(0, 2) === 'on' && typeof v === 'function') el.addEventListener(k.slice(2), v);
        else if (k === 'class') el.className = v;
        else if (k === 'text') el.textContent = v;
        else if (k === 'value') el.value = v;
        else if (v === true) el.setAttribute(k, '');
        else el.setAttribute(k, v);
      });
    }
    for (let i = 2; i < arguments.length; i++) append(el, arguments[i]);
    return el;
  }
  function append(el, child) {
    if (child === undefined || child === null || child === false) return;
    if (Array.isArray(child)) child.forEach(function (c) { append(el, c); });
    else if (child instanceof Node) el.appendChild(child);
    else el.appendChild(document.createTextNode(String(child)));
  }

  /* ---------- Toasts and banner ---------- */
  function toast(message, kind, detail) {
    const root = document.getElementById('toast-root');
    const node = h('div', { class: 'toast ' + (kind || 'info'), role: kind === 'error' ? 'alert' : 'status' },
      message, detail ? h('small', null, detail) : null);
    root.appendChild(node);
    setTimeout(function () { node.remove(); }, kind === 'error' ? 8000 : 4000);
    return node;
  }
  function showError(err, prefix) {
    const msg = (prefix ? prefix + ': ' : '') + (err && err.detail ? err.detail : (err && err.message) || 'Unexpected error');
    const meta = [];
    if (err && err.errorCode) meta.push(err.errorCode);
    if (err && err.correlationId) meta.push('correlation id ' + err.correlationId);
    return toast(msg, 'error', meta.join(' / ') || null);
  }
  function showBanner(message, kind) {
    const root = document.getElementById('banner-root');
    root.replaceChildren(h('div', { class: 'banner ' + (kind || 'info'), role: 'alert' }, message));
  }
  function clearBanner() {
    document.getElementById('banner-root').replaceChildren();
  }

  /* ---------- ProblemDetail rendering ---------- */
  function renderProblem(err) {
    const panel = h('div', { class: 'result-panel error', role: 'alert' },
      h('strong', null, '✗ ' + (err && err.errorCode ? err.errorCode : 'Request failed')),
      h('div', { class: 'result-grid' },
        h('span', null, 'Error code'), h('code', null, (err && err.errorCode) || 'n/a'),
        h('span', null, 'Correlation id'), h('code', null, (err && err.correlationId) || 'n/a'),
        h('span', null, 'Detail'), h('code', null, (err && err.detail) || (err && err.message) || 'Unknown error'),
        h('span', null, 'HTTP status'), h('code', null, err && err.status ? String(err.status) : 'n/a')));
    if (err && err.errors && err.errors.length) {
      panel.appendChild(h('ul', { class: 'error-list' },
        err.errors.map(function (e) { return h('li', null, (e.field ? e.field + ': ' : '') + e.message); })));
    }
    return panel;
  }

  /* ---------- Modal ---------- */
  const modalRoot = function () { return document.getElementById('modal-root'); };
  function openModal(opts) {
    const body = h('div', { class: 'modal-body' }, opts.body);
    const footer = h('div', { class: 'modal-footer' }, opts.footer);
    const el = h('div', { class: 'modal' + (opts.danger ? ' modal-danger' : ''), role: 'dialog', 'aria-modal': 'true' },
      h('div', { class: 'modal-header' }, opts.title), body, footer);
    const backdrop = h('div', { class: 'modal-backdrop' }, el);
    function close() {
      document.removeEventListener('keydown', onKey);
      backdrop.remove();
      if (opts.onClose) opts.onClose();
    }
    function onKey(e) { if (e.key === 'Escape') close(); }
    backdrop.addEventListener('mousedown', function (e) { if (e.target === backdrop) close(); });
    document.addEventListener('keydown', onKey);
    modalRoot().appendChild(backdrop);
    return { el: el, body: body, footer: footer, close: close };
  }

  /* ---------- Screens and router ---------- */
  function registerScreen(name, screen) {
    screens[name] = screen;
  }

  function leaveCurrent() {
    if (!current) return;
    const leaving = current;
    leaving.active = false;
    leaving.pollers.forEach(function (p) { p.stop(); });
    leaving.pollers.length = 0;
    try {
      if (leaving.screen && typeof leaving.screen.unmount === 'function') leaving.screen.unmount();
    } catch (e) { console.error('screen unmount failed', e); }
    current = null;
  }

  function placeholder(root, name) {
    root.replaceChildren(h('div', { class: 'placeholder' },
      h('h1', null, TITLES[name] || name), 'This screen could not be loaded (its script is missing from index.html).'));
  }

  function render(name) {
    leaveCurrent();
    clearBanner();
    const root = document.getElementById('screen-root');
    root.replaceChildren();
    const screen = screens[name];
    const state = { name: name, screen: screen, pollers: [], active: true };
    current = state;
    if (!screen) {
      placeholder(root, name);
      return;
    }
    const ctx = {
      poll: function (fn, ms) {
        const handle = global.ApiClient.startPolling(fn, ms || 2000);
        if (state.active) state.pollers.push(handle); else handle.stop();
        return handle;
      },
      isActive: function () { return state.active; },
      navigate: navigate
    };
    state.ctx = ctx;
    try {
      screen.mount(root, ctx);
    } catch (e) {
      console.error('screen mount failed', e);
      showBanner('Failed to render ' + (TITLES[name] || name) + ': ' + e.message, 'error');
    }
  }

  function routeFromHash() {
    const m = /^#\/([a-z]+)/.exec(location.hash);
    return m && ROUTES.indexOf(m[1]) >= 0 ? m[1] : null;
  }
  function navigate(name) { location.hash = '#/' + name; }

  function onRouteChange() {
    const name = routeFromHash();
    if (!name) {
      location.replace('#/' + DEFAULT_ROUTE); // triggers hashchange again
      return;
    }
    document.querySelectorAll('.navlink').forEach(function (a) {
      const active = a.getAttribute('data-route') === name;
      a.classList.toggle('active', active);
      if (active) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current');
    });
    document.title = (TITLES[name] || name) + ' - Kafka Tester';
    render(name);
  }

  function start() {
    window.addEventListener('hashchange', onRouteChange);
    onRouteChange();
  }

  global.App = {
    ROUTES: ROUTES,
    registerScreen: registerScreen,
    navigate: navigate,
    h: h,
    toast: toast,
    showError: showError,
    showBanner: showBanner,
    clearBanner: clearBanner,
    renderProblem: renderProblem,
    openModal: openModal,
    get modalRoot() { return modalRoot(); }
  };

  // Scripts at the end of <body> have all run by DOMContentLoaded, so screens are registered before routing.
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})(window);
