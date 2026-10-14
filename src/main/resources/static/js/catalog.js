/*
 * Event Catalog screen: card grid of discovered event types, detail panel (fields + example payload).
 * Backend: GET /api/events?offset=&limit= (paginated, all pages are loaded), GET /api/events/{t}?format=,
 * GET /api/events/{t}/example?format=. The same event name can exist in both formats (PingEvent), so every
 * per-type call passes `format`.
 *
 * The per-card "Register / Update Schema" button only dispatches the document-level 'register-schema' event
 * ({eventType, format}); the registration modal (registration.js) owns everything that follows. This file never
 * calls any registration endpoint itself.
 */
(function () {
  'use strict';

  const h = App.h;
  const PAGE_SIZE = 50;
  const MAX_PAGES = 100; // safety net against a misbehaving server

  function formatBadge(format) {
    const f = String(format || '').toUpperCase();
    return h('span', { class: 'badge ' + (f === 'PROTOBUF' ? 'badge-protobuf' : 'badge-avro') }, f);
  }
  function keyOf(t) { return t.name + '|' + t.format; }
  function pretty(text) {
    try { return JSON.stringify(JSON.parse(text), null, 2); } catch (e) { return String(text); }
  }

  async function loadAll() {
    const items = [];
    let total = 0;
    for (let page = 0; page < MAX_PAGES; page++) {
      const res = await ApiClient.get('/api/events', { query: { offset: items.length, limit: PAGE_SIZE } });
      const batch = (res && res.items) || [];
      total = res && typeof res.total === 'number' ? res.total : items.length + batch.length;
      batch.forEach(function (t) { items.push(t); });
      if (batch.length === 0 || items.length >= total) break;
    }
    return { items: items, total: total };
  }

  function mount(root, ctx) {
    let types = [];
    let selected = null;      // key of the selected card
    let detailSeq = 0;        // guards against out-of-order detail responses
    const cardEls = {};

    const subtitle = h('span', { class: 'muted', id: 'catalog-count', 'aria-live': 'polite' }, '');
    const grid = h('div', { class: 'catalog-grid', id: 'catalog-grid' });
    const status = h('div', { id: 'catalog-status', 'aria-live': 'polite' });
    const detail = h('div', { id: 'catalog-detail', 'aria-live': 'polite' });

    root.appendChild(h('div', { id: 'catalog-screen', class: 'page-wide-inner' },
      h('div', { class: 'row-between' }, h('h1', { class: 'm-0' }, 'Event Catalog'), subtitle),
      status, grid, detail));

    load();

    async function load() {
      status.replaceChildren(h('div', { class: 'empty-hint' }, 'Loading event types...'));
      grid.replaceChildren();
      detail.replaceChildren();
      try {
        const res = await loadAll();
        if (!ctx.isActive()) return;
        types = res.items;
        subtitle.textContent = res.total + ' event types discovered';
        status.replaceChildren();
        renderGrid();
      } catch (err) {
        if (!ctx.isActive()) return;
        subtitle.textContent = '';
        status.replaceChildren(App.renderProblem(err),
          h('button', { type: 'button', class: 'btn-ghost small mt-sm', onclick: load }, 'Retry'));
      }
    }

    function renderGrid() {
      grid.replaceChildren();
      Object.keys(cardEls).forEach(function (k) { delete cardEls[k]; });
      if (types.length === 0) {
        status.replaceChildren(h('div', { class: 'empty-hint', id: 'catalog-empty' },
          'No event types found. Add an .avsc under schemas/avro or a .proto under schemas/proto and restart.'));
        return;
      }
      types.forEach(function (t) {
        const registerBtn = h('button', {
          type: 'button', class: 'btn-ghost small', 'data-annot': 'register-action',
          onclick: function (e) {
            e.stopPropagation();
            document.dispatchEvent(new CustomEvent('register-schema', { detail: { eventType: t.name, format: t.format } }));
          },
          onkeydown: function (e) { e.stopPropagation(); }
        }, 'Register / Update Schema →');
        const card = h('div', {
          class: 'catalog-card', role: 'button', tabindex: '0', 'data-annot': 'catalog-card',
          'aria-pressed': 'false',
          onclick: function () { select(t); },
          onkeydown: function (e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); select(t); } }
        },
          h('div', { class: 'card-head' }, formatBadge(t.format), h('strong', null, t.name),
            t.name === 'PingEvent' ? h('span', { class: 'tag' }, 'sample') : null),
          h('p', { class: 'card-desc' }, t.description || 'No description available.'),
          registerBtn);
        cardEls[keyOf(t)] = card;
        grid.appendChild(card);
      });
      if (selected && cardEls[selected]) markSelected(selected);
    }

    function markSelected(key) {
      Object.keys(cardEls).forEach(function (k) {
        cardEls[k].classList.toggle('selected', k === key);
        cardEls[k].setAttribute('aria-pressed', k === key ? 'true' : 'false');
      });
    }

    async function select(t) {
      selected = keyOf(t);
      markSelected(selected);
      const seq = ++detailSeq;
      detail.replaceChildren(h('div', { class: 'empty-hint' }, 'Loading ' + t.name + '...'));
      try {
        const q = { format: t.format };
        const name = encodeURIComponent(t.name);
        const results = await Promise.all([
          ApiClient.get('/api/events/' + name, { query: q }),
          ApiClient.get('/api/events/' + name + '/example', { query: q })
        ]);
        if (!ctx.isActive() || seq !== detailSeq) return;
        renderDetail(results[0] || t, results[1]);
      } catch (err) {
        if (!ctx.isActive() || seq !== detailSeq) return;
        if (err && err.errorCode === 'UNKNOWN_EVENT_TYPE') {
          detail.replaceChildren(h('div', { class: 'banner warning', role: 'alert' },
            'Event type ' + t.name + ' (' + t.format + ') is no longer in the catalog. The list has been reloaded.'));
          selected = null;
          const notice = detail.firstChild;
          await load();
          if (ctx.isActive()) detail.replaceChildren(notice);
        } else {
          detail.replaceChildren(App.renderProblem(err));
        }
      }
    }

    function renderDetail(desc, example) {
      const fields = desc.fields || [];
      const fieldsBody = fields.length === 0
        ? h('p', { class: 'help-text' }, 'This event type declares no fields.')
        : h('table', { class: 'fields-table' },
          h('tbody', null, fields.map(function (f) {
            return h('tr', null,
              h('td', null, f.name),
              h('td', null, f.type, f.nullable ? h('span', { class: 'tag nullable-tag', title: 'nullable' }, 'nullable') : null));
          })));
      detail.replaceChildren(h('div', { class: 'detail-panel' },
        h('h2', null, desc.name, ' ', formatBadge(desc.format)),
        h('div', { class: 'detail-grid' },
          h('div', null, h('label', null, 'Fields'), fieldsBody),
          h('div', null, h('label', null, 'Example Payload'),
            h('pre', { class: 'json-block' }, pretty(example && example.payload))))));
    }
  }

  App.registerScreen('catalog', { mount: mount, unmount: function () {} });
})();
