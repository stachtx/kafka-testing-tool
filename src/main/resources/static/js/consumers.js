/*
 * Consumers screen: start bar, session cards, event viewer.
 * Backend: GET/POST /api/consumers, GET /api/consumers/{id}/events?sinceOffset=, DELETE /api/consumers/{id}.
 *
 * The session list is always rebuilt from the server (on mount and every 2s), so a page reload or a screen switch
 * never orphans a running session. The event viewer polls with the returned nextOffset cursor and only appends
 * events it has not seen; rendering is capped at the newest MAX_ROWS rows.
 */
(function () {
  'use strict';

  const h = App.h;
  const FORMATS = ['AVRO', 'PROTOBUF'];
  const MODES = ['LATEST', 'EARLIEST'];
  const MAX_ROWS = 100;
  const TOPIC_RE = /^[A-Za-z0-9._-]{1,249}$/;

  function pad(n) { return n < 10 ? '0' + n : String(n); }
  function localTime(iso) {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return String(iso);
    return pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  }
  // Record timestamps are epoch millis; shown in local time like the session "since" label.
  function recordTime(ts) {
    if (ts === null || ts === undefined) return '';
    return localTime(ts);
  }
  function pretty(text) {
    try { return JSON.stringify(JSON.parse(text), null, 2); } catch (e) { return String(text); }
  }

  function mount(root, ctx) {
    let format = 'AVRO';
    let mode = 'LATEST';
    let starting = false;
    let selectedId = null;
    let knownCap = null;                 // effectiveCap learned from any EARLIEST session reported by the server
    let configuredCap = null;            // earliestCap from GET /api/kafka/health (read once on mount)
    const cards = {};                    // sessionId -> { el, session, refs }
    const viewers = {};                  // sessionId -> { cursor, rows[], list, missed, missedEl, empty }
    const stopping = {};                 // sessionId -> true while a DELETE is in flight

    /* ---------- start bar ---------- */
    const topicInput = h('input', {
      class: 'input input-grow', id: 'consumers-topic', autocomplete: 'off',
      placeholder: 'any DEV topic, e.g. orders.created', 'aria-label': 'Topic',
      oninput: function () { clearStartError(); },
      onkeydown: function (e) { if (e.key === 'Enter') onStartClick(); }
    });
    const formatButtons = FORMATS.map(function (f) {
      return h('button', {
        type: 'button', class: 'seg-btn' + (f === format ? ' active' : ''), 'data-format': f,
        onclick: function () { format = f; syncSeg(formatButtons, 'data-format', f); }
      }, f);
    });
    const modeButtons = MODES.map(function (m) {
      return h('button', {
        type: 'button', class: 'seg-btn' + (m === mode ? ' active' : ''), 'data-mode': m,
        onclick: function () { mode = m; syncSeg(modeButtons, 'data-mode', m); }
      }, m);
    });
    const startBtn = h('button', {
      type: 'button', class: 'btn-primary nowrap', id: 'consumers-start', 'data-annot': 'start', onclick: onStartClick
    }, '▶ Start Consumer');
    const startError = h('div', { id: 'consumers-start-error', 'aria-live': 'polite' });

    function syncSeg(buttons, attr, value) {
      buttons.forEach(function (b) { b.classList.toggle('active', b.getAttribute(attr) === value); });
    }
    function clearStartError() { startError.replaceChildren(); }

    /* ---------- layout ---------- */
    const sessionList = h('div', { class: 'session-list', id: 'consumers-session-list' });
    const emptyHint = h('div', { class: 'empty-hint', id: 'consumers-empty' }, 'No active sessions. Start a consumer above.');
    const helpHint = h('div', { class: 'empty-hint' },
      'Sessions poll every 2s. Stopping a session tears down its listener container immediately.');
    const notices = h('div', { id: 'consumers-notices', 'aria-live': 'polite' });
    const viewer = h('div', { class: 'event-viewer', id: 'consumers-viewer' });

    root.appendChild(h('div', { id: 'consumers-screen', class: 'page-wide-inner' },
      h('h1', null, 'Consumers'),
      h('div', { class: 'start-bar' },
        topicInput,
        h('div', { class: 'segmented small', role: 'group', 'aria-label': 'Format' }, formatButtons),
        h('div', { class: 'segmented small', role: 'group', 'aria-label': 'Offset mode' }, modeButtons),
        startBtn),
      startError,
      notices,
      h('div', { class: 'layout-split' },
        h('div', null, sessionList, helpHint),
        viewer)));
    renderViewerPlaceholder();
    renderEmptyState();

    // The health endpoint also reports the configured EARLIEST cap so the confirmation can name it exactly.
    ApiClient.get('/api/kafka/health').then(function (res) {
      if (res && Number.isInteger(res.earliestCap) && res.earliestCap > 0) configuredCap = res.earliestCap;
    }).catch(function () { /* keep the generic wording */ });

    /* ---------- start ---------- */
    function onStartClick() {
      if (starting) return;
      clearStartError();
      const topic = topicInput.value.trim();
      if (!topic) { showStartError('Topic is required'); return; }
      if (!TOPIC_RE.test(topic)) {
        showStartError('Topic may only contain letters, digits, ".", "_" and "-" (max 249 characters)');
        return;
      }
      if (mode === 'EARLIEST') confirmEarliest(topic, format); else doStart(topic, format, 'LATEST');
    }
    function showStartError(message) {
      startError.replaceChildren(h('div', { class: 'field-error' }, message));
    }

    function confirmEarliest(topic, fmt) {
      const cap = configuredCap !== null ? configuredCap : knownCap;
      const capText = cap !== null
        ? 'The session stops automatically after ' + cap + ' events (the configured EARLIEST cap).'
        : 'The server-side EARLIEST cap applies (operator-configured, default 1000 events); the exact value is shown on the session card once the session has started.';
      const dlg = App.openModal({
        title: 'Start EARLIEST consumer?',
        body: [
          h('p', null, 'This reads topic ', h('code', null, topic), ' (' + fmt + ') from the beginning of every partition.'),
          h('p', null, capText),
          h('p', null, 'The session auto-stops when the cap is reached. This may read a large amount of data from the shared DEV cluster.')
        ],
        footer: [
          h('button', { type: 'button', class: 'btn-ghost', id: 'consumers-earliest-cancel', onclick: function () { dlg.close(); } }, 'Cancel'),
          h('button', {
            type: 'button', class: 'btn-primary', id: 'consumers-earliest-confirm',
            onclick: function () { dlg.close(); doStart(topic, fmt, 'EARLIEST'); }
          }, 'Start EARLIEST consumer')
        ]
      });
    }

    function doStart(topic, fmt, offsetMode) {
      starting = true;
      startBtn.disabled = true;
      ApiClient.post('/api/consumers', { topic: topic, format: fmt, offsetMode: offsetMode })
        .then(function (session) {
          if (!ctx.isActive()) return;
          upsertCard(session);
          select(session.id);
          renderEmptyState();
          App.toast('Consumer started for ' + session.topic, 'success');
        })
        .catch(function (err) {
          if (!ctx.isActive()) return;
          startError.replaceChildren(App.renderProblem(err));
        })
        .finally(function () { starting = false; startBtn.disabled = false; });
    }

    /* ---------- session cards ---------- */
    function sinceLabel(s) {
      const modeText = s.offsetMode === 'EARLIEST'
        ? 'EARLIEST (capped ' + (s.effectiveCap !== null && s.effectiveCap !== undefined ? s.effectiveCap : '?') + ')'
        : s.offsetMode;
      return 'since: ' + localTime(s.startedAt) + ' - ' + modeText;
    }
    function statusLabel(s) {
      if (s.status === 'CAP_REACHED') return 'auto-stopped at cap';
      if (s.status === 'STARTING') return 'starting...';
      return null;
    }

    function upsertCard(s) {
      if (s.effectiveCap !== null && s.effectiveCap !== undefined) knownCap = s.effectiveCap;
      let card = cards[s.id];
      if (!card) {
        const refs = {
          since: h('div', null), events: h('div', null), status: h('div', { class: 'session-status' }),
          stop: h('button', { type: 'button', class: 'btn-stop small', 'data-role': 'stop', onclick: function (e) { e.stopPropagation(); stop(s.id); } }, '■ Stop')
        };
        const el = h('div', {
          class: 'session-card', 'data-annot': 'session-card', 'data-session-id': s.id, tabindex: '0',
          onclick: function () { select(s.id); },
          onkeydown: function (e) { if (e.target === el && (e.key === 'Enter' || e.key === ' ')) { e.preventDefault(); select(s.id); } }
        },
          h('div', { class: 'session-head' },
            h('span', { class: 'badge ' + (s.format === 'PROTOBUF' ? 'badge-protobuf' : 'badge-avro') }, s.format),
            h('strong', null, s.topic)),
          h('div', { class: 'session-meta' },
            h('div', null, 'group: ' + s.groupId), refs.since, refs.events, refs.status),
          refs.stop);
        card = cards[s.id] = { el: el, session: s, refs: refs };
        sessionList.appendChild(el);
      }
      card.session = s;
      card.refs.since.textContent = sinceLabel(s);
      card.refs.events.textContent = 'events: ' + s.eventCount;
      const st = statusLabel(s);
      card.refs.status.textContent = st || '';
      card.refs.status.hidden = !st;
      card.el.classList.toggle('active', s.id === selectedId);
      card.el.classList.toggle('capped', s.status === 'CAP_REACHED');
      return card;
    }

    function removeCard(id) {
      const card = cards[id];
      if (card) card.el.remove();
      delete cards[id];
      delete viewers[id];
      delete stopping[id];
      if (selectedId === id) {
        selectedId = null;
        const ids = Object.keys(cards);
        if (ids.length) select(ids[0]); else renderViewerPlaceholder();
      }
      renderEmptyState();
    }

    function notice(message) {
      const n = h('div', { class: 'banner warning' }, message);
      notices.appendChild(n);
      setTimeout(function () { n.remove(); }, 10000);
    }

    function renderEmptyState() {
      const none = Object.keys(cards).length === 0;
      if (none && !emptyHint.parentNode) sessionList.appendChild(emptyHint);
      if (!none && emptyHint.parentNode) emptyHint.remove();
    }

    function stop(id) {
      if (stopping[id]) return;
      stopping[id] = true;
      const card = cards[id];
      if (card) card.refs.stop.disabled = true;
      const topic = card ? card.session.topic : id;
      ApiClient.del('/api/consumers/' + encodeURIComponent(id))
        .then(function () {
          if (!ctx.isActive()) return;
          removeCard(id);
          App.toast('Stopped consumer for ' + topic, 'info');
        })
        .catch(function (err) {
          if (!ctx.isActive()) return;
          delete stopping[id];
          if (err && err.errorCode === 'CONSUMER_SESSION_NOT_FOUND') {
            removeCard(id);
            notice('Session for ' + topic + ' no longer exists (already stopped or auto-stopped); removed from the list.');
          } else {
            if (card) card.refs.stop.disabled = false;
            App.showError(err, 'Could not stop consumer');
          }
        });
    }

    /* ---------- event viewer ---------- */
    function select(id) {
      selectedId = id;
      Object.keys(cards).forEach(function (k) { cards[k].el.classList.toggle('active', k === id); });
      renderViewer();
      pollEvents(); // immediate refresh for the newly selected session
    }

    function renderViewerPlaceholder() {
      viewer.replaceChildren(h('div', { class: 'empty-hint' }, 'Select or start a session to see its events.'));
    }

    function viewerFor(id) {
      let v = viewers[id];
      if (!v) {
        v = viewers[id] = {
          cursor: 0, rows: [], missed: 0,
          list: h('div', { class: 'event-list' }),
          missedEl: h('div', { class: 'banner warning', hidden: true, role: 'status' }),
          empty: h('div', { class: 'empty-hint' }, 'Waiting for events...')
        };
        v.list.appendChild(v.empty);
      }
      return v;
    }

    function renderViewer() {
      const card = cards[selectedId];
      if (!card) { renderViewerPlaceholder(); return; }
      const s = card.session;
      const v = viewerFor(s.id);
      viewer.replaceChildren(
        h('div', { class: 'viewer-head' },
          h('strong', null, s.topic),
          h('span', { class: 'muted' }, ' · ' + s.groupId + ' · polling 2s')),
        v.missedEl,
        v.list);
    }

    function appendEvents(v, events) {
      if (!events.length) return;
      if (v.empty.parentNode) v.empty.remove();
      // Events arrive oldest-first; show newest on top.
      events.forEach(function (ev) {
        const row = buildRow(ev);
        v.list.insertBefore(row, v.list.firstChild);
        v.rows.push(row);
      });
      while (v.rows.length > MAX_ROWS) v.rows.shift().remove();
    }

    function buildRow(ev) {
      const detail = h('pre', { class: 'event-detail', hidden: true }, detailText(ev));
      const summary = h('div', {
        class: 'event-summary', role: 'button', tabindex: '0', 'aria-expanded': 'false',
        onclick: toggle, onkeydown: function (e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); } }
      },
        h('code', null, '#' + ev.offset),
        h('span', null, 'partition ' + ev.partition),
        h('span', null, recordTime(ev.timestamp)),
        ev.error ? h('span', { class: 'pill pill-danger' }, 'undecodable') : null);
      const row = h('div', { class: 'event-row', 'data-annot': 'event-row' }, summary, detail);
      function toggle() {
        const open = detail.hidden;
        detail.hidden = !open;
        row.classList.toggle('expanded', open);
        summary.setAttribute('aria-expanded', open ? 'true' : 'false');
      }
      return row;
    }

    function detailText(ev) {
      const lines = [];
      lines.push('key: ' + (ev.key === null || ev.key === undefined ? '(none)' : ev.key));
      const headers = ev.headers && Object.keys(ev.headers).length ? ev.headers : null;
      lines.push('headers: ' + (headers ? '{' + Object.keys(headers).map(function (k) { return k + ': ' + headers[k]; }).join(', ') + '}' : '{}'));
      if (ev.error) {
        lines.push('error: ' + ev.error);
        lines.push('payload: (not decoded) raw bytes are Base64-encoded below');
        lines.push('raw (base64): ' + (ev.rawBase64 || '(unavailable)'));
      } else {
        if (ev.payloadTruncated) lines.push('note: payload truncated at the server-side size limit (not valid JSON)');
        lines.push('payload: ' + (ev.payload === null || ev.payload === undefined ? '(null)' : pretty(ev.payload)));
      }
      return lines.join('\n');
    }

    /* ---------- polling ---------- */
    let listBusy = false;
    function pollList() {
      if (listBusy) return Promise.resolve();
      listBusy = true;
      return ApiClient.get('/api/consumers').then(function (sessions) {
        if (!ctx.isActive()) return;
        const seen = {};
        (sessions || []).forEach(function (s) {
          if (s.status === 'STOPPED') return;
          seen[s.id] = true;
          upsertCard(s);
        });
        Object.keys(cards).forEach(function (id) {
          if (!seen[id] && !stopping[id]) {
            const topic = cards[id].session.topic;
            removeCard(id);
            notice('Session for ' + topic + ' is no longer running; removed from the list.');
          }
        });
        if (!selectedId && Object.keys(cards).length) select(Object.keys(cards)[0]);
        renderEmptyState();
      }).catch(function (err) {
        if (ctx.isActive() && err && err.status === 0) App.showBanner('Cannot reach the backend; retrying...', 'error');
      }).finally(function () { listBusy = false; });
    }

    let eventsBusy = false;
    function pollEvents() {
      const id = selectedId;
      if (!id || eventsBusy || !cards[id]) return Promise.resolve();
      eventsBusy = true;
      const v = viewerFor(id);
      return ApiClient.get('/api/consumers/' + encodeURIComponent(id) + '/events', { query: { sinceOffset: v.cursor } })
        .then(function (res) {
          if (!ctx.isActive() || !cards[id]) return;
          App.clearBanner();
          appendEvents(v, res.events || []);
          v.cursor = res.nextOffset;
          if (res.missed > 0) {
            v.missed += res.missed;
            v.missedEl.textContent = v.missed + ' event(s) were missed: the session buffers only the newest 100 events and ' +
              'they were evicted before this page fetched them.';
            v.missedEl.hidden = false;
          }
        })
        .catch(function (err) {
          if (!ctx.isActive()) return;
          if (err && err.errorCode === 'CONSUMER_SESSION_NOT_FOUND') {
            const topic = cards[id] ? cards[id].session.topic : id;
            removeCard(id);
            notice('Session for ' + topic + ' no longer exists (stopped or auto-stopped); removed from the list.');
          }
        })
        .finally(function () { eventsBusy = false; });
    }

    // One shared 2s visibility-aware tick: rebuild the list from the server, then fetch new events for the viewer.
    ctx.poll(function () { return pollList().then(pollEvents); }, 2000);
  }

  App.registerScreen('consumers', { mount: mount, unmount: function () { } });
})();
