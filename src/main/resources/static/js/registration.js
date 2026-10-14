/*
 * Schema Registration modal (ADR-003). Loaded eagerly by the app shell so the document-level listener exists
 * before the Event Catalog can fire it.
 *
 * Trigger: document event 'register-schema' with detail {eventType, format} (dispatched by catalog.js).
 * Flow:    1. open the danger modal, 2. GET /api/schemas/{eventType}/registration-preview?format= (read-only),
 *          3. render the side-by-side diff + compatibility pill, 4. enable confirm ONLY when the preview loaded and is
 *          compatible and not identical, 5. confirm issues exactly one write call (see CONFIRM below).
 *
 * This is the ONLY script allowed to reference the registration write endpoint. Cancel / Esc / backdrop never write.
 * Schema text is rendered with textContent only. No dependencies beyond App and ApiClient.
 */
(function () {
  'use strict';

  const h = App.h;

  function pretty(text) {
    if (text === null || text === undefined) return '';
    try { return JSON.stringify(JSON.parse(text), null, 2); } catch (e) { return String(text); }
  }

  /* Line-oriented LCS diff. Returns per-side arrays of {text, changed}. Falls back to "no highlight" for huge inputs. */
  function diffLines(oldText, newText) {
    const a = oldText === '' ? [] : oldText.split('\n');
    const b = newText === '' ? [] : newText.split('\n');
    const left = a.map(function (t) { return { text: t, changed: false }; });
    const right = b.map(function (t) { return { text: t, changed: false }; });
    if (a.length * b.length > 4000000) return { left: left, right: right };
    const n = a.length;
    const m = b.length;
    const lcs = [];
    for (let i = 0; i <= n; i++) lcs.push(new Array(m + 1).fill(0));
    for (let i = n - 1; i >= 0; i--) {
      for (let j = m - 1; j >= 0; j--) {
        lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
      }
    }
    let i = 0;
    let j = 0;
    while (i < n && j < m) {
      if (a[i] === b[j]) { i++; j++; }
      else if (lcs[i + 1][j] >= lcs[i][j + 1]) { left[i].changed = true; i++; }
      else { right[j].changed = true; j++; }
    }
    while (i < n) { left[i++].changed = true; }
    while (j < m) { right[j++].changed = true; }
    return { left: left, right: right };
  }

  function diffBlock(lines, kind) {
    const pre = h('pre', { class: 'diff-block' });
    lines.forEach(function (l, idx) {
      const span = h('span', { class: l.changed ? 'diff-line ' + kind : 'diff-line' });
      span.textContent = l.text;
      pre.appendChild(span);
      if (idx < lines.length - 1) pre.appendChild(document.createTextNode('\n'));
    });
    return pre;
  }

  function pane(title, content) {
    return h('div', { class: 'diff-pane' }, h('h3', null, title), content);
  }

  let openModalHandle = null;

  function openRegistration(detail) {
    if (openModalHandle) return; // one registration modal at a time
    const eventType = detail && detail.eventType;
    const format = detail && detail.format;
    if (!eventType) return;

    let closed = false;
    let busy = false;      // true while the single register request is in flight (double-click guard)
    let done = false;      // true after a successful registration
    let previewOk = false; // true only for a loaded preview that is compatible and not identical

    const compatLine = h('div', { class: 'reg-compat-line' }, eventType + ' - compatibility: ',
      h('span', { class: 'pill pill-muted' }, '...'));
    const previewArea = h('div', { class: 'reg-preview' }, h('div', { class: 'empty-hint' }, 'Loading preview...'));
    const notice = h('div', { class: 'reg-notice', 'aria-live': 'polite' });
    const resultArea = h('div', { class: 'reg-result', 'aria-live': 'polite' });

    const cancelBtn = h('button', { type: 'button', class: 'btn-ghost', onclick: function () { modal.close(); } }, 'Cancel');
    const confirmBtn = h('button', { type: 'button', class: 'btn-danger', disabled: true, onclick: confirm },
      '⚠ Register New Version to DEV Registry');

    const body = [
      h('p', { class: 'reg-warning' }, 'This writes a new schema version to the shared enterprise Schema Registry. ' +
        "Other teams' consumers may be affected."),
      compatLine, previewArea, notice, resultArea
    ];
    const title = h('span', { class: 'reg-title' },
      h('span', { class: 'reg-warn-icon', 'aria-hidden': 'true' }, '⚠'), 'Register Schema to Shared DEV Registry');

    const modal = App.openModal({
      title: title, danger: true, body: body, footer: [cancelBtn, confirmBtn],
      onClose: function () { closed = true; openModalHandle = null; }
    });
    openModalHandle = modal;

    function updateConfirm() {
      if (previewOk && !busy && !done) confirmBtn.disabled = false;
      else confirmBtn.disabled = true;
    }

    function setNotice(kind, text, extra) {
      notice.replaceChildren(h('div', { class: 'banner ' + kind, role: 'alert' }, text, extra || null));
    }

    function renderCompat(preview) {
      const ok = preview.compatible === true;
      compatLine.replaceChildren(eventType + ' - compatibility: ',
        h('span', { class: 'pill ' + (ok ? 'pill-ok' : 'pill-danger') }, preview.compatibilityMode || 'UNKNOWN'));
    }

    function renderPreview(preview) {
      renderCompat(preview);
      const localText = pretty(preview.localSchema);
      let registryPane;
      let localPane;
      if (preview.firstRegistration || preview.registrySchema === null || preview.registrySchema === undefined) {
        registryPane = pane('Registry (no schema yet)',
          h('div', { class: 'empty-hint' }, 'No schema registered yet for subject ' + (preview.subject || eventType) + '.'));
        const all = diffLines('', localText);
        localPane = pane('Local schema (to register)', diffBlock(all.right, 'diff-add'));
      } else {
        const registryText = pretty(preview.registrySchema);
        const d = diffLines(registryText, localText);
        registryPane = pane('Registry (latest, v' + preview.registryVersion + ')', diffBlock(d.left, 'diff-del'));
        localPane = pane('Local schema (to register)', diffBlock(d.right, 'diff-add'));
      }
      previewArea.replaceChildren(h('div', { class: 'diff-grid' }, registryPane, localPane));

      if (preview.compatible === false) {
        const msgs = preview.compatibilityMessages || [];
        setNotice('error', 'Incompatible with the registry compatibility mode ' + (preview.compatibilityMode || '') +
          '. Registration is blocked.',
          msgs.length ? h('ul', { class: 'error-list' }, msgs.map(function (m) { return h('li', null, String(m)); })) : null);
      } else if (preview.identical === true) {
        setNotice('info', 'No changes: the local schema is identical to the latest registered version. Nothing to register.');
      } else {
        notice.replaceChildren();
        previewOk = true;
      }
      updateConfirm();
    }

    ApiClient.get('/api/schemas/' + encodeURIComponent(eventType) + '/registration-preview', { query: { format: format } })
      .then(function (preview) {
        if (closed) return;
        if (!preview) throw new ApiClient.ApiError({ status: 0, detail: 'Empty preview response.' });
        renderPreview(preview);
      })
      .catch(function (err) {
        if (closed) return;
        previewOk = false;
        previewArea.replaceChildren(App.renderProblem(err));
        setNotice('warning', 'The registration preview could not be loaded, so registration is disabled. Close and retry.');
        updateConfirm();
      });

    /* CONFIRM: the only write call of the whole UI. Disabled immediately so a double click sends one request. */
    function confirm() {
      if (busy || done || !previewOk) return;
      busy = true;
      previewOk = false;
      confirmBtn.disabled = true;
      cancelBtn.disabled = true;
      notice.replaceChildren();
      resultArea.replaceChildren(h('div', { class: 'empty-hint' }, 'Registering...'));
      ApiClient.post('/api/schemas/register', { eventType: eventType, format: format })
        .then(function (res) {
          done = true;
          if (closed) { App.toast('Schema registered: ' + res.subject + ' v' + res.version, 'success'); return; }
          resultArea.replaceChildren(h('div', { class: 'result-panel success', role: 'status' },
            h('strong', null, '✓ Schema registered'),
            h('div', { class: 'result-grid' },
              h('span', null, 'Subject'), h('code', null, res.subject),
              h('span', null, 'Version'), h('code', null, String(res.version)),
              h('span', null, 'Schema id'), h('code', null, String(res.schemaId)))));
        })
        .catch(function (err) {
          if (closed) { App.showError(err, 'Schema registration failed'); return; }
          resultArea.replaceChildren(App.renderProblem(err));
        })
        .finally(function () {
          busy = false;
          cancelBtn.disabled = false;
          cancelBtn.textContent = done ? 'Close' : 'Cancel';
          updateConfirm();
        });
    }
  }

  document.addEventListener('register-schema', function (e) { openRegistration(e.detail); });
})();
