/*
 * Producer screen: Topic, Format, Event Type, Payload (JSON), Key, Partition, Headers, Validate / Send Event,
 * result panel. Backend: GET /api/events, GET /api/events/{t}/example, POST /api/producer/validate|send.
 */
(function () {
  'use strict';

  const h = App.h;
  const FORMATS = ['AVRO', 'PROTOBUF'];

  function pretty(text) {
    try { return JSON.stringify(JSON.parse(text), null, 2); } catch (e) { return text; }
  }

  function mount(root, ctx) {
    let eventTypes = [];           // all EventType items from GET /api/events
    let format = 'AVRO';
    let topicIsAuto = false;       // topic was filled from the event type's defaultTopic (not typed by the user)
    let busy = false;

    /* ---------- fields ---------- */
    const topicInput = h('input', {
      class: 'input', id: 'producer-topic', list: 'producer-topics', autocomplete: 'off',
      placeholder: 'search or type a DEV topic...',
      oninput: function () { topicIsAuto = false; clearFieldError('topic'); }
    });
    const topicList = h('datalist', { id: 'producer-topics' });

    const formatButtons = FORMATS.map(function (f) {
      return h('button', {
        type: 'button', class: 'seg-btn' + (f === format ? ' active' : ''), 'data-format': f,
        onclick: function () { setFormat(f); }
      }, f);
    });
    const formatSeg = h('div', { class: 'segmented', role: 'group', 'aria-label': 'Format' }, formatButtons);

    const typeSelect = h('select', { class: 'input', id: 'producer-event-type', onchange: onTypeChange });

    const payloadArea = h('textarea', {
      class: 'json-editor', id: 'producer-payload', rows: '10', spellcheck: 'false',
      placeholder: 'Click "Load Example" to generate a sample payload for the selected event type',
      oninput: function () { clearFieldError('payload'); }
    });
    const payloadError = h('div', { class: 'field-error', id: 'producer-payload-error', hidden: true });

    const keyInput = h('input', { class: 'input', id: 'producer-key', placeholder: 'optional message key' });
    const partitionInput = h('input', {
      class: 'input', id: 'producer-partition', type: 'number', min: '0', step: '1', placeholder: 'auto',
      oninput: function () { clearFieldError('partition'); }
    });
    const partitionError = h('div', { class: 'field-error', hidden: true });
    const topicError = h('div', { class: 'field-error', hidden: true });

    const validateBtn = h('button', {
      type: 'button', class: 'btn-ghost', id: 'producer-validate', 'data-annot': 'validate',
      onclick: function () { submit('validate'); }
    }, 'Validate');
    const sendBtn = h('button', {
      type: 'button', class: 'btn-primary', id: 'producer-send', 'data-annot': 'send',
      onclick: function () { submit('send'); }
    }, 'Send Event ▶');

    const headersList = h('div', { class: 'headers-list', id: 'producer-headers' });
    const addHeaderBtn = h('button', {
      type: 'button', class: 'btn-ghost small', id: 'producer-add-header', onclick: function () { addHeaderRow('', ''); }
    }, '+ Add header');

    const resultSlot = h('div', { id: 'producer-result', 'aria-live': 'polite' });

    const loadExampleBtn = h('button', {
      type: 'button', class: 'btn-ghost', 'data-annot': 'load-example', onclick: loadExample
    }, 'Load Example');
    const formatJsonBtn = h('button', {
      type: 'button', class: 'btn-ghost', 'data-annot': 'format-json', onclick: formatJson
    }, 'Format JSON');

    root.appendChild(h('div', { id: 'producer-screen' },
      h('h1', null, 'Producer'),
      h('div', { class: 'form-grid' },
        h('div', { class: 'field' }, h('label', { for: 'producer-topic' }, 'Topic'), topicInput, topicList, topicError),
        h('div', { class: 'field' }, h('label', null, 'Format'), formatSeg),
        h('div', { class: 'field' }, h('label', { for: 'producer-event-type' }, 'Event Type'), typeSelect)),
      h('div', { class: 'field full' },
        h('div', { class: 'row-between' },
          h('label', { for: 'producer-payload' }, 'Payload (JSON)'),
          h('div', { class: 'btn-group' }, loadExampleBtn, formatJsonBtn)),
        payloadArea, payloadError),
      h('div', { class: 'form-grid three' },
        h('div', { class: 'field' }, h('label', { for: 'producer-key' }, 'Key'), keyInput),
        h('div', { class: 'field' }, h('label', { for: 'producer-partition' }, 'Partition'), partitionInput, partitionError),
        h('div', { class: 'field' }, h('label', null, ' '),
          h('div', { class: 'btn-group' }, validateBtn, h('div', { class: 'flex-1' }, sendBtn)))),
      h('div', { class: 'field full' }, h('label', null, 'Headers'),
        h('div', { class: 'headers-list' }, headersList, addHeaderBtn)),
      resultSlot));

    // Make Send fill the remaining width of its cell like the mockup's full-width button.
    sendBtn.classList.add('full-width');
    addHeaderRow('', '');

    /* ---------- event types ---------- */
    function typesForFormat() {
      return eventTypes.filter(function (t) { return t.format === format; });
    }
    function currentType() {
      const name = typeSelect.value;
      return typesForFormat().filter(function (t) { return t.name === name; })[0] || null;
    }
    function renderTypeOptions(keep) {
      const list = typesForFormat();
      typeSelect.replaceChildren();
      if (!list.length) {
        typeSelect.appendChild(h('option', { value: '' }, 'No ' + format + ' event types'));
        typeSelect.disabled = true;
        return;
      }
      typeSelect.disabled = false;
      list.forEach(function (t) {
        typeSelect.appendChild(h('option', { value: t.name }, t.name + (t.name === 'PingEvent' ? ' (sample)' : '')));
      });
      if (keep && list.some(function (t) { return t.name === keep; })) typeSelect.value = keep;
      applyDefaultTopic();
    }
    function applyDefaultTopic() {
      const t = currentType();
      if (t && t.defaultTopic && (topicInput.value.trim() === '' || topicIsAuto)) {
        topicInput.value = t.defaultTopic;
        topicIsAuto = true;
      }
      topicList.replaceChildren();
      typesForFormat().forEach(function (x) {
        if (x.defaultTopic) topicList.appendChild(h('option', { value: x.defaultTopic }));
      });
    }
    function setFormat(f) {
      if (f === format) return;
      format = f;
      formatButtons.forEach(function (b) { b.classList.toggle('active', b.getAttribute('data-format') === f); });
      renderTypeOptions(typeSelect.value);
    }
    function onTypeChange() { applyDefaultTopic(); }

    ApiClient.get('/api/events', { query: { offset: 0, limit: 500 } }).then(function (page) {
      if (!ctx.isActive()) return;
      eventTypes = (page && page.items) || [];
      renderTypeOptions();
    }).catch(function (err) {
      if (!ctx.isActive()) return;
      renderTypeOptions();
      App.showError(err, 'Could not load event types');
    });
    renderTypeOptions();

    /* ---------- payload helpers ---------- */
    function showFieldError(which, message) {
      const el = which === 'payload' ? payloadError : which === 'partition' ? partitionError : topicError;
      el.textContent = message;
      el.hidden = false;
      if (which === 'payload') payloadArea.classList.add('invalid');
    }
    function clearFieldError(which) {
      const el = which === 'payload' ? payloadError : which === 'partition' ? partitionError : topicError;
      el.hidden = true;
      el.textContent = '';
      if (which === 'payload') payloadArea.classList.remove('invalid');
    }

    function formatJson() {
      try {
        payloadArea.value = JSON.stringify(JSON.parse(payloadArea.value), null, 2);
        clearFieldError('payload');
      } catch (e) {
        showFieldError('payload', 'Invalid JSON: ' + e.message);
      }
    }

    function loadExample() {
      const t = currentType();
      if (!t) { App.toast('Select an event type first', 'error'); return; }
      loadExampleBtn.disabled = true;
      ApiClient.get('/api/events/' + encodeURIComponent(t.name) + '/example', { query: { format: format } })
        .then(function (res) {
          if (!ctx.isActive()) return;
          payloadArea.value = pretty(res.payload);
          clearFieldError('payload');
          applyDefaultTopic();
        })
        .catch(function (err) { if (ctx.isActive()) resultSlot.replaceChildren(App.renderProblem(err)); })
        .finally(function () { loadExampleBtn.disabled = false; });
    }

    /* ---------- headers ---------- */
    function addHeaderRow(key, value) {
      const row = h('div', { class: 'header-row' },
        h('input', { class: 'input', placeholder: 'key', value: key, 'data-role': 'header-key' }),
        h('input', { class: 'input', placeholder: 'value', value: value, 'data-role': 'header-value' }),
        h('button', {
          type: 'button', class: 'btn-icon', 'aria-label': 'Remove header',
          onclick: function () { row.remove(); }
        }, '✕'));
      headersList.appendChild(row);
    }
    function collectHeaders() {
      const headers = {};
      let any = false;
      headersList.querySelectorAll('.header-row').forEach(function (row) {
        const k = row.querySelector('[data-role=header-key]').value.trim();
        if (!k) return;
        headers[k] = row.querySelector('[data-role=header-value]').value;
        any = true;
      });
      return any ? headers : null;
    }

    /* ---------- submit ---------- */
    function buildRequest() {
      let ok = true;
      const t = currentType();
      if (!t) { App.toast('Select an event type first', 'error'); ok = false; }

      let topic = topicInput.value.trim();
      if (!topic && t && t.defaultTopic) {
        topic = t.defaultTopic;
        topicInput.value = topic;
        topicIsAuto = true;
      }
      if (!topic) { showFieldError('topic', 'Topic is required'); ok = false; }

      let payload = null;
      const text = payloadArea.value.trim();
      if (!text) {
        showFieldError('payload', 'Payload is required (use Load Example to generate one)');
        ok = false;
      } else {
        try {
          payload = JSON.parse(text);
          if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
            showFieldError('payload', 'Payload must be a JSON object');
            ok = false;
          }
        } catch (e) {
          showFieldError('payload', 'Invalid JSON: ' + e.message);
          ok = false;
        }
      }

      let partition;
      const pText = partitionInput.value.trim();
      if (pText !== '') {
        const n = Number(pText);
        if (!Number.isInteger(n) || n < 0) { showFieldError('partition', 'Partition must be a whole number >= 0'); ok = false; }
        else partition = n;
      }
      if (!ok) return null;

      const body = { topic: topic, eventType: t.name, format: format, payload: payload };
      if (keyInput.value !== '') body.key = keyInput.value;
      if (partition !== undefined) body.partition = partition;
      const headers = collectHeaders();
      if (headers) body.headers = headers;
      return body;
    }

    function setBusy(value) {
      busy = value;
      validateBtn.disabled = value;
      sendBtn.disabled = value;
    }

    function submit(action) {
      if (busy) return;
      clearFieldError('payload'); clearFieldError('partition'); clearFieldError('topic');
      const body = buildRequest();
      if (!body) return;
      setBusy(true);
      resultSlot.replaceChildren();
      ApiClient.post('/api/producer/' + action, body)
        .then(function (res) {
          if (!ctx.isActive()) return;
          resultSlot.replaceChildren(action === 'send' ? renderSent(res) : renderValid(res));
        })
        .catch(function (err) {
          if (!ctx.isActive()) return;
          const panel = App.renderProblem(err);
          if (err && err.errorCode === 'INVALID_AVRO_PAYLOAD') panel.appendChild(avroTip());
          resultSlot.replaceChildren(panel);
        })
        .finally(function () { setBusy(false); });
    }

    // Avro's JSON encoding is strict; the raw detail is shown as-is above, this tip explains the usual causes.
    function avroTip() {
      return h('details', { class: 'tip', id: 'producer-avro-tip' },
        h('summary', null, 'Tip: Avro JSON encoding'),
        h('ul', null,
          h('li', null, 'Union (nullable) fields must wrap the value with its type name, e.g. ',
            h('code', null, '{"note": {"string": "x"}}'), ' instead of ', h('code', null, '"note": "x"'),
            '; use ', h('code', null, 'null'), ' for the null branch.'),
          h('li', null, 'Fields without a default value are required, and defaults are not applied automatically: ' +
            'include every field of the schema, even optional ones that have a default.'),
          h('li', null, 'Use "Load Example" to see the exact structure the schema expects.')));
    }

    function renderSent(res) {
      return h('div', { class: 'result-panel success' },
        h('strong', null, '✓ Sent successfully'),
        h('div', { class: 'result-grid' },
          h('span', null, 'Topic'), h('code', null, res.topic),
          h('span', null, 'Partition'), h('code', null, String(res.partition)),
          h('span', null, 'Offset'), h('code', null, String(res.offset)),
          h('span', null, 'Timestamp'), h('code', null, new Date(res.timestamp).toISOString())));
    }
    function renderValid(res) {
      return h('div', { class: 'result-panel success' },
        h('strong', null, '✓ Payload is valid (nothing was sent)'),
        h('div', { class: 'result-grid' },
          h('span', null, 'Topic'), h('code', null, res.topic),
          h('span', null, 'Event Type'), h('code', null, res.eventType),
          h('span', null, 'Format'), h('code', null, res.format)),
        res.normalizedPayload ? h('pre', { class: 'code-block' }, pretty(res.normalizedPayload)) : null);
    }
  }

  App.registerScreen('producer', { mount: mount, unmount: function () { } });
})();
