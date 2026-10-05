/* Canvas sơ đồ tổ chức: thêm ô + kéo tọa độ + nối mũi tên (lưu sd_so_do_arrows). */
(function (global) {
  let canvasMode = 'move';
  let canvasLocked = false;
  let shapeDraft = null; // { type: 'arrow'|'line'|'rect', x1, y1, x2, y2, pointerId }
  let chartState = null; // { nodes, arrows, loai, idDepart, isAdmin }
  let selectedArrowId = null;
  let selectedNodeId = null;
  let editingNodeId = null; // so_do_id đang sửa trong modal Thêm ô/Sửa ô; null = đang ở chế độ thêm mới
  let editingNoteId = null; // id ghi chú đang sửa inline trong bảng chú thích; null = không sửa
  let noteEditDraft = { color: '#1A1C1D', content: '' }; // nội dung tạm khi đang sửa ghi chú
  let drawStyle = { color: '#1A1C1D', dash: '' };
  try {
    const saved = JSON.parse(localStorage.getItem('mtcl_draw_style') || 'null');
    if (saved && typeof saved.color === 'string') drawStyle = { color: saved.color, dash: saved.dash || '' };
  } catch (_) { /* ignore */ }
  function saveDrawStyle() {
    try {
      localStorage.setItem('mtcl_draw_style', JSON.stringify(drawStyle));
    } catch (_) { /* ignore */ }
  }
  let drawRAF = null;
  // Gộp nhiều lần gọi drawLines() trong cùng 1 khung hình (kéo chuột bắn pointermove
  // rất nhiều lần/giây) để kéo/vẽ mượt hơn, tránh giật khi có nhiều khối/mũi tên.
  function scheduleDraw() {
    if (drawRAF) return;
    drawRAF = requestAnimationFrame(() => {
      drawRAF = null;
      drawLines();
    });
  }
  let gridVisible = false;
  try {
    gridVisible = localStorage.getItem('mtcl_grid_visible') === '1';
  } catch (_) { /* private mode / storage blocked — giữ mặc định tắt */ }
  let undoStack = [];
  let undoContextKey = null;
  const catalog = { employees: [], chuc_vu: [], vai_tro: [] };
  const picks = {
    emp: { id: null, other: false },
  };
  const checked = { cv: new Set(), vt: new Set() };

  const COMBOS = {
    emp: {
      searchId: 'dv-add-emp-search',
      hiddenId: 'dv-add-emp',
      listId: 'dv-add-emp-list',
      comboId: 'dv-emp-combo',
      items: () => catalog.employees,
      label: (it) => (it.code ? `${it.name} (${it.code})` : it.name),
      sub: (it) => it.code || '',
      otherLabel: 'Khác (nhập tên + mã NV)',
      onOther: () => {
        setCustomVisible('emp', true);
        document.getElementById('dv-add-emp-name')?.focus();
      },
      onSelect: () => {
        setCustomVisible('emp', false);
        document.getElementById('dv-add-emp-name').value = '';
        document.getElementById('dv-add-emp-code').value = '';
      },
      onClear: () => setCustomVisible('emp', false),
    },
  };

  function renderChecklist(key) {
    const list = document.getElementById(`dv-add-${key}-list`);
    if (!list) return;
    const items = key === 'vt' ? catalog.vai_tro : catalog.chuc_vu;
    const filterEl = document.getElementById(`dv-add-${key}-filter`);
    const q = stripVn(filterEl ? filterEl.value : '');
    const filtered = q ? items.filter((it) => stripVn(it.name).includes(q)) : items;
    const esc = global.mtclEsc;
    if (!filtered.length) {
      list.innerHTML = '<p class="dv-combo-empty">Không tìm thấy</p>';
      return;
    }
    list.innerHTML = filtered.map((it) => {
      const isChecked = checked[key].has(it.id) ? ' checked' : '';
      return `<label class="dv-check-item">
        <input type="checkbox" data-check-id="${it.id}"${isChecked}>
        <span>${esc(it.name)}</span>
      </label>`;
    }).join('');
  }

  function bindChecklist(key) {
    const filterEl = document.getElementById(`dv-add-${key}-filter`);
    const list = document.getElementById(`dv-add-${key}-list`);
    if (!filterEl || !list || filterEl.dataset.bound === '1') return;
    filterEl.dataset.bound = '1';
    filterEl.addEventListener('input', () => renderChecklist(key));
    list.addEventListener('change', (event) => {
      const cb = event.target.closest('input[type=checkbox]');
      if (!cb) return;
      const id = Number(cb.dataset.checkId);
      if (cb.checked) checked[key].add(id);
      else checked[key].delete(id);
    });
  }

  function mark(id, color, size) {
    return (
      `<marker id="${id}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="${size}" markerHeight="${size}" orient="auto">`
      + `<path d="M0,0 L10,5 L0,10 z" fill="${color}"/></marker>`
    );
  }

  function arrowDefs(arrows) {
    const marks = (arrows || [])
      .filter((a) => a.shape_type === 'arrow' || !a.shape_type)
      .map((a) => mark(`dv-ah-${a.id}`, a.color || '#1A1C1D', 7))
      .join('');
    const extra = mark('dv-ah-draft', drawStyle.color, 7)
      + mark('dv-ah-legacy-cap', '#1A1C1D', 7)
      + mark('dv-ah-legacy-bao', '#1a73e8', 7)
      + mark('dv-ah-legacy-hotro', '#e67e22', 7);
    return `<defs>${marks}${extra}</defs>`;
  }

  function strokePath(d, color, markerEnd, width, dash) {
    const m = markerEnd ? ` marker-end="url(#${markerEnd})"` : '';
    const dashAttr = dash ? ` stroke-dasharray="${dash}"` : '';
    return `<path d="${d}" fill="none" stroke="${color}" stroke-width="${width}" stroke-linejoin="round" stroke-linecap="round"${m}${dashAttr}/>`;
  }

  function elbow(x1, y1, x2, y2) {
    const f = (n) => n.toFixed(1);
    return `M${f(x1)},${f(y1)} L${f(x2)},${f(y2)}`;
  }

  function polylinePath(points) {
    const f = (n) => n.toFixed(1);
    return points.map((p, i) => `${i === 0 ? 'M' : 'L'}${f(p[0])},${f(p[1])}`).join(' ');
  }

  function midpoint(a, b) {
    return [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2];
  }

  function stripVn(text) {
    return String(text || '')
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .toLowerCase();
  }

  function cardHtml(node) {
    const esc = global.mtclEsc;
    const code = node.code ? `<span class="dv-code">${esc(node.code)}</span>` : '';
    // Map label → link cho nội dung có sẵn
    const links = node.links || [];
    const linkMap = {};
    const matched = new Set();
    links.forEach((l) => { if (l.label) linkMap[l.label] = l; });

    function wrapLink(text, cssClass) {
      const lnk = linkMap[text];
      if (lnk) {
        matched.add(lnk.id);
        return `<a class="${cssClass} dv-has-link" href="${esc(lnk.url)}" target="_blank" rel="noopener" title="${esc(lnk.url)}">${esc(text)}<span class="material-symbols-outlined dv-link-icon">open_in_new</span></a>`;
      }
      return `<span class="${cssClass}">${esc(text)}</span>`;
    }

    // Tên người — check link trên tên
    const nameLnk = linkMap[node.name];
    let nameHtml;
    if (nameLnk) {
      matched.add(nameLnk.id);
      nameHtml = `<a class="dv-pos-person dv-has-link" href="${esc(nameLnk.url)}" target="_blank" rel="noopener" title="${esc(nameLnk.url)}">${esc(node.name)}${code}<span class="material-symbols-outlined dv-link-icon">open_in_new</span></a>`;
    } else {
      nameHtml = `<div class="dv-pos-person">${esc(node.name)}${code}</div>`;
    }

    const roles = node.roles && node.roles.length ? node.roles : [{ chuc_vu: '', vai_tro: '' }];
    const roleBlocks = roles.map((role) => {
      const cvText = role.chuc_vu || '';
      const cvHtml = cvText ? `<p class="dv-role">${wrapLink(cvText, 'dv-role-text')}</p>` : '<p class="dv-role"></p>';
      const vtHtml = role.vai_tro ? `<p class="dv-vai-tro">${wrapLink(role.vai_tro, 'dv-vt-text')}</p>` : '';
      return `<div class="dv-role-block">${cvHtml}${vtHtml}</div>`;
    }).join('');
    const tags = node.vai_tro_tags || [];
    const tagsHtml = tags.length
      ? `<div class="dv-vai-tro-tags">${tags.map((t) => wrapLink(t.vai_tro, 'dv-vai-tro-chip')).join('')}</div>`
      : '';
    // Link không khớp nội dung nào → hiển thị dòng riêng
    const unmatched = links.filter((l) => !matched.has(l.id));
    const unmatchedHtml = unmatched.length
      ? `<div class="dv-node-links">${unmatched.map((l) => {
          const display = esc(l.label || l.url).slice(0, 35);
          return `<a class="dv-node-link" href="${esc(l.url)}" target="_blank" rel="noopener" title="${esc(l.url)}"><span class="material-symbols-outlined">open_in_new</span><span>${display}</span></a>`;
        }).join('')}</div>`
      : '';
    const zIndex = 2 + (Number(node.z_index) || 0);
    return `<div class="dv-pos" data-node-id="${node.id}" style="left:${Number(node.x) || 0}px;top:${Number(node.y) || 0}px;z-index:${zIndex}" title="Double-click để sửa ô">
      <div class="dv-pos-head">
        <div class="dv-pos-people">${nameHtml}</div>
      </div>
      ${roleBlocks}
      ${tagsHtml}
      ${unmatchedHtml}
    </div>`;
  }

  const DRAW_COLORS = ['#1A1C1D', '#1a73e8', '#e67e22', '#1b6d24', '#d32f2f', '#7b1fa2'];

  function currentStyleTarget() {
    if (selectedArrowId && !String(selectedArrowId).startsWith('temp-')) {
      const arrow = ((chartState && chartState.arrows) || []).find((a) => String(a.id) === String(selectedArrowId));
      if (arrow) return { color: arrow.color || '#1A1C1D', dash: arrow.dash || '', arrow };
    }
    return { color: drawStyle.color, dash: drawStyle.dash, arrow: null };
  }

  function stylePickerHtml() {
    const target = currentStyleTarget();
    if (!target.arrow && !['arrow', 'line', 'rect'].includes(canvasMode)) return '';
    const swatches = DRAW_COLORS.map((c) => `<button type="button" class="dv-color-swatch${c === target.color ? ' active' : ''}" data-color="${c}" style="background:${c}" title="${c}"></button>`).join('');
    const label = target.arrow ? 'Sửa màu/kiểu mũi tên đang chọn' : 'Màu/kiểu khi vẽ mới';
    return `<div class="dv-style-picker" id="dv-style-picker">
      <span class="dv-style-label">${label}</span>
      ${swatches}
      <input type="color" id="dv-color-custom" class="dv-color-custom" value="${target.color}" title="Chọn màu khác">
      <button type="button" class="dv-dash-btn${target.dash ? '' : ' active'}" data-dash="">Nét liền</button>
      <button type="button" class="dv-dash-btn${target.dash ? ' active' : ''}" data-dash="6 5">Nét đứt</button>
    </div>`;
  }

  let lastPickerSig = null;
  function renderStylePicker() {
    const slot = document.getElementById('dv-style-picker-slot');
    if (!slot) return;
    const html = stylePickerHtml();
    if (html === lastPickerSig) return;
    lastPickerSig = html;
    slot.innerHTML = html;
  }

  // Ghi chú hiển thị dạng bảng chú thích cố định (như thanh công cụ) — không phải hình vẽ trên canvas.
  // Form sửa ghi chú render theo STATE (editingNoteId/noteEditDraft), không thao tác DOM 1 lần —
  // vì panel bị render lại thường xuyên (vd. bấm ra ngoài bỏ chọn khối/mũi tên cũng gọi drawLines()),
  // nếu form sửa chỉ là 1 lần innerHTML thủ công thì sẽ bị ghi đè mất ngay khi có render lại.
  function renderLegendPanel() {
    const panel = document.getElementById('dv-legend-panel');
    if (!panel || !chartState) return;
    const notes = (chartState.arrows || []).filter((a) => a.shape_type === 'note');
    if (!notes.length) {
      panel.hidden = true;
      panel.innerHTML = '';
      editingNoteId = null;
      return;
    }
    const esc = global.mtclEsc;
    const isAdmin = !!chartState.isAdmin;
    panel.innerHTML = notes.map((n) => {
      if (isAdmin && String(n.id) === String(editingNoteId)) {
        const swatches = DRAW_COLORS.map((c) =>
          `<button type="button" class="dv-color-swatch${c === noteEditDraft.color ? ' active' : ''}" data-edit-color="${c}" style="background:${c}"></button>`
        ).join('');
        return `<div class="dv-legend-row dv-legend-row-editing" data-legend-note-id="${n.id}">
          <div class="dv-legend-edit-form">
            <div class="dv-legend-edit-colors">${swatches}</div>
            <input type="text" class="dv-add-input dv-legend-edit-input" data-edit-input="${n.id}" value="${esc(noteEditDraft.content)}" placeholder="Nội dung ghi chú">
            <div class="dv-legend-edit-actions">
              <button type="button" class="dv-tool active" data-edit-save="${n.id}">
                <span class="material-symbols-outlined text-[14px]">check</span>Lưu
              </button>
              <button type="button" class="dv-tool" data-edit-cancel="${n.id}">Hủy</button>
            </div>
          </div>
        </div>`;
      }
      const adminBtns = isAdmin
        ? `<button type="button" class="dv-legend-remove" data-note-edit="${n.id}" title="Sửa ghi chú" aria-label="Sửa ghi chú">
            <span class="material-symbols-outlined text-[14px]">edit</span>
          </button>
          <button type="button" class="dv-legend-remove" data-note-id="${n.id}" title="Xóa ghi chú" aria-label="Xóa ghi chú">
            <span class="material-symbols-outlined text-[14px]">close</span>
          </button>`
        : '';
      return `<div class="dv-legend-row" data-legend-note-id="${n.id}">
        <span class="dv-legend-line" style="background:${n.color || '#1A1C1D'}"></span>
        <span class="dv-legend-text">${esc(n.content || '')}</span>
        ${adminBtns}
      </div>`;
    }).join('');
    panel.hidden = false;
    if (editingNoteId != null) {
      const input = panel.querySelector(`input[data-edit-input="${editingNoteId}"]`);
      if (input && document.activeElement !== input) {
        input.focus();
        input.setSelectionRange(input.value.length, input.value.length);
      }
    }
  }

  function bindLegendPanel() {
    const panel = document.getElementById('dv-legend-panel');
    if (!panel || panel.dataset.bound === '1') return;
    panel.dataset.bound = '1';

    // Gõ nội dung trong form sửa — lưu vào state để render lại (vd. khi đổi màu) không mất chữ đã gõ.
    panel.addEventListener('input', (event) => {
      const input = event.target.closest('.dv-legend-edit-input');
      if (!input) return;
      noteEditDraft.content = input.value;
    });

    panel.addEventListener('click', async (event) => {
      // Sửa ghi chú — mở form sửa (theo state, không phải thao tác DOM 1 lần)
      const editBtn = event.target.closest('[data-note-edit]');
      if (editBtn && chartState) {
        const note = (chartState.arrows || []).find((a) => String(a.id) === String(editBtn.dataset.noteEdit));
        if (note) {
          editingNoteId = editBtn.dataset.noteEdit;
          noteEditDraft = { color: note.color || '#1A1C1D', content: note.content || '' };
          renderLegendPanel();
        }
        return;
      }

      // Chọn màu trong form sửa
      const colorBtn = event.target.closest('[data-edit-color]');
      if (colorBtn) {
        noteEditDraft.color = colorBtn.dataset.editColor;
        renderLegendPanel();
        return;
      }

      // Hủy sửa
      const cancelBtn = event.target.closest('[data-edit-cancel]');
      if (cancelBtn) {
        editingNoteId = null;
        renderLegendPanel();
        return;
      }

      // Lưu sửa
      const saveBtn = event.target.closest('[data-edit-save]');
      if (saveBtn && chartState) {
        const noteId = saveBtn.dataset.editSave;
        const content = noteEditDraft.content.trim();
        if (!content) { alert('Nhập nội dung ghi chú'); return; }
        const color = noteEditDraft.color || '#1A1C1D';
        try {
          const response = await fetch('/api/mtcl/so-do/notes', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              id_depart: chartState.idDepart,
              loai_so_do: chartState.loai,
              note_id: Number(noteId),
              content,
              color,
            }),
          });
          const data = await response.json().catch(() => ({}));
          if (!response.ok) {
            alert(data.detail || 'Không lưu được ghi chú');
            return;
          }
          const note = (chartState.arrows || []).find((a) => String(a.id) === String(noteId));
          if (note) {
            note.content = content;
            note.color = color;
          }
          editingNoteId = null;
          drawLines();
        } catch (_) {
          alert('Không kết nối được máy chủ');
        }
        return;
      }

      // Xóa ghi chú
      const btn = event.target.closest('[data-note-id]');
      if (!btn || !chartState) return;
      const noteId = btn.dataset.noteId;
      if (!confirm('Xóa ghi chú này?')) return;
      try {
        const response = await fetch(
          `/api/mtcl/so-do/arrows/delete?id_depart=${chartState.idDepart}&loai_so_do=${chartState.loai}&arrow_id=${noteId}`,
          { method: 'POST' }
        );
        if (!response.ok) {
          const data = await response.json().catch(() => ({}));
          alert(data.detail || 'Không xóa được ghi chú');
          return;
        }
        chartState.arrows = (chartState.arrows || []).filter((a) => String(a.id) !== String(noteId));
        if (String(editingNoteId) === String(noteId)) editingNoteId = null;
        drawLines();
      } catch (_) {
        alert('Không kết nối được máy chủ');
      }
    });
  }

  function applyStyleChoice(color, dash) {
    const target = currentStyleTarget();
    if (target.arrow) {
      if (color != null) target.arrow.color = color;
      if (dash != null) target.arrow.dash = dash;
      saveArrowCoords([target.arrow.id]);
      lastPickerSig = null;
      drawLines();
      return;
    }
    if (color != null) drawStyle.color = color;
    if (dash != null) drawStyle.dash = dash;
    saveDrawStyle();
    lastPickerSig = null;
    renderStylePicker();
  }

  function bindStylePicker() {
    const wrap = document.getElementById('dv-style-picker-slot');
    if (!wrap || wrap.dataset.bound === '1') return;
    wrap.dataset.bound = '1';
    wrap.addEventListener('click', (event) => {
      const swatch = event.target.closest('.dv-color-swatch');
      if (swatch) {
        applyStyleChoice(swatch.dataset.color, null);
        return;
      }
      const dashBtn = event.target.closest('.dv-dash-btn');
      if (dashBtn) {
        applyStyleChoice(null, dashBtn.dataset.dash || '');
      }
    });
    wrap.addEventListener('change', (event) => {
      const input = event.target.closest('#dv-color-custom');
      if (!input) return;
      applyStyleChoice(input.value, null);
    });
  }

  function toolbarHtml(isAdmin) {
    const tools = [
      ['move', 'drag_pan', 'Kéo ô'],
      ['arrow', 'north_east', 'Vẽ mũi tên'],
      ['line', 'horizontal_rule', 'Vẽ đường thẳng'],
      ['rect', 'crop_square', 'Vẽ ô vuông'],
    ];
    const btns = tools.map(([mode, icon, label]) => {
      const disabled = !isAdmin && mode !== 'move' ? ' disabled' : '';
      const active = mode === canvasMode ? ' active' : '';
      return `<button type="button" class="dv-tool${active}" data-mode="${mode}"${disabled}>
        <span class="material-symbols-outlined text-[16px]">${icon}</span>${label}
      </button>`;
    }).join('');
    const adminBtns = isAdmin
      ? `<button type="button" class="dv-tool" data-action="add-node">
          <span class="material-symbols-outlined text-[16px]">person_add</span>Thêm ô
        </button>
        <button type="button" class="dv-tool" data-action="add-note">
          <span class="material-symbols-outlined text-[16px]">sticky_note_2</span>Thêm ghi chú
        </button>`
      : '';
    const alignBtn = isAdmin
      ? `<button type="button" class="dv-tool" data-action="auto-align" title="Tự động căn chỉnh các khối cho đều">
          <span class="material-symbols-outlined text-[16px]">auto_fix_high</span>Căn chỉnh
        </button>`
      : '';
    const undoBtn = isAdmin
      ? `<button type="button" class="dv-tool" id="dv-undo-btn" data-action="undo" title="Hoàn tác (Ctrl+Z)" disabled>
          <span class="material-symbols-outlined text-[16px]">undo</span>Hoàn tác
        </button>`
      : '';
    const zBtns = isAdmin
      ? `<button type="button" class="dv-tool" id="dv-z-front" data-action="z-front" title="Đưa lên trên cùng" disabled>
          <span class="material-symbols-outlined text-[16px]">vertical_align_top</span>
        </button>
        <button type="button" class="dv-tool" id="dv-z-forward" data-action="z-forward" title="Đưa lên 1 bậc" disabled>
          <span class="material-symbols-outlined text-[16px]">flip_to_front</span>
        </button>
        <button type="button" class="dv-tool" id="dv-z-backward" data-action="z-backward" title="Đưa xuống 1 bậc" disabled>
          <span class="material-symbols-outlined text-[16px]">flip_to_back</span>
        </button>
        <button type="button" class="dv-tool" id="dv-z-back" data-action="z-back" title="Đưa xuống dưới cùng" disabled>
          <span class="material-symbols-outlined text-[16px]">vertical_align_bottom</span>
        </button>`
      : '';
    return `<div class="dv-toolbar" id="dv-toolbar">
      ${adminBtns}${btns}${zBtns}${alignBtn}${undoBtn}
      <button type="button" class="dv-tool${gridVisible ? ' active' : ''}" id="dv-grid-btn" data-action="toggle-grid" title="Bật/tắt lưới nền">
        <span class="material-symbols-outlined text-[16px]">grid_4x4</span>Lưới
      </button>
      <button type="button" class="dv-tool${canvasLocked ? ' active' : ''}" id="dv-lock-btn" data-action="toggle-lock" title="Khóa/mở khóa sơ đồ — khi khóa không thể di chuyển hay chỉnh sửa">
        <span class="material-symbols-outlined text-[16px]">${canvasLocked ? 'lock' : 'lock_open'}</span>${canvasLocked ? 'Đã khóa' : 'Khóa'}
      </button>
      <span class="dv-tool-hint" id="dv-tool-hint">${canvasLocked ? 'Sơ đồ đang bị khóa — nhấn Khóa để mở' : 'Thêm ô → kéo vị trí → nối mũi tên'}</span>
      <div id="dv-style-picker-slot">${stylePickerHtml()}</div>
    </div>`;
  }

  function addNodeModalHtml() {
    return `<div id="dv-add-modal" class="dv-add-modal" hidden>
      <div class="dv-add-card">
        <h4 id="dv-add-modal-title">Thêm ô vào sơ đồ</h4>
        <label>Nhân viên</label>
        <div class="dv-combo" id="dv-emp-combo">
          <input type="hidden" id="dv-add-emp" value="">
          <input type="text" id="dv-add-emp-search" class="dv-add-input" placeholder="Gõ tên hoặc mã NV để tìm..." autocomplete="off">
          <ul id="dv-add-emp-list" class="dv-combo-list" hidden></ul>
        </div>
        <div id="dv-add-emp-custom" class="dv-add-row" hidden>
          <div>
            <label>Họ tên mới</label>
            <input id="dv-add-emp-name" class="dv-add-input" placeholder="Họ tên">
          </div>
          <div>
            <label>Mã NV</label>
            <input id="dv-add-emp-code" class="dv-add-input" placeholder="VD: C0741">
          </div>
        </div>
        <label>Chức vụ (tick chọn 1 hoặc nhiều)</label>
        <input type="text" id="dv-add-cv-filter" class="dv-add-input" placeholder="Gõ để lọc danh sách..." autocomplete="off">
        <div class="dv-checklist" id="dv-add-cv-list"></div>
        <input id="dv-add-cv-new" class="dv-add-input" placeholder="Chức vụ mới (cách nhau bằng dấu phẩy nếu nhiều)">

        <label>Vai trò (không bắt buộc, tick chọn 0 hoặc nhiều)</label>
        <input type="text" id="dv-add-vt-filter" class="dv-add-input" placeholder="Gõ để lọc danh sách..." autocomplete="off">
        <div class="dv-checklist" id="dv-add-vt-list"></div>
        <input id="dv-add-vt-new" class="dv-add-input" placeholder="Vai trò mới (cách nhau bằng dấu phẩy nếu nhiều)">

        <p id="dv-add-err" class="dv-add-err"></p>
        <div class="dv-add-actions">
          <button type="button" class="dv-tool active" id="dv-add-save"><span id="dv-add-save-label">Lưu ô</span></button>
          <button type="button" class="dv-tool" id="dv-add-cancel">Hủy</button>
        </div>
      </div>
    </div>`;
  }

  let noteRowIndices = [];
  let nextNoteRowIdx = 0;
  const noteRowColors = {};

  function noteRowHtml(idx) {
    const color = noteRowColors[idx] || DRAW_COLORS[0];
    const swatches = DRAW_COLORS.map((c) => `<button type="button" class="dv-color-swatch${c === color ? ' active' : ''}" data-note-idx="${idx}" data-color="${c}" style="background:${c}" title="${c}"></button>`).join('');
    const removeBtn = idx > 0
      ? `<button type="button" class="dv-role-row-remove" data-remove-note="${idx}" aria-label="Xóa dòng ghi chú này">
          <span class="material-symbols-outlined text-[16px]">close</span>
        </button>`
      : '';
    return `<div class="dv-note-row" data-note-row="${idx}">
      <div class="dv-role-row-head">
        <label>Ghi chú</label>
        ${removeBtn}
      </div>
      <div class="dv-note-colors">${swatches}</div>
      <textarea class="dv-add-input" data-note-content="${idx}" rows="3" placeholder="Nội dung ghi chú..."></textarea>
    </div>`;
  }

  function addNoteRow() {
    const container = document.getElementById('dv-note-rows');
    if (!container) return;
    const idx = nextNoteRowIdx++;
    noteRowColors[idx] = DRAW_COLORS[0];
    container.insertAdjacentHTML('beforeend', noteRowHtml(idx));
    noteRowIndices.push(idx);
  }

  function removeNoteRow(idx) {
    const row = document.querySelector(`.dv-note-row[data-note-row="${idx}"]`);
    if (row) row.remove();
    delete noteRowColors[idx];
    noteRowIndices = noteRowIndices.filter((i) => i !== idx);
  }

  function noteModalHtml() {
    return `<div id="dv-note-modal" class="dv-add-modal" hidden>
      <div class="dv-add-card">
        <h4>Thêm ghi chú</h4>
        <div id="dv-note-rows"></div>
        <button type="button" id="dv-note-add-row" class="dv-role-add-btn">
          <span class="material-symbols-outlined text-[16px]">add</span>Thêm dòng ghi chú
        </button>
        <p id="dv-note-err" class="dv-add-err"></p>
        <div class="dv-add-actions">
          <button type="button" class="dv-tool active" id="dv-note-save">Lưu ghi chú</button>
          <button type="button" class="dv-tool" id="dv-note-cancel">Hủy</button>
        </div>
      </div>
    </div>`;
  }

  function openAddNoteModal() {
    if (!chartState) return;
    const modal = document.getElementById('dv-note-modal');
    const err = document.getElementById('dv-note-err');
    if (!modal) return;
    if (err) err.textContent = '';
    const container = document.getElementById('dv-note-rows');
    if (container) container.innerHTML = '';
    noteRowIndices = [];
    nextNoteRowIdx = 0;
    Object.keys(noteRowColors).forEach((k) => delete noteRowColors[k]);
    addNoteRow();
    modal.hidden = false;
  }

  async function submitNoteForm() {
    if (!chartState) return;
    const err = document.getElementById('dv-note-err');
    const notes = [];
    noteRowIndices.forEach((idx) => {
      const textarea = document.querySelector(`textarea[data-note-content="${idx}"]`);
      const content = textarea ? textarea.value.trim() : '';
      if (content) notes.push({ content, color: noteRowColors[idx] || DRAW_COLORS[0] });
    });
    if (!notes.length) {
      if (err) err.textContent = 'Nhập nội dung cho ít nhất 1 ghi chú';
      return;
    }
    const payload = {
      id_depart: chartState.idDepart,
      loai_so_do: chartState.loai,
      notes,
    };
    try {
      const response = await fetch('/api/mtcl/so-do/notes', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        if (err) err.textContent = typeof data.detail === 'string' ? data.detail : 'Không lưu được ghi chú';
        return;
      }
      (data.arrows || []).forEach((a) => chartState.arrows.push(a));
      document.getElementById('dv-note-modal').hidden = true;
      drawLines();
    } catch (_) {
      if (err) err.textContent = 'Không kết nối được máy chủ';
    }
  }

  function setCustomVisible(key, show) {
    const box = document.getElementById(`dv-add-${key}-custom`);
    if (box) box.hidden = !show;
  }

  function selectComboItem(key, item) {
    const cfg = COMBOS[key];
    const search = document.getElementById(cfg.searchId);
    const hidden = document.getElementById(cfg.hiddenId);
    const list = document.getElementById(cfg.listId);
    if (item === 'other') {
      picks[key] = { id: null, other: true };
      if (hidden) hidden.value = '';
      if (search) search.value = cfg.otherLabel;
      cfg.onOther();
    } else if (item) {
      picks[key] = { id: item.id, other: false };
      if (hidden) hidden.value = String(item.id);
      if (search) search.value = cfg.label(item);
      cfg.onSelect();
    } else {
      picks[key] = { id: null, other: false };
      if (hidden) hidden.value = '';
      cfg.onClear();
    }
    if (list) list.hidden = true;
  }

  function filterComboItems(key, query) {
    const cfg = COMBOS[key];
    const items = cfg.items();
    const q = stripVn(query);
    if (!q || q === stripVn(cfg.otherLabel)) return items.slice(0, 100);
    return items
      .filter((it) => stripVn(`${cfg.label(it)} ${cfg.sub(it)}`).includes(q))
      .slice(0, 100);
  }

  function renderComboSuggestions(key, query) {
    const cfg = COMBOS[key];
    const list = document.getElementById(cfg.listId);
    if (!list) return;
    const items = filterComboItems(key, query);
    const esc = global.mtclEsc;
    const rows = items.map((it) => {
      const subText = cfg.sub(it);
      const sub = subText ? `<span class="dv-combo-sub">${esc(subText)}</span>` : '';
      return `<li><button type="button" class="dv-combo-item" data-item-id="${it.id}">
        <span>${esc(cfg.label(it))}</span>${sub}
      </button></li>`;
    }).join('');
    const empty = items.length
      ? ''
      : '<li class="dv-combo-empty">Không tìm thấy — chọn Khác để nhập mới</li>';
    list.innerHTML = `${rows}${empty}
      <li><button type="button" class="dv-combo-item is-other" data-item-id="other">${esc(cfg.otherLabel)}</button></li>`;
    list.hidden = false;
  }

  function bindCombo(key) {
    const cfg = COMBOS[key];
    const search = document.getElementById(cfg.searchId);
    const list = document.getElementById(cfg.listId);
    if (!search || !list || search.dataset.bound === '1') return;
    search.dataset.bound = '1';

    search.addEventListener('focus', () => {
      const q = picks[key].id || picks[key].other ? '' : search.value;
      renderComboSuggestions(key, q);
    });
    search.addEventListener('click', () => {
      renderComboSuggestions(key, picks[key].id || picks[key].other ? '' : search.value);
    });
    search.addEventListener('input', () => {
      picks[key] = { id: null, other: false };
      document.getElementById(cfg.hiddenId).value = '';
      cfg.onClear();
      renderComboSuggestions(key, search.value);
    });
    search.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        list.hidden = true;
        return;
      }
      const buttons = [...list.querySelectorAll('.dv-combo-item')];
      if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        event.preventDefault();
        if (!buttons.length) return;
        let idx = buttons.findIndex((b) => b.classList.contains('is-active'));
        idx = event.key === 'ArrowDown' ? idx + 1 : idx - 1;
        if (idx < 0) idx = buttons.length - 1;
        if (idx >= buttons.length) idx = 0;
        buttons.forEach((b) => b.classList.remove('is-active'));
        buttons[idx].classList.add('is-active');
        buttons[idx].scrollIntoView({ block: 'nearest' });
        return;
      }
      if (event.key === 'Enter') {
        const active = list.querySelector('.dv-combo-item.is-active') || list.querySelector('.dv-combo-item');
        if (active && !list.hidden) {
          event.preventDefault();
          active.click();
        }
      }
    });

    list.addEventListener('mousedown', (event) => {
      event.preventDefault();
    });
    list.addEventListener('click', (event) => {
      const btn = event.target.closest('.dv-combo-item');
      if (!btn) return;
      const raw = btn.dataset.itemId;
      if (raw === 'other') {
        selectComboItem(key, 'other');
        return;
      }
      const item = cfg.items().find((it) => String(it.id) === String(raw));
      if (item) selectComboItem(key, item);
    });

    document.addEventListener('click', (event) => {
      const combo = document.getElementById(cfg.comboId);
      if (!combo || combo.contains(event.target)) return;
      if (list) list.hidden = true;
    });
  }

  function hintForMode(mode) {
    if (mode === 'move') return 'Kéo khối để di chuyển • double-click khối để sửa, bấm chọn rồi nhấn Delete để xóa khối • bấm mũi tên để chọn, kéo tâm đoạn để bẻ gập khúc, double-click điểm gập để xóa, Delete để xóa cả mũi tên';
    if (mode === 'arrow') return 'Giữ chuột và kéo để vẽ 1 mũi tên ở bất kỳ đâu trên canvas, không cần nối từ khối nào';
    if (mode === 'line') return 'Giữ chuột và kéo để vẽ 1 đoạn thẳng ở bất kỳ đâu trên canvas';
    if (mode === 'rect') return 'Giữ chuột và kéo để vẽ 1 ô vuông/chữ nhật ở bất kỳ đâu trên canvas';
    return '';
  }

  function setMode(mode) {
    canvasMode = mode;
    shapeDraft = null;
    selectedArrowId = null;
    selectedNodeId = null;
    document.querySelectorAll('#dv-toolbar .dv-tool').forEach((btn) => {
      btn.classList.toggle('active', btn.dataset.mode === mode);
    });
    const hint = document.getElementById('dv-tool-hint');
    if (hint) hint.textContent = hintForMode(mode);
    document.querySelectorAll('.dv-pos').forEach((el) => {
      el.classList.remove('is-node-selected');
      if (['arrow', 'line', 'rect'].includes(mode)) el.style.cursor = 'crosshair';
      else if (mode === 'move') el.style.cursor = 'grab';
      else el.style.cursor = '';
    });
    const pan = document.getElementById('dv-pan');
    if (pan) pan.style.cursor = mode === 'move' ? 'grab' : '';
    drawLines();
  }

  function nodeCenter(el, origin) {
    const box = el.getBoundingClientRect();
    return {
      top: box.top - origin.top,
      bottom: box.bottom - origin.top,
      left: box.left - origin.left,
      right: box.right - origin.left,
      cx: box.left - origin.left + box.width / 2,
      cy: box.top - origin.top + box.height / 2,
    };
  }

  function attachPoint(fromBox, toBox) {
    const dx = toBox.cx - fromBox.cx;
    const dy = toBox.cy - fromBox.cy;
    if (Math.abs(dx) > Math.abs(dy)) {
      return dx > 0 ? { x: fromBox.right, y: fromBox.cy } : { x: fromBox.left, y: fromBox.cy };
    }
    return dy > 0 ? { x: fromBox.cx, y: fromBox.bottom } : { x: fromBox.cx, y: fromBox.top };
  }

  function arrowStyle(arrow) {
    const color = arrow.color || '#1A1C1D';
    const dash = arrow.dash || null;
    const marker = arrow.shape_type === 'arrow' || !arrow.shape_type ? `dv-ah-${arrow.id}` : null;
    return [color, marker, 2.2, dash];
  }

  function linkEndpoints(fromId, toId) {
    const chart = document.querySelector('#dv-body .dv-chart');
    if (!chart) return null;
    const fromEl = chart.querySelector(`.dv-pos[data-node-id="${fromId}"]`);
    const toEl = chart.querySelector(`.dv-pos[data-node-id="${toId}"]`);
    if (!fromEl || !toEl) return null;
    const origin = chart.getBoundingClientRect();
    const fromBox = nodeCenter(fromEl, origin);
    const toBox = nodeCenter(toEl, origin);
    const a = attachPoint(fromBox, toBox);
    const b = attachPoint(toBox, fromBox);
    return { x1: a.x, y1: a.y, x2: b.x, y2: b.y };
  }

  function ensureArrowCoords(arrow, fromBox, toBox) {
    const has = Number(arrow.x1) || Number(arrow.y1) || Number(arrow.x2) || Number(arrow.y2);
    if (has) return;
    if (!fromBox || !toBox) return;
    const a = attachPoint(fromBox, toBox);
    const b = attachPoint(toBox, fromBox);
    arrow.x1 = a.x;
    arrow.y1 = a.y;
    arrow.x2 = b.x;
    arrow.y2 = b.y;
  }

  function offsetArrowsForNode(nodeId, dx, dy) {
    if (!chartState || (!dx && !dy)) return;
    for (const arrow of chartState.arrows || []) {
      let moved = false;
      if (String(arrow.from_node_id) === String(nodeId)) {
        arrow.x1 = Number(arrow.x1 || 0) + dx;
        arrow.y1 = Number(arrow.y1 || 0) + dy;
        moved = true;
      }
      if (String(arrow.to_node_id) === String(nodeId)) {
        arrow.x2 = Number(arrow.x2 || 0) + dx;
        arrow.y2 = Number(arrow.y2 || 0) + dy;
        moved = true;
      }
      if (moved && arrow.points && arrow.points.length) {
        arrow.points = arrow.points.map((p) => [p[0] + dx, p[1] + dy]);
      }
    }
  }

  function drawLines() {
    const chart = document.querySelector('#dv-body .dv-chart');
    const svg = chart && chart.querySelector('.dv-lines');
    if (!chart || !svg || !chartState) return;
    const cards = [...chart.querySelectorAll('.dv-pos[data-node-id]')];
    const origin = chart.getBoundingClientRect();
    let maxR = 640;
    let maxB = 480;
    const boxes = new Map();
    for (const el of cards) {
      const box = nodeCenter(el, origin);
      boxes.set(el.dataset.nodeId, box);
      maxR = Math.max(maxR, box.right + 80);
      maxB = Math.max(maxB, box.bottom + 80);
    }
    for (const arrow of chartState.arrows || []) {
      maxR = Math.max(maxR, Number(arrow.x1 || 0) + 40, Number(arrow.x2 || 0) + 40);
      maxB = Math.max(maxB, Number(arrow.y1 || 0) + 40, Number(arrow.y2 || 0) + 40);
      for (const p of arrow.points || []) {
        maxR = Math.max(maxR, Number(p[0] || 0) + 40);
        maxB = Math.max(maxB, Number(p[1] || 0) + 40);
      }
    }
    chart.style.minWidth = `${Math.ceil(maxR)}px`;
    chart.style.minHeight = `${Math.ceil(maxB)}px`;
    svg.setAttribute('width', String(Math.ceil(maxR)));
    svg.setAttribute('height', String(Math.ceil(maxB)));
    svg.setAttribute('viewBox', `0 0 ${Math.ceil(maxR)} ${Math.ceil(maxB)}`);
    const isAdmin = !!chartState.isAdmin;
    const arrows = (chartState.arrows || [])
      .filter((a) => a.shape_type !== 'note')
      .slice()
      .sort((a, b) => (a.z_index || 0) - (b.z_index || 0));
    const parts = [arrowDefs(arrows)];

    if (arrows.length) {
      for (const arrow of arrows) {
        const fromBox = boxes.get(String(arrow.from_node_id));
        const toBox = boxes.get(String(arrow.to_node_id));
        ensureArrowCoords(arrow, fromBox, toBox);
        const x1 = Number(arrow.x1) || 0;
        const y1 = Number(arrow.y1) || 0;
        const x2 = Number(arrow.x2) || 0;
        const y2 = Number(arrow.y2) || 0;
        const [color, marker, width, dash] = arrowStyle(arrow);
        const isSelected = isAdmin && String(arrow.id) === String(selectedArrowId);
        const isRect = arrow.shape_type === 'rect';

        if (isRect) {
          const rx = Math.min(x1, x2);
          const ry = Math.min(y1, y2);
          const rw = Math.abs(x2 - x1);
          const rh = Math.abs(y2 - y1);
          if (isSelected) {
            parts.push(`<rect x="${(rx - 4).toFixed(1)}" y="${(ry - 4).toFixed(1)}" width="${(rw + 8).toFixed(1)}" height="${(rh + 8).toFixed(1)}" fill="none" stroke="rgba(0,90,156,0.35)" stroke-width="${width + 6}"/>`);
          }
          parts.push(`<rect x="${rx.toFixed(1)}" y="${ry.toFixed(1)}" width="${rw.toFixed(1)}" height="${rh.toFixed(1)}" fill="none" stroke="${color}" stroke-width="${width}"${dash ? ` stroke-dasharray="${dash}"` : ''}/>`);
          if (isAdmin) {
            parts.push(`<rect class="dv-arrow-hit" data-arrow-id="${arrow.id}" x="${rx.toFixed(1)}" y="${ry.toFixed(1)}" width="${rw.toFixed(1)}" height="${rh.toFixed(1)}" fill="transparent" stroke="transparent" stroke-width="16"/>`);
          }
          if (isSelected) {
            parts.push(`<circle class="dv-arrow-handle" data-arrow-id="${arrow.id}" data-end="1" cx="${x1.toFixed(1)}" cy="${y1.toFixed(1)}" r="8" fill="#fff" stroke="${color}" stroke-width="2.5"/>`);
            parts.push(`<circle class="dv-arrow-handle" data-arrow-id="${arrow.id}" data-end="2" cx="${x2.toFixed(1)}" cy="${y2.toFixed(1)}" r="8" fill="#fff" stroke="${color}" stroke-width="2.5"/>`);
          }
          continue;
        }

        const bendPoints = arrow.points || [];
        const fullPoints = [[x1, y1], ...bendPoints, [x2, y2]];
        const d = polylinePath(fullPoints);
        if (isSelected) {
          parts.push(strokePath(d, 'rgba(0,90,156,0.35)', null, width + 8, null));
        }
        parts.push(strokePath(d, color, marker, width, dash));
        if (isAdmin) {
          parts.push(
            `<path class="dv-arrow-hit" data-arrow-id="${arrow.id}" d="${d}" fill="none" stroke="transparent" stroke-width="16"/>`
          );
        }
        if (isSelected) {
          parts.push(
            `<circle class="dv-arrow-handle" data-arrow-id="${arrow.id}" data-end="1" cx="${x1.toFixed(1)}" cy="${y1.toFixed(1)}" r="8" fill="#fff" stroke="${color}" stroke-width="2.5"/>`
          );
          parts.push(
            `<circle class="dv-arrow-handle" data-arrow-id="${arrow.id}" data-end="2" cx="${x2.toFixed(1)}" cy="${y2.toFixed(1)}" r="8" fill="#fff" stroke="${color}" stroke-width="2.5"/>`
          );
          bendPoints.forEach((p, i) => {
            parts.push(
              `<circle class="dv-arrow-handle" data-arrow-id="${arrow.id}" data-end="pt-${i}" cx="${p[0].toFixed(1)}" cy="${p[1].toFixed(1)}" r="7" fill="#fff" stroke="${color}" stroke-width="2.5"/>`
            );
          });
          for (let i = 0; i < fullPoints.length - 1; i++) {
            const [mx, my] = midpoint(fullPoints[i], fullPoints[i + 1]);
            parts.push(
              `<circle class="dv-arrow-mid" data-arrow-id="${arrow.id}" data-seg="${i}" cx="${mx.toFixed(1)}" cy="${my.toFixed(1)}" r="5" fill="${color}" fill-opacity="0.55" stroke="#fff" stroke-width="1.5"/>`
            );
          }
        }
      }
    } else {
      const drawnHotro = new Set();
      for (const node of chartState.nodes) {
        const fromBox = boxes.get(String(node.id));
        if (!fromBox) continue;
        for (const [targetId, markerId, color] of [
          [node.cap_parent, 'dv-ah-legacy-cap', '#1A1C1D'],
          [node.reports_to, 'dv-ah-legacy-bao', '#1a73e8'],
        ]) {
          if (!targetId || !boxes.has(String(targetId))) continue;
          const toBox = boxes.get(String(targetId));
          const a = attachPoint(fromBox, toBox);
          const b = attachPoint(toBox, fromBox);
          parts.push(strokePath(elbow(a.x, a.y, b.x, b.y), color, markerId, 2.2, null));
        }
        for (const hid of node.ho_tro || []) {
          const key = [node.id, hid].sort().join('|');
          if (drawnHotro.has(key) || !boxes.has(String(hid))) continue;
          drawnHotro.add(key);
          const toBox = boxes.get(String(hid));
          const a = attachPoint(fromBox, toBox);
          const b = attachPoint(toBox, fromBox);
          parts.push(strokePath(elbow(a.x, a.y, b.x, b.y), '#e67e22', 'dv-ah-legacy-hotro', 2.2, '6 5'));
        }
      }
    }
    if (shapeDraft) {
      const color = drawStyle.color;
      if (shapeDraft.type === 'rect') {
        const rx = Math.min(shapeDraft.x1, shapeDraft.x2);
        const ry = Math.min(shapeDraft.y1, shapeDraft.y2);
        const rw = Math.abs(shapeDraft.x2 - shapeDraft.x1);
        const rh = Math.abs(shapeDraft.y2 - shapeDraft.y1);
        parts.push(`<rect x="${rx.toFixed(1)}" y="${ry.toFixed(1)}" width="${rw.toFixed(1)}" height="${rh.toFixed(1)}" fill="none" stroke="${color}" stroke-width="2.2"${drawStyle.dash ? ` stroke-dasharray="${drawStyle.dash}"` : ' stroke-dasharray="4 4"'}/>`);
      } else {
        const marker = shapeDraft.type === 'arrow' ? 'dv-ah-draft' : null;
        parts.push(strokePath(elbow(shapeDraft.x1, shapeDraft.y1, shapeDraft.x2, shapeDraft.y2), color, marker, 2.2, drawStyle.dash || '4 4'));
      }
    }
    svg.innerHTML = parts.join('');
    updateZButtons();
    renderStylePicker();
    renderLegendPanel();
  }

  async function saveArrowCoords(arrowIds) {
    if (!chartState || !chartState.isAdmin) return;
    const ids = new Set((arrowIds || []).map(String));
    const arrows = (chartState.arrows || [])
      .filter((a) => !ids.size || ids.has(String(a.id)))
      .map((a) => ({
        id: Number(a.id),
        x1: Number(a.x1) || 0,
        y1: Number(a.y1) || 0,
        x2: Number(a.x2) || 0,
        y2: Number(a.y2) || 0,
        points: (a.points || []).map((p) => [Number(p[0]) || 0, Number(p[1]) || 0]),
      }));
    if (!arrows.length) return;
    try {
      await fetch('/api/mtcl/so-do/arrows', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id_depart: chartState.idDepart,
          loai_so_do: chartState.loai,
          arrows,
        }),
      });
    } catch (_) { /* ignore */ }
  }

  async function savePosition(nodeId, x, y) {
    if (!chartState) return;
    const node = chartState.nodes.find((n) => String(n.id) === String(nodeId));
    if (node) {
      node.x = x;
      node.y = y;
    }
    try {
      await fetch('/api/mtcl/so-do/positions', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id_depart: chartState.idDepart,
          loai_so_do: chartState.loai,
          positions: [{ node_id: Number(nodeId), x, y }],
        }),
      });
    } catch (_) { /* ignore */ }
  }

  async function saveLink(fromId, toId, kind, coords, points, style) {
    if (!chartState) return;
    const useStyle = style || drawStyle;
    const payload = {
      id_depart: chartState.idDepart,
      loai_so_do: chartState.loai,
      from_node_id: Number(fromId),
      to_node_id: Number(toId || fromId),
      kind,
    };
    if (kind !== 'clear') {
      if (coords && coords.x1 != null && coords.y1 != null && coords.x2 != null && coords.y2 != null) {
        Object.assign(payload, {
          x1: Number(coords.x1),
          y1: Number(coords.y1),
          x2: Number(coords.x2),
          y2: Number(coords.y2),
        });
      } else {
        const pts = linkEndpoints(fromId, toId);
        if (pts) Object.assign(payload, pts);
      }
      if (points && points.length) payload.points = points;
      payload.color = useStyle.color || '';
      payload.dash = useStyle.dash || '';
    }

    // Vẽ ngay 1 mũi tên tạm trước khi chờ server — thao tác nhanh liên tiếp vẫn không mất
    // mũi tên vì không còn phụ thuộc độ trễ mạng để hiện lên.
    let tempArrow = null;
    if (kind !== 'clear' && payload.x1 != null && payload.y1 != null && payload.x2 != null && payload.y2 != null) {
      tempArrow = {
        id: `temp-${Date.now()}-${Math.random().toString(36).slice(2)}`,
        kind,
        shape_type: 'arrow',
        color: payload.color || '#1A1C1D',
        dash: payload.dash || '',
        z_index: 0,
        from_node_id: payload.from_node_id,
        to_node_id: payload.to_node_id,
        x1: payload.x1,
        y1: payload.y1,
        x2: payload.x2,
        y2: payload.y2,
        points: payload.points || [],
      };
      chartState.arrows = (chartState.arrows || []).filter(
        (a) => !(
          String(a.from_node_id) === String(payload.from_node_id)
          && String(a.to_node_id) === String(payload.to_node_id)
          && a.kind === kind
        )
      );
      chartState.arrows.push(tempArrow);
      drawLines();
    }

    let response;
    try {
      response = await fetch('/api/mtcl/so-do/link', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
    } catch (_) {
      if (tempArrow) {
        chartState.arrows = (chartState.arrows || []).filter((a) => a.id !== tempArrow.id);
        drawLines();
      }
      alert('Không kết nối được máy chủ');
      return;
    }
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      if (tempArrow) {
        chartState.arrows = (chartState.arrows || []).filter((a) => a.id !== tempArrow.id);
        drawLines();
      }
      alert(data.detail || 'Không lưu được nối');
      return;
    }
    // Cập nhật trực tiếp trên canvas từ dữ liệu server trả về, không load lại cả sơ đồ.
    const removedIds = new Set((data.removed_arrow_ids || []).map(String));
    chartState.arrows = (chartState.arrows || []).filter(
      (a) => !removedIds.has(String(a.id)) && (!tempArrow || a.id !== tempArrow.id)
    );
    if (data.arrow) chartState.arrows.push(data.arrow);
    drawLines();
  }

  async function saveShape(type, x1, y1, x2, y2, style) {
    if (!chartState) return;
    const useStyle = style || drawStyle;
    const tempId = `temp-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const tempShape = {
      id: tempId,
      kind: 'clear',
      shape_type: type,
      color: useStyle.color || '#1A1C1D',
      dash: useStyle.dash || '',
      z_index: 0,
      from_node_id: 0,
      to_node_id: 0,
      x1,
      y1,
      x2,
      y2,
      points: [],
    };
    chartState.arrows.push(tempShape);
    drawLines();
    try {
      const response = await fetch('/api/mtcl/so-do/shapes', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id_depart: chartState.idDepart,
          loai_so_do: chartState.loai,
          shape_type: type,
          x1,
          y1,
          x2,
          y2,
          color: useStyle.color || '#1A1C1D',
          dash: useStyle.dash || '',
        }),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        chartState.arrows = chartState.arrows.filter((a) => a.id !== tempId);
        drawLines();
        alert(data.detail || 'Không vẽ được hình');
        return;
      }
      chartState.arrows = chartState.arrows.filter((a) => a.id !== tempId);
      if (data.arrow) chartState.arrows.push(data.arrow);
      drawLines();
    } catch (_) {
      chartState.arrows = chartState.arrows.filter((a) => a.id !== tempId);
      drawLines();
      alert('Không kết nối được máy chủ');
    }
  }

  async function openAddNodeModal() {
    if (!chartState) return;
    const modal = document.getElementById('dv-add-modal');
    const err = document.getElementById('dv-add-err');
    if (!modal) return;
    if (err) err.textContent = '';
    editingNodeId = null;
    const title = document.getElementById('dv-add-modal-title');
    if (title) title.textContent = 'Thêm ô vào sơ đồ';
    const saveLabel = document.getElementById('dv-add-save-label');
    if (saveLabel) saveLabel.textContent = 'Lưu ô';
    const combo = document.getElementById('dv-emp-combo');
    if (combo) combo.hidden = false;

    bindCombo('emp');
    picks.emp = { id: null, other: false };
    const empCfg = COMBOS.emp;
    const empSearchEl = document.getElementById(empCfg.searchId);
    const empHidden = document.getElementById(empCfg.hiddenId);
    const empList = document.getElementById(empCfg.listId);
    if (empSearchEl) empSearchEl.value = '';
    if (empHidden) empHidden.value = '';
    if (empList) {
      empList.hidden = true;
      empList.innerHTML = '';
    }
    setCustomVisible('emp', false);

    // Reset danh sách chức vụ/vai trò đã tick + ô lọc + ô nhập mới.
    checked.cv.clear();
    checked.vt.clear();
    bindChecklist('cv');
    bindChecklist('vt');
    const cvFilter = document.getElementById('dv-add-cv-filter');
    const vtFilter = document.getElementById('dv-add-vt-filter');
    if (cvFilter) cvFilter.value = '';
    if (vtFilter) vtFilter.value = '';
    document.getElementById('dv-add-cv-new').value = '';
    document.getElementById('dv-add-vt-new').value = '';

    try {
      const response = await fetch(`/api/mtcl/so-do/catalog?id_depart=${chartState.idDepart}`);
      const data = await response.json();
      catalog.employees = data.employees || [];
      catalog.chuc_vu = data.chuc_vu || [];
      catalog.vai_tro = data.vai_tro || [];
    } catch (_) {
      catalog.employees = [];
      catalog.chuc_vu = [];
      catalog.vai_tro = [];
    }
    renderChecklist('cv');
    renderChecklist('vt');
    document.getElementById('dv-add-emp-name').value = '';
    document.getElementById('dv-add-emp-code').value = '';
    modal.hidden = false;
    const empSearch = document.getElementById('dv-add-emp-search');
    if (empSearch) {
      empSearch.focus();
      renderComboSuggestions('emp', '');
    }
  }

  // Nhấn đúp vào 1 khối: sửa toàn bộ nội dung (tên NV, chức vụ, vai trò) thay vì xóa —
  // xóa khối giờ làm qua: bấm chọn khối rồi nhấn phím Delete.
  async function openEditNodeModal(nodeId) {
    if (!chartState || !chartState.isAdmin) return;
    const node = (chartState.nodes || []).find((n) => String(n.id) === String(nodeId));
    if (!node) return;
    const modal = document.getElementById('dv-add-modal');
    const err = document.getElementById('dv-add-err');
    if (!modal) return;
    if (err) err.textContent = '';
    editingNodeId = nodeId;
    const title = document.getElementById('dv-add-modal-title');
    if (title) title.textContent = 'Sửa ô';
    const saveLabel = document.getElementById('dv-add-save-label');
    if (saveLabel) saveLabel.textContent = 'Lưu thay đổi';

    // Ẩn ô tìm nhân viên — sửa thì chỉ sửa tên/mã của đúng người đang giữ ô này,
    // không cho đổi sang nhân viên khác (tránh gộp nhầm 2 ô).
    const combo = document.getElementById('dv-emp-combo');
    if (combo) combo.hidden = true;
    picks.emp = { id: null, other: true };
    setCustomVisible('emp', true);
    document.getElementById('dv-add-emp-name').value = node.name || '';
    document.getElementById('dv-add-emp-code').value = node.code || '';

    checked.cv = new Set((node.roles || []).map((r) => r.chuc_vu_id).filter(Boolean));
    checked.vt = new Set((node.vai_tro_tags || []).map((t) => t.vai_tro_id).filter(Boolean));
    bindChecklist('cv');
    bindChecklist('vt');
    const cvFilter = document.getElementById('dv-add-cv-filter');
    const vtFilter = document.getElementById('dv-add-vt-filter');
    if (cvFilter) cvFilter.value = '';
    if (vtFilter) vtFilter.value = '';
    document.getElementById('dv-add-cv-new').value = '';
    document.getElementById('dv-add-vt-new').value = '';

    try {
      const response = await fetch(`/api/mtcl/so-do/catalog?id_depart=${chartState.idDepart}`);
      const data = await response.json();
      catalog.employees = data.employees || [];
      catalog.chuc_vu = data.chuc_vu || [];
      catalog.vai_tro = data.vai_tro || [];
    } catch (_) {
      catalog.employees = [];
      catalog.chuc_vu = [];
      catalog.vai_tro = [];
    }
    renderChecklist('cv');
    renderChecklist('vt');
    modal.hidden = false;
    const empName = document.getElementById('dv-add-emp-name');
    if (empName) empName.focus();
  }

  async function submitAddNode() {
    if (!chartState) return;
    const err = document.getElementById('dv-add-err');
    const pan = document.getElementById('dv-pan');
    const n = chartState.nodes.length;
    const employeeId = picks.emp.other ? null : (Number(document.getElementById('dv-add-emp').value || 0) || null);
    if (!employeeId && !picks.emp.other) {
      err.textContent = 'Chọn nhân viên trong danh sách hoặc chọn Khác';
      return;
    }
    if (picks.emp.other && !document.getElementById('dv-add-emp-name').value.trim()) {
      err.textContent = 'Nhập họ tên khi chọn Khác';
      return;
    }

    const splitNames = (value) => value.split(',').map((s) => s.trim()).filter(Boolean);
    const chucVuIds = [...checked.cv];
    const chucVuNames = splitNames(document.getElementById('dv-add-cv-new').value);
    const vaiTroIds = [...checked.vt];
    const vaiTroNames = splitNames(document.getElementById('dv-add-vt-new').value);
    if (!chucVuIds.length && !chucVuNames.length) {
      err.textContent = 'Chọn ít nhất 1 chức vụ (tick trong danh sách hoặc gõ thêm mới)';
      return;
    }

    const payload = {
      id_depart: chartState.idDepart,
      loai_so_do: chartState.loai,
      employee_id: employeeId,
      employee_name: picks.emp.other ? document.getElementById('dv-add-emp-name').value.trim() : '',
      employee_code: picks.emp.other ? document.getElementById('dv-add-emp-code').value.trim() : '',
      chuc_vu_ids: chucVuIds,
      chuc_vu_names: chucVuNames,
      vai_tro_ids: vaiTroIds,
      vai_tro_names: vaiTroNames,
      cap: 1,
      x: (pan ? pan.scrollLeft : 0) + 80 + (n % 4) * 40,
      y: (pan ? pan.scrollTop : 0) + 100 + Math.floor(n / 4) * 40,
    };
    try {
      const response = await fetch('/api/mtcl/so-do/nodes/batch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        err.textContent = typeof data.detail === 'string' ? data.detail : 'Không thêm được ô';
        return;
      }
      document.getElementById('dv-add-modal').hidden = true;
      if (data.detail_ids && data.detail_ids.length) {
        const idDepart = chartState.idDepart;
        const loai = chartState.loai;
        const soDoId = data.so_do_id;
        const detailIds = data.detail_ids;
        pushUndo(async () => {
          await fetch('/api/mtcl/so-do/nodes/details/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id_depart: idDepart, loai_so_do: loai, so_do_id: soDoId, detail_ids: detailIds }),
          });
          if (typeof global.mtclReloadDepart === 'function') await global.mtclReloadDepart();
        });
      }
      if (typeof global.mtclReloadDepart === 'function') await global.mtclReloadDepart();
    } catch (_) {
      err.textContent = 'Không kết nối được máy chủ';
    }
  }

  async function submitEditNode() {
    if (!chartState || !editingNodeId) return;
    const err = document.getElementById('dv-add-err');
    const node = (chartState.nodes || []).find((n) => String(n.id) === String(editingNodeId));
    if (!node) return;

    const name = document.getElementById('dv-add-emp-name').value.trim();
    if (!name) {
      err.textContent = 'Nhập họ tên';
      return;
    }
    const splitNames = (value) => value.split(',').map((s) => s.trim()).filter(Boolean);
    const chucVuIds = [...checked.cv];
    const chucVuNames = splitNames(document.getElementById('dv-add-cv-new').value);
    const vaiTroIds = [...checked.vt];
    const vaiTroNames = splitNames(document.getElementById('dv-add-vt-new').value);
    if (!chucVuIds.length && !chucVuNames.length) {
      err.textContent = 'Chọn ít nhất 1 chức vụ (tick trong danh sách hoặc gõ thêm mới)';
      return;
    }

    const idDepart = chartState.idDepart;
    const loai = chartState.loai;
    const soDoId = editingNodeId;
    // Lưu lại nội dung cũ để hoàn tác (Ctrl+Z) nếu cần.
    const oldPayload = {
      id_depart: idDepart,
      loai_so_do: loai,
      so_do_id: soDoId,
      employee_name: node.name || '',
      employee_code: node.code || '',
      chuc_vu_ids: (node.roles || []).map((r) => r.chuc_vu_id).filter(Boolean),
      chuc_vu_names: [],
      vai_tro_ids: (node.vai_tro_tags || []).map((t) => t.vai_tro_id).filter(Boolean),
      vai_tro_names: [],
    };
    const payload = {
      id_depart: idDepart,
      loai_so_do: loai,
      so_do_id: soDoId,
      employee_name: name,
      employee_code: document.getElementById('dv-add-emp-code').value.trim(),
      chuc_vu_ids: chucVuIds,
      chuc_vu_names: chucVuNames,
      vai_tro_ids: vaiTroIds,
      vai_tro_names: vaiTroNames,
    };
    try {
      const response = await fetch('/api/mtcl/so-do/nodes/update', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        err.textContent = typeof data.detail === 'string' ? data.detail : 'Không lưu được thay đổi';
        return;
      }
      document.getElementById('dv-add-modal').hidden = true;
      editingNodeId = null;
      pushUndo(async () => {
        await fetch('/api/mtcl/so-do/nodes/update', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(oldPayload),
        });
        if (typeof global.mtclReloadDepart === 'function') await global.mtclReloadDepart();
      });
      if (typeof global.mtclReloadDepart === 'function') await global.mtclReloadDepart();
    } catch (_) {
      err.textContent = 'Không kết nối được máy chủ';
    }
  }

  async function deleteNode(nodeId) {
    if (!chartState || !chartState.isAdmin) return;
    if (!confirm('Xóa ô này khỏi sơ đồ?')) return;
    const response = await fetch('/api/mtcl/so-do/nodes/delete', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id_depart: chartState.idDepart,
        loai_so_do: chartState.loai,
        node_id: Number(nodeId),
      }),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      alert(data.detail || 'Không xóa được ô');
      return;
    }
    // Cập nhật trực tiếp trên canvas, không load lại cả sơ đồ.
    chartState.nodes = chartState.nodes.filter((n) => String(n.id) !== String(nodeId));
    const removedArrowIds = new Set(
      (chartState.arrows || [])
        .filter((a) => String(a.from_node_id) === String(nodeId) || String(a.to_node_id) === String(nodeId))
        .map((a) => String(a.id))
    );
    chartState.arrows = (chartState.arrows || []).filter((a) => !removedArrowIds.has(String(a.id)));
    if (removedArrowIds.has(String(selectedArrowId))) selectedArrowId = null;
    if (String(selectedNodeId) === String(nodeId)) selectedNodeId = null;
    const el = document.querySelector(`.dv-pos[data-node-id="${nodeId}"]`);
    if (el) el.remove();
    drawLines();
  }

  function bindInteractions(chart, pan) {
    let drag = null;
    let arrowDrag = null;
    let panning = null;
    const svg = chart.querySelector('.dv-lines');

    function chartPoint(clientX, clientY) {
      const box = chart.getBoundingClientRect();
      return { x: clientX - box.left, y: clientY - box.top };
    }

    chart.querySelectorAll('.dv-pos').forEach((el) => {
      el.addEventListener('contextmenu', (event) => {
        if (canvasLocked) return;
        event.preventDefault();
        event.stopPropagation();
        showNodeCtxMenu(el.dataset.nodeId, event.clientX, event.clientY);
      });
      el.addEventListener('dblclick', (event) => {
        if (canvasLocked) return;
        event.preventDefault();
        event.stopPropagation();
        openEditNodeModal(el.dataset.nodeId);
      });
      el.addEventListener('pointerdown', (event) => {
        if (canvasLocked) return;
        if (canvasMode === 'arrow' || canvasMode === 'line' || canvasMode === 'rect') return; // để nổi bọt lên nền, vẽ hình tự do từ đây
        event.stopPropagation();
        if (selectedArrowId) {
          selectedArrowId = null;
          drawLines();
        }
        const id = el.dataset.nodeId;
        if (canvasMode !== 'move') return;
        el.setPointerCapture(event.pointerId);
        drag = {
          el,
          nodeId: id,
          startX: event.clientX,
          startY: event.clientY,
          left: parseFloat(el.style.left) || 0,
          top: parseFloat(el.style.top) || 0,
          lastLeft: parseFloat(el.style.left) || 0,
          lastTop: parseFloat(el.style.top) || 0,
          moved: false,
          prevZIndex: el.style.zIndex,
        };
        el.classList.add('is-dragging');
        el.style.zIndex = '1000';
      });
      el.addEventListener('pointermove', (event) => {
        if (!drag || drag.el !== el) return;
        const dx = event.clientX - drag.startX;
        const dy = event.clientY - drag.startY;
        if (Math.abs(dx) + Math.abs(dy) > 3) drag.moved = true;
        const nextLeft = Math.max(0, drag.left + dx);
        const nextTop = Math.max(0, drag.top + dy);
        offsetArrowsForNode(drag.nodeId, nextLeft - drag.lastLeft, nextTop - drag.lastTop);
        drag.lastLeft = nextLeft;
        drag.lastTop = nextTop;
        el.style.left = `${nextLeft}px`;
        el.style.top = `${nextTop}px`;
        scheduleDraw();
      });
      el.addEventListener('pointerup', (event) => {
        if (!drag || drag.el !== el) return;
        el.releasePointerCapture(event.pointerId);
        el.classList.remove('is-dragging');
        el.style.zIndex = drag.prevZIndex || '';
        if (drag.moved) {
          const nodeId = el.dataset.nodeId;
          const oldX = drag.left;
          const oldY = drag.top;
          const newX = parseFloat(el.style.left) || 0;
          const newY = parseFloat(el.style.top) || 0;
          savePosition(nodeId, newX, newY);
          pushUndo(async () => {
            el.style.left = `${oldX}px`;
            el.style.top = `${oldY}px`;
            offsetArrowsForNode(nodeId, oldX - newX, oldY - newY);
            drawLines();
            await savePosition(nodeId, oldX, oldY);
          });
        } else {
          // Bấm không kéo = chọn khối này (để dùng nút sắp xếp lớp trên/dưới).
          selectedNodeId = drag.nodeId;
          document.querySelectorAll('.dv-pos').forEach((c) => {
            c.classList.toggle('is-node-selected', c === el);
          });
          drawLines();
        }
        drag = null;
      });
    });

    if (svg) {
      svg.addEventListener('pointerdown', (event) => {
        if (canvasLocked) return;
        if (!chartState || !chartState.isAdmin) return;
        const handle = event.target.closest('.dv-arrow-handle');
        const mid = event.target.closest('.dv-arrow-mid');
        const hit = event.target.closest('.dv-arrow-hit');
        const target = handle || mid || hit;
        if (!target) return;
        event.preventDefault();
        event.stopPropagation();
        const arrowId = target.getAttribute('data-arrow-id');
        const arrow = (chartState.arrows || []).find((a) => String(a.id) === String(arrowId));
        if (!arrow) return;
        if (!arrow.points) arrow.points = [];
        selectedArrowId = arrowId;
        if (selectedNodeId) {
          selectedNodeId = null;
          document.querySelectorAll('.dv-pos').forEach((c) => c.classList.remove('is-node-selected'));
        }

        let end = 'both';
        let origX = Number(arrow.x1) || 0;
        let origY = Number(arrow.y1) || 0;
        let insertedIndex = -1;
        if (mid) {
          // Kéo tâm đoạn thẳng → chèn 1 điểm gập khúc mới ngay tại đó rồi kéo tiếp điểm này.
          const segIndex = Number(mid.getAttribute('data-seg'));
          const fullPoints = [
            [Number(arrow.x1) || 0, Number(arrow.y1) || 0],
            ...arrow.points,
            [Number(arrow.x2) || 0, Number(arrow.y2) || 0],
          ];
          const [mx, my] = midpoint(fullPoints[segIndex], fullPoints[segIndex + 1]);
          arrow.points.splice(segIndex, 0, [mx, my]);
          end = `pt-${segIndex}`;
          origX = mx;
          origY = my;
          insertedIndex = segIndex;
        } else if (handle) {
          end = handle.getAttribute('data-end');
          if (end === '2') {
            origX = Number(arrow.x2) || 0;
            origY = Number(arrow.y2) || 0;
          } else if (end && end.startsWith('pt-')) {
            const idx = Number(end.slice(3));
            const p = arrow.points[idx];
            if (!p) return;
            origX = p[0];
            origY = p[1];
          }
        }

        drawLines();
        const pt = chartPoint(event.clientX, event.clientY);
        svg.setPointerCapture(event.pointerId);
        arrowDrag = {
          id: event.pointerId,
          arrowId,
          end,
          insertedIndex,
          startX: pt.x,
          startY: pt.y,
          origX,
          origY,
          x1: Number(arrow.x1) || 0,
          y1: Number(arrow.y1) || 0,
          x2: Number(arrow.x2) || 0,
          y2: Number(arrow.y2) || 0,
          origPoints: arrow.points.map((p) => [p[0], p[1]]),
          moved: false,
        };
      });
      svg.addEventListener('pointermove', (event) => {
        if (!arrowDrag || arrowDrag.id !== event.pointerId) return;
        const arrow = (chartState.arrows || []).find((a) => String(a.id) === String(arrowDrag.arrowId));
        if (!arrow) return;
        const pt = chartPoint(event.clientX, event.clientY);
        const dx = pt.x - arrowDrag.startX;
        const dy = pt.y - arrowDrag.startY;
        if (Math.abs(dx) + Math.abs(dy) > 2) arrowDrag.moved = true;
        if (arrowDrag.end === '1') {
          arrow.x1 = arrowDrag.x1 + dx;
          arrow.y1 = arrowDrag.y1 + dy;
        } else if (arrowDrag.end === '2') {
          arrow.x2 = arrowDrag.x2 + dx;
          arrow.y2 = arrowDrag.y2 + dy;
        } else if (arrowDrag.end && arrowDrag.end.startsWith('pt-')) {
          const idx = Number(arrowDrag.end.slice(3));
          if (arrow.points && arrow.points[idx]) {
            arrow.points[idx] = [arrowDrag.origX + dx, arrowDrag.origY + dy];
          }
        } else {
          arrow.x1 = arrowDrag.x1 + dx;
          arrow.y1 = arrowDrag.y1 + dy;
          arrow.x2 = arrowDrag.x2 + dx;
          arrow.y2 = arrowDrag.y2 + dy;
          if (arrowDrag.origPoints.length) {
            arrow.points = arrowDrag.origPoints.map((p) => [p[0] + dx, p[1] + dy]);
          }
        }
        scheduleDraw();
      });
      const stopArrowDrag = (event) => {
        if (!arrowDrag || (event && arrowDrag.id !== event.pointerId)) return;
        const drag = arrowDrag;
        arrowDrag = null;
        try {
          svg.releasePointerCapture(event.pointerId);
        } catch (_) { /* ignore */ }
        if (!drag.moved) return;
        saveArrowCoords([drag.arrowId]);
        if (drag.insertedIndex >= 0) {
          // Undo: bỏ điểm gập khúc vừa chèn (trả về đường thẳng như trước khi kéo tâm đoạn).
          pushUndo(async () => {
            const arrow = (chartState.arrows || []).find((a) => String(a.id) === String(drag.arrowId));
            if (arrow && arrow.points) arrow.points.splice(drag.insertedIndex, 1);
            await saveArrowCoords([drag.arrowId]);
            drawLines();
          });
        } else if (drag.end === '1' || drag.end === '2' || (drag.end && drag.end.startsWith('pt-'))) {
          // Undo: trả điểm (đầu mũi tên hoặc điểm gập có sẵn) về đúng tọa độ cũ.
          pushUndo(async () => {
            const arrow = (chartState.arrows || []).find((a) => String(a.id) === String(drag.arrowId));
            if (arrow) {
              if (drag.end === '1') {
                arrow.x1 = drag.origX;
                arrow.y1 = drag.origY;
              } else if (drag.end === '2') {
                arrow.x2 = drag.origX;
                arrow.y2 = drag.origY;
              } else {
                const idx = Number(drag.end.slice(3));
                if (arrow.points && arrow.points[idx]) arrow.points[idx] = [drag.origX, drag.origY];
              }
            }
            await saveArrowCoords([drag.arrowId]);
            drawLines();
          });
        } else {
          // Undo: kéo cả mũi tên (thân đường) — trả toàn bộ tọa độ về vị trí cũ.
          pushUndo(async () => {
            const arrow = (chartState.arrows || []).find((a) => String(a.id) === String(drag.arrowId));
            if (arrow) {
              arrow.x1 = drag.x1;
              arrow.y1 = drag.y1;
              arrow.x2 = drag.x2;
              arrow.y2 = drag.y2;
              arrow.points = drag.origPoints.map((p) => [p[0], p[1]]);
            }
            await saveArrowCoords([drag.arrowId]);
            drawLines();
          });
        }
      };
      svg.addEventListener('pointerup', stopArrowDrag);
      svg.addEventListener('pointercancel', stopArrowDrag);
      svg.addEventListener('dblclick', (event) => {
        if (canvasLocked) return;
        if (!chartState || !chartState.isAdmin) return;
        const handle = event.target.closest('.dv-arrow-handle');
        const end = handle && handle.getAttribute('data-end');
        if (!end || !end.startsWith('pt-')) return;
        event.preventDefault();
        event.stopPropagation();
        const arrowId = handle.getAttribute('data-arrow-id');
        const arrow = (chartState.arrows || []).find((a) => String(a.id) === String(arrowId));
        if (!arrow || !arrow.points) return;
        arrow.points.splice(Number(end.slice(3)), 1);
        drawLines();
        saveArrowCoords([arrowId]);
      });
    }

    pan.addEventListener('pointerdown', (event) => {
      if (
        event.target.closest('.dv-pos')
        || event.target.closest('.dv-add-modal')
        || event.target.closest('.dv-toolbar')
        || event.target.closest('.dv-arrow-handle')
        || event.target.closest('.dv-arrow-hit')
        || event.target.closest('.dv-arrow-mid')
      ) {
        return;
      }
      if (canvasLocked) {
        // Khi khóa: chỉ cho phép pan (cuộn), chặn vẽ hình.
        if (canvasMode !== 'move') return;
      }
      if (canvasMode === 'arrow' || canvasMode === 'line' || canvasMode === 'rect') {
        // Vẽ hình/mũi tên tự do: giữ chuột ở bất kỳ đâu và kéo, không cần liên quan tới khối nào.
        const pt = chartPoint(event.clientX, event.clientY);
        event.preventDefault();
        pan.setPointerCapture(event.pointerId);
        shapeDraft = {
          type: canvasMode,
          x1: pt.x,
          y1: pt.y,
          x2: pt.x,
          y2: pt.y,
          pointerId: event.pointerId,
        };
        drawLines();
        return;
      }
      if (canvasMode !== 'move') return;
      panning = {
        id: event.pointerId,
        x: event.clientX,
        y: event.clientY,
        left: pan.scrollLeft,
        top: pan.scrollTop,
      };
      pan.setPointerCapture(event.pointerId);
      pan.classList.add('is-panning');
    });
    pan.addEventListener('pointermove', (event) => {
      if (shapeDraft && shapeDraft.pointerId === event.pointerId) {
        const pt = chartPoint(event.clientX, event.clientY);
        shapeDraft.x2 = pt.x;
        shapeDraft.y2 = pt.y;
        scheduleDraw();
        return;
      }
      if (!panning || panning.id !== event.pointerId) return;
      pan.scrollLeft = panning.left - (event.clientX - panning.x);
      pan.scrollTop = panning.top - (event.clientY - panning.y);
    });
    pan.addEventListener('pointerup', (event) => {
      if (shapeDraft && shapeDraft.pointerId === event.pointerId) {
        const pt = chartPoint(event.clientX, event.clientY);
        const draft = shapeDraft;
        draft.x2 = pt.x;
        draft.y2 = pt.y;
        try {
          pan.releasePointerCapture(event.pointerId);
        } catch (_) { /* ignore */ }
        shapeDraft = null;
        drawLines();
        const moved = Math.abs(draft.x2 - draft.x1) + Math.abs(draft.y2 - draft.y1) > 3;
        if (moved) saveShape(draft.type, draft.x1, draft.y1, draft.x2, draft.y2);
      }
    });
    const stopPan = () => {
      panning = null;
      pan.classList.remove('is-panning');
    };
    pan.addEventListener('pointerup', stopPan);
    pan.addEventListener('pointercancel', (event) => {
      stopPan();
      if (shapeDraft && shapeDraft.pointerId === event.pointerId) {
        shapeDraft = null;
        drawLines();
      }
    });

    const toolbar = document.getElementById('dv-toolbar');
    if (toolbar) {
      toolbar.addEventListener('click', async (event) => {
        const btn = event.target.closest('.dv-tool');
        if (!btn || btn.disabled) return;
        event.preventDefault();
        event.stopPropagation();
        if (btn.dataset.action === 'add-node') {
          openAddNodeModal();
          return;
        }
        if (btn.dataset.action === 'toggle-grid') {
          toggleGrid();
          return;
        }
        if (btn.dataset.action === 'undo') {
          performUndo();
          return;
        }
        if (btn.dataset.action === 'z-front') {
          applyZOrder('front');
          return;
        }
        if (btn.dataset.action === 'z-forward') {
          applyZOrder('forward');
          return;
        }
        if (btn.dataset.action === 'z-backward') {
          applyZOrder('backward');
          return;
        }
        if (btn.dataset.action === 'z-back') {
          applyZOrder('back');
          return;
        }
        if (btn.dataset.action === 'add-note') {
          openAddNoteModal();
          return;
        }
        if (btn.dataset.action === 'auto-align') {
          autoAlign();
          return;
        }
        if (btn.dataset.action === 'toggle-lock') {
          toggleLock();
          return;
        }
        if (canvasLocked) return;
        if (!btn.dataset.mode) return;
        setMode(btn.dataset.mode);
      });
    }

    const saveBtn = document.getElementById('dv-add-save');
    const cancelBtn = document.getElementById('dv-add-cancel');
    if (saveBtn) saveBtn.onclick = () => (editingNodeId ? submitEditNode() : submitAddNode());
    if (cancelBtn) {
      cancelBtn.onclick = () => {
        editingNodeId = null;
        document.getElementById('dv-add-modal').hidden = true;
      };
    }

    const noteSaveBtn = document.getElementById('dv-note-save');
    const noteCancelBtn = document.getElementById('dv-note-cancel');
    const noteAddRowBtn = document.getElementById('dv-note-add-row');
    if (noteSaveBtn) noteSaveBtn.onclick = submitNoteForm;
    if (noteCancelBtn) {
      noteCancelBtn.onclick = () => {
        document.getElementById('dv-note-modal').hidden = true;
      };
    }
    if (noteAddRowBtn) noteAddRowBtn.onclick = () => addNoteRow();
    const noteRows = document.getElementById('dv-note-rows');
    if (noteRows) {
      noteRows.addEventListener('click', (event) => {
        const removeBtn = event.target.closest('[data-remove-note]');
        if (removeBtn) {
          removeNoteRow(Number(removeBtn.dataset.removeNote));
          return;
        }
        const swatch = event.target.closest('.dv-color-swatch');
        if (swatch) {
          const idx = Number(swatch.dataset.noteIdx);
          noteRowColors[idx] = swatch.dataset.color;
          noteRows.querySelectorAll(`.dv-color-swatch[data-note-idx="${idx}"]`).forEach((s) => {
            s.classList.toggle('active', s.dataset.color === swatch.dataset.color);
          });
        }
      });
    }
  }

  async function autoAlign() {
    if (!chartState || !chartState.nodes.length) return;
    const nodes = chartState.nodes;
    const chart = document.querySelector('#dv-body .dv-chart');
    if (!chart) return;

    // Lưu vị trí cũ để undo
    const oldPositions = nodes.map((n) => ({ id: n.id, x: Number(n.x) || 0, y: Number(n.y) || 0 }));
    const oldArrows = (chartState.arrows || []).map((a) => ({
      id: a.id,
      x1: Number(a.x1) || 0, y1: Number(a.y1) || 0,
      x2: Number(a.x2) || 0, y2: Number(a.y2) || 0,
      points: (a.points || []).map((p) => [p[0], p[1]]),
    }));

    // Đọc kích thước thật của từng card trên DOM
    const cardSizes = new Map();
    for (const node of nodes) {
      const el = chart.querySelector(`.dv-pos[data-node-id="${node.id}"]`);
      if (el) {
        cardSizes.set(String(node.id), { w: el.offsetWidth, h: el.offsetHeight });
      } else {
        cardSizes.set(String(node.id), { w: 260, h: 120 });
      }
    }

    // Gom các node thành hàng dựa theo toạ độ Y gần nhau (threshold dựa trên chiều cao card trung bình)
    const avgH = [...cardSizes.values()].reduce((s, c) => s + c.h, 0) / cardSizes.size;
    const rowThreshold = avgH * 0.6;
    const sorted = nodes.slice().sort((a, b) => (Number(a.y) || 0) - (Number(b.y) || 0));
    const rows = [];
    for (const node of sorted) {
      const ny = Number(node.y) || 0;
      let placed = false;
      for (const row of rows) {
        const rowAvgY = row.reduce((s, n) => s + (Number(n.y) || 0), 0) / row.length;
        if (Math.abs(ny - rowAvgY) <= rowThreshold) {
          row.push(node);
          placed = true;
          break;
        }
      }
      if (!placed) rows.push([node]);
    }

    // Sắp xếp từng hàng theo X
    for (const row of rows) {
      row.sort((a, b) => (Number(a.x) || 0) - (Number(b.x) || 0));
    }
    // Sắp xếp hàng theo Y trung bình
    rows.sort((a, b) => {
      const ay = a.reduce((s, n) => s + (Number(n.y) || 0), 0) / a.length;
      const by = b.reduce((s, n) => s + (Number(n.y) || 0), 0) / b.length;
      return ay - by;
    });

    const gapX = 30;
    const gapY = 40;

    // Hàng trên cùng làm mốc cố định: giữ nguyên Y, lấy tâm X làm trục căn giữa
    const topRow = rows[0];
    const topRowY = topRow.reduce((s, n) => s + (Number(n.y) || 0), 0) / topRow.length;
    const topRowMinX = Math.min(...topRow.map((n) => Number(n.x) || 0));
    const topRowMaxRight = Math.max(...topRow.map((n) => {
      const sz = cardSizes.get(String(n.id)) || { w: 260 };
      return (Number(n.x) || 0) + sz.w;
    }));
    const topRowCenterX = (topRowMinX + topRowMaxRight) / 2;

    function rowTotalWidth(row) {
      let w = 0;
      for (let i = 0; i < row.length; i++) {
        w += (cardSizes.get(String(row[i].id)) || { w: 260 }).w;
        if (i < row.length - 1) w += gapX;
      }
      return w;
    }

    const rowMaxH = rows.map((row) =>
      Math.max(...row.map((n) => (cardSizes.get(String(n.id)) || { h: 120 }).h))
    );

    let currentY = Math.round(topRowY);
    const newPositions = [];

    for (let r = 0; r < rows.length; r++) {
      const row = rows[r];
      const totalW = rowTotalWidth(row);
      let currentX = topRowCenterX - totalW / 2;

      for (let c = 0; c < row.length; c++) {
        const node = row[c];
        const size = cardSizes.get(String(node.id)) || { w: 260, h: 120 };
        const oldX = Number(node.x) || 0;
        const oldY = Number(node.y) || 0;
        const newX = Math.round(currentX);
        const newY = currentY;

        offsetArrowsForNode(node.id, newX - oldX, newY - oldY);
        node.x = newX;
        node.y = newY;
        newPositions.push({ node_id: Number(node.id), x: newX, y: newY });

        const el = chart.querySelector(`.dv-pos[data-node-id="${node.id}"]`);
        if (el) {
          el.style.left = `${newX}px`;
          el.style.top = `${newY}px`;
        }

        currentX += size.w + gapX;
      }
      currentY += rowMaxH[r] + gapY;
    }

    drawLines();

    // Lưu tất cả vị trí mới lên server
    try {
      await fetch('/api/mtcl/so-do/positions', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id_depart: chartState.idDepart,
          loai_so_do: chartState.loai,
          positions: newPositions,
        }),
      });
      // Lưu tọa độ mũi tên đã dịch
      const arrowIds = (chartState.arrows || []).map((a) => a.id);
      if (arrowIds.length) await saveArrowCoords(arrowIds);
    } catch (_) { /* ignore */ }

    // Undo: trả về vị trí cũ
    pushUndo(async () => {
      for (const old of oldPositions) {
        const node = (chartState.nodes || []).find((n) => String(n.id) === String(old.id));
        if (node) {
          node.x = old.x;
          node.y = old.y;
          const el = chart.querySelector(`.dv-pos[data-node-id="${node.id}"]`);
          if (el) {
            el.style.left = `${old.x}px`;
            el.style.top = `${old.y}px`;
          }
        }
      }
      for (const oa of oldArrows) {
        const arrow = (chartState.arrows || []).find((a) => String(a.id) === String(oa.id));
        if (arrow) {
          arrow.x1 = oa.x1; arrow.y1 = oa.y1;
          arrow.x2 = oa.x2; arrow.y2 = oa.y2;
          arrow.points = oa.points.map((p) => [p[0], p[1]]);
        }
      }
      drawLines();
      try {
        await fetch('/api/mtcl/so-do/positions', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            id_depart: chartState.idDepart,
            loai_so_do: chartState.loai,
            positions: oldPositions.map((o) => ({ node_id: Number(o.id), x: o.x, y: o.y })),
          }),
        });
        const arrowIds = oldArrows.map((a) => a.id);
        if (arrowIds.length) await saveArrowCoords(arrowIds);
      } catch (_) { /* ignore */ }
    });
  }

  // ── Context menu (chuột phải) trên khối ──────────────────────────
  let ctxMenu = null;
  function hideCtxMenu() {
    if (ctxMenu) { ctxMenu.remove(); ctxMenu = null; }
  }
  document.addEventListener('click', hideCtxMenu);
  document.addEventListener('contextmenu', (e) => {
    if (ctxMenu && !ctxMenu.contains(e.target)) hideCtxMenu();
  });

  function showNodeCtxMenu(nodeId, clientX, clientY) {
    hideCtxMenu();
    if (!chartState || !chartState.isAdmin) return;
    const node = (chartState.nodes || []).find((n) => String(n.id) === String(nodeId));
    if (!node) return;
    const esc = global.mtclEsc;
    const links = node.links || [];

    const menu = document.createElement('div');
    menu.className = 'dv-ctx-menu';
    menu.style.left = `${clientX}px`;
    menu.style.top = `${clientY}px`;

    let html = `<div class="dv-ctx-title">${esc(node.name)}</div>`;
    html += `<button type="button" class="dv-ctx-item" data-ctx-action="add-link"><span class="material-symbols-outlined">add_link</span>Thêm link</button>`;
    if (links.length) {
      html += `<div class="dv-ctx-sep"></div>`;
      links.forEach((l) => {
        const display = esc(l.label || l.url).slice(0, 40);
        html += `<div class="dv-ctx-link-row" data-link-id="${l.id}">
          <a href="${esc(l.url)}" target="_blank" rel="noopener" class="dv-ctx-link-url" title="${esc(l.url)}">${display}</a>
          <button type="button" class="dv-ctx-link-btn" data-ctx-action="edit-link" data-link-id="${l.id}" title="Sửa"><span class="material-symbols-outlined">edit</span></button>
          <button type="button" class="dv-ctx-link-btn dv-ctx-link-del" data-ctx-action="del-link" data-link-id="${l.id}" title="Xóa"><span class="material-symbols-outlined">delete</span></button>
        </div>`;
      });
    }
    menu.innerHTML = html;
    document.body.appendChild(menu);
    ctxMenu = menu;

    // Ensure menu stays within viewport
    const rect = menu.getBoundingClientRect();
    if (rect.right > window.innerWidth) menu.style.left = `${clientX - rect.width}px`;
    if (rect.bottom > window.innerHeight) menu.style.top = `${clientY - rect.height}px`;

    menu.addEventListener('click', async (e) => {
      const btn = e.target.closest('[data-ctx-action]');
      if (!btn) return;
      e.stopPropagation();
      const action = btn.dataset.ctxAction;
      if (action === 'add-link') {
        hideCtxMenu();
        openLinkModal(nodeId, null);
      } else if (action === 'edit-link') {
        hideCtxMenu();
        openLinkModal(nodeId, Number(btn.dataset.linkId));
      } else if (action === 'del-link') {
        hideCtxMenu();
        await deleteLinkAction(nodeId, Number(btn.dataset.linkId));
      }
    });
  }

  // ── Link modal (thêm / sửa link) ──────────────────────────────
  function ensureLinkModal() {
    let modal = document.getElementById('dv-link-modal');
    if (modal) return modal;
    modal = document.createElement('div');
    modal.id = 'dv-link-modal';
    modal.className = 'dv-link-modal-overlay';
    modal.hidden = true;
    modal.innerHTML = `
      <div class="dv-link-modal-box">
        <h3 id="dv-link-modal-title">Thêm link</h3>
        <label>URL <input id="dv-link-url" type="url" placeholder="https://..." autocomplete="off"></label>
        <label>Tiêu đề (tuỳ chọn) <input id="dv-link-label" type="text" placeholder="Tên hiển thị" autocomplete="off"></label>
        <div id="dv-link-suggestions" class="dv-link-suggestions"></div>
        <div id="dv-link-err" style="color:#d32f2f;font-size:12px;min-height:16px"></div>
        <div class="dv-link-modal-actions">
          <button type="button" id="dv-link-cancel">Huỷ</button>
          <button type="button" id="dv-link-save">Lưu</button>
        </div>
      </div>`;
    document.body.appendChild(modal);
    modal.querySelector('#dv-link-cancel').onclick = () => { modal.hidden = true; };
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.hidden = true; });
    return modal;
  }

  let linkModalNodeId = null;
  let linkModalLinkId = null;

  function openLinkModal(nodeId, linkId) {
    const modal = ensureLinkModal();
    const esc = global.mtclEsc;
    linkModalNodeId = nodeId;
    linkModalLinkId = linkId;
    const titleEl = modal.querySelector('#dv-link-modal-title');
    const urlEl = modal.querySelector('#dv-link-url');
    const labelEl = modal.querySelector('#dv-link-label');
    const errEl = modal.querySelector('#dv-link-err');
    const sugEl = modal.querySelector('#dv-link-suggestions');
    errEl.textContent = '';

    const node = (chartState.nodes || []).find((n) => String(n.id) === String(nodeId));
    if (linkId) {
      titleEl.textContent = 'Sửa link';
      const link = node && (node.links || []).find((l) => l.id === linkId);
      urlEl.value = link ? link.url : '';
      labelEl.value = link ? link.label : '';
    } else {
      titleEl.textContent = 'Thêm link';
      urlEl.value = '';
      labelEl.value = '';
    }

    // Gợi ý nội dung có sẵn từ khối
    const suggestions = [];
    if (node) {
      if (node.name) suggestions.push(node.name);
      if (node.code) suggestions.push(node.code);
      (node.roles || []).forEach((r) => {
        if (r.chuc_vu) suggestions.push(r.chuc_vu);
        if (r.vai_tro) suggestions.push(r.vai_tro);
      });
      (node.vai_tro_tags || []).forEach((t) => {
        if (t.vai_tro) suggestions.push(t.vai_tro);
      });
    }
    const unique = [...new Set(suggestions)].filter(Boolean);
    if (unique.length) {
      sugEl.innerHTML = unique.map((s) =>
        `<button type="button" class="dv-link-sug-chip" title="${esc(s)}">${esc(s)}</button>`
      ).join('');
      sugEl.hidden = false;
      sugEl.onclick = (e) => {
        const chip = e.target.closest('.dv-link-sug-chip');
        if (chip) labelEl.value = chip.textContent;
      };
    } else {
      sugEl.innerHTML = '';
      sugEl.hidden = true;
    }

    modal.hidden = false;
    urlEl.focus();
    const saveBtn = modal.querySelector('#dv-link-save');
    saveBtn.onclick = () => saveLinkAction();
  }

  async function saveLinkAction() {
    const modal = document.getElementById('dv-link-modal');
    const urlEl = modal.querySelector('#dv-link-url');
    const labelEl = modal.querySelector('#dv-link-label');
    const errEl = modal.querySelector('#dv-link-err');
    const url = (urlEl.value || '').trim();
    const label = (labelEl.value || '').trim();
    if (!url) { errEl.textContent = 'Vui lòng nhập URL'; return; }
    const { loai, idDepart } = chartState;
    try {
      let res;
      if (linkModalLinkId) {
        res = await fetch('/api/mtcl/so-do/nodes/links', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id_depart: idDepart, loai_so_do: loai, link_id: linkModalLinkId, url, label }),
        });
      } else {
        res = await fetch('/api/mtcl/so-do/nodes/links', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ id_depart: idDepart, loai_so_do: loai, so_do_id: Number(linkModalNodeId), url, label }),
        });
      }
      const data = await res.json();
      if (!data.ok) { errEl.textContent = data.error || 'Lỗi'; return; }
      modal.hidden = true;
      // Cập nhật local state
      const node = (chartState.nodes || []).find((n) => String(n.id) === String(linkModalNodeId));
      if (node) {
        if (!node.links) node.links = [];
        if (linkModalLinkId) {
          const idx = node.links.findIndex((l) => l.id === linkModalLinkId);
          if (idx >= 0) { node.links[idx].url = url; node.links[idx].label = label; }
        } else {
          node.links.push({ id: data.id, url: data.url, label: data.label });
        }
      }
      // Re-render card
      refreshNodeCard(linkModalNodeId);
      const undoLinkId = linkModalLinkId;
      const undoNodeId = linkModalNodeId;
      if (undoLinkId) {
        // Undo sửa link — không cần, user có thể sửa lại
      } else {
        // Undo thêm link
        pushUndo(async () => {
          await fetch('/api/mtcl/so-do/nodes/links/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id_depart: idDepart, loai_so_do: loai, link_id: data.id }),
          });
          if (node) node.links = (node.links || []).filter((l) => l.id !== data.id);
          refreshNodeCard(undoNodeId);
        });
      }
    } catch (_) {
      errEl.textContent = 'Không kết nối được máy chủ';
    }
  }

  async function deleteLinkAction(nodeId, linkId) {
    if (!chartState) return;
    const { loai, idDepart } = chartState;
    const node = (chartState.nodes || []).find((n) => String(n.id) === String(nodeId));
    const oldLink = node && (node.links || []).find((l) => l.id === linkId);
    try {
      await fetch('/api/mtcl/so-do/nodes/links/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id_depart: idDepart, loai_so_do: loai, link_id: linkId }),
      });
      if (node) node.links = (node.links || []).filter((l) => l.id !== linkId);
      refreshNodeCard(nodeId);
      if (oldLink) {
        pushUndo(async () => {
          const res = await fetch('/api/mtcl/so-do/nodes/links', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id_depart: idDepart, loai_so_do: loai, so_do_id: Number(nodeId), url: oldLink.url, label: oldLink.label }),
          });
          const data = await res.json();
          if (data.ok && node) {
            if (!node.links) node.links = [];
            node.links.push({ id: data.id, url: data.url, label: data.label });
            refreshNodeCard(nodeId);
          }
        });
      }
    } catch (_) { /* ignore */ }
  }

  function refreshNodeCard(nodeId) {
    const el = document.querySelector(`.dv-pos[data-node-id="${nodeId}"]`);
    if (!el) return;
    const node = (chartState.nodes || []).find((n) => String(n.id) === String(nodeId));
    if (!node) return;
    const tmp = document.createElement('div');
    tmp.innerHTML = cardHtml(node);
    const newCard = tmp.firstElementChild;
    el.style.left = newCard.style.left;
    el.style.top = newCard.style.top;
    el.innerHTML = newCard.innerHTML;
  }

  function renderOrgCanvas(nodes, emptyMessage, loai, idDepart, isAdmin, arrows) {
    const list = nodes || [];
    chartState = {
      nodes: list.map((n) => ({ ...n })),
      arrows: (arrows || []).map((a) => ({ ...a })),
      loai,
      idDepart,
      isAdmin: !!isAdmin,
    };
    // Chỉ giữ lịch sử hoàn tác khi vẫn cùng 1 sơ đồ (idDepart+loai) — đổi đơn vị/loại sơ đồ thì xóa,
    // vì lệnh hoàn tác cũ tham chiếu tới id ô/mũi tên của sơ đồ trước, áp nhầm sang sơ đồ khác sẽ sai dữ liệu.
    const contextKey = `${idDepart}:${loai}`;
    if (contextKey !== undoContextKey) {
      undoContextKey = contextKey;
      undoStack = [];
    }
    canvasMode = 'move';
    shapeDraft = null;
    selectedArrowId = null;
    selectedNodeId = null;
    editingNodeId = null;
    editingNoteId = null;
    const emptyHint = !list.length
      ? `<p class="dv-canvas-empty">${global.mtclEsc(emptyMessage || 'Chưa có ô — bấm Thêm ô để bắt đầu')}</p>`
      : '';
    return `<div class="dv-org-wrap">
      ${toolbarHtml(!!isAdmin)}
      <div class="dv-canvas-area">
        <div class="dv-pan" id="dv-pan">
          ${addNodeModalHtml()}
          ${noteModalHtml()}
          <div class="dv-chart">
            <svg class="dv-lines" aria-hidden="true"></svg>
            ${list.map(cardHtml).join('')}
            ${emptyHint}
          </div>
        </div>
        <div class="dv-legend-panel" id="dv-legend-panel" hidden></div>
      </div>
    </div>`;
  }

  function mountOrgCanvas() {
    const pan = document.getElementById('dv-pan');
    const chart = document.querySelector('#dv-body .dv-chart');
    if (!pan || !chart) return;
    chart.classList.toggle('dv-grid-on', gridVisible);
    updateUndoButton();
    bindStylePicker();
    bindLegendPanel();
    lastPickerSig = null;
    bindInteractions(chart, pan);
    // Restore lock state from localStorage
    try {
      const lockKey = chartState ? `mtcl_locked_${chartState.idDepart}_${chartState.loai}` : 'mtcl_locked';
      const saved = localStorage.getItem(lockKey);
      canvasLocked = saved === '1';
      if (canvasLocked) {
        const btn = document.getElementById('dv-lock-btn');
        if (btn) {
          btn.classList.add('active');
          btn.innerHTML = '<span class="material-symbols-outlined text-[16px]">lock</span>Đã khóa';
        }
        const hint = document.getElementById('dv-tool-hint');
        if (hint) hint.textContent = 'Sơ đồ đang bị khóa — nhấn Khóa để mở';
        pan.classList.add('dv-locked');
        const toolbar = document.getElementById('dv-toolbar');
        if (toolbar) {
          toolbar.querySelectorAll('.dv-tool').forEach((b) => {
            const act = b.dataset.action || '';
            if (act === 'toggle-lock' || act === 'toggle-grid') return;
            b.disabled = true;
          });
        }
      }
    } catch (_) { /* ignore */ }
    requestAnimationFrame(() => requestAnimationFrame(drawLines));
  }

  function toggleGrid() {
    gridVisible = !gridVisible;
    try {
      localStorage.setItem('mtcl_grid_visible', gridVisible ? '1' : '0');
    } catch (_) { /* ignore */ }
    const chart = document.querySelector('#dv-body .dv-chart');
    if (chart) chart.classList.toggle('dv-grid-on', gridVisible);
    const btn = document.getElementById('dv-grid-btn');
    if (btn) btn.classList.toggle('active', gridVisible);
  }

  function toggleLock() {
    canvasLocked = !canvasLocked;
    const key = chartState ? `mtcl_locked_${chartState.idDepart}_${chartState.loai}` : 'mtcl_locked';
    try {
      localStorage.setItem(key, canvasLocked ? '1' : '0');
    } catch (_) { /* ignore */ }
    const btn = document.getElementById('dv-lock-btn');
    if (btn) {
      btn.classList.toggle('active', canvasLocked);
      btn.innerHTML = `<span class="material-symbols-outlined text-[16px]">${canvasLocked ? 'lock' : 'lock_open'}</span>${canvasLocked ? 'Đã khóa' : 'Khóa'}`;
    }
    const hint = document.getElementById('dv-tool-hint');
    if (hint) hint.textContent = canvasLocked ? 'Sơ đồ đang bị khóa — nhấn Khóa để mở' : 'Thêm ô → kéo vị trí → nối mũi tên';
    // Disable/enable toolbar buttons
    const toolbar = document.getElementById('dv-toolbar');
    if (toolbar) {
      toolbar.querySelectorAll('.dv-tool').forEach((b) => {
        const act = b.dataset.action || '';
        const mode = b.dataset.mode || '';
        if (act === 'toggle-lock' || act === 'toggle-grid') return;
        if (canvasLocked) {
          b.disabled = true;
        } else {
          b.disabled = false;
        }
      });
    }
    // Toggle visual overlay
    const pan = document.querySelector('#dv-body .dv-pan');
    if (pan) pan.classList.toggle('dv-locked', canvasLocked);
  }

  function pushUndo(undoFn) {
    undoStack.push(undoFn);
    if (undoStack.length > 30) undoStack.shift();
    updateUndoButton();
  }

  function updateUndoButton() {
    const btn = document.getElementById('dv-undo-btn');
    if (btn) btn.disabled = !undoStack.length;
  }

  function updateZButtons() {
    const has = !!(selectedNodeId || selectedArrowId);
    ['dv-z-front', 'dv-z-forward', 'dv-z-backward', 'dv-z-back'].forEach((id) => {
      const btn = document.getElementById(id);
      if (btn) btn.disabled = !has;
    });
  }

  async function applyZOrder(action) {
    if (!chartState) return;
    const targetType = selectedNodeId ? 'node' : (selectedArrowId ? 'arrow' : null);
    const targetId = selectedNodeId || selectedArrowId;
    if (!targetType || !targetId || String(targetId).startsWith('temp-')) return;
    try {
      const response = await fetch('/api/mtcl/so-do/zorder', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          id_depart: chartState.idDepart,
          loai_so_do: chartState.loai,
          target_type: targetType,
          target_id: Number(targetId),
          action,
        }),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        alert(data.detail || 'Không sắp xếp được lớp');
        return;
      }
      if (targetType === 'node') {
        const node = (chartState.nodes || []).find((n) => String(n.id) === String(targetId));
        if (node) node.z_index = data.z_index;
        const el = document.querySelector(`.dv-pos[data-node-id="${targetId}"]`);
        if (el) el.style.zIndex = String(2 + data.z_index);
      } else {
        const arrow = (chartState.arrows || []).find((a) => String(a.id) === String(targetId));
        if (arrow) arrow.z_index = data.z_index;
      }
      drawLines();
    } catch (_) {
      alert('Không kết nối được máy chủ');
    }
  }

  async function performUndo() {
    if (!undoStack.length) return;
    const undoFn = undoStack.pop();
    updateUndoButton();
    try {
      await undoFn();
    } catch (_) { /* ignore */ }
  }

  async function deleteSelectedArrow() {
    if (!selectedArrowId || !chartState || !chartState.isAdmin) return;
    const arrowId = selectedArrowId;
    const arrow = (chartState.arrows || []).find((a) => String(a.id) === String(arrowId));
    try {
      const response = await fetch(
        `/api/mtcl/so-do/arrows/delete?id_depart=${chartState.idDepart}&loai_so_do=${chartState.loai}&arrow_id=${arrowId}`,
        { method: 'POST' }
      );
      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        alert(data.detail || 'Không xóa được mũi tên');
        return;
      }
      selectedArrowId = null;
      // Cập nhật trực tiếp trên canvas, không load lại cả sơ đồ.
      chartState.arrows = (chartState.arrows || []).filter((a) => String(a.id) !== String(arrowId));
      drawLines();
      if (arrow) {
        const { from_node_id, to_node_id, kind, shape_type, color, dash, x1, y1, x2, y2, points } = arrow;
        const style = { color, dash };
        const isFreeShape = Number(from_node_id) === 0 && Number(to_node_id) === 0;
        pushUndo(async () => {
          if (isFreeShape) {
            await saveShape(shape_type || 'line', x1, y1, x2, y2, style);
          } else {
            await saveLink(from_node_id, to_node_id, kind, { x1, y1, x2, y2 }, points, style);
          }
        });
      }
    } catch (_) {
      alert('Không kết nối được máy chủ');
    }
  }

  // Bấm ra ngoài mũi tên/khối đang chọn thì bỏ chọn.
  document.addEventListener('pointerdown', (event) => {
    if (!selectedArrowId && !selectedNodeId) return;
    const target = event.target;
    const onShape = target && target.closest && (
      target.closest('.dv-arrow-hit') || target.closest('.dv-arrow-handle') || target.closest('.dv-arrow-mid')
    );
    const onNode = target && target.closest && target.closest('.dv-pos');
    const onLegend = target && target.closest && target.closest('.dv-legend-panel');
    if (onShape || onNode || onLegend) return;
    selectedArrowId = null;
    selectedNodeId = null;
    document.querySelectorAll('.dv-pos').forEach((c) => c.classList.remove('is-node-selected'));
    drawLines();
  });

  // Phím Delete/Backspace xóa mũi tên hoặc khối đang chọn, Ctrl+Z hoàn tác thao tác gần nhất
  // (bỏ qua khi đang gõ trong ô nhập liệu).
  document.addEventListener('keydown', (event) => {
    const tag = (event.target && event.target.tagName) || '';
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return;
    if ((event.ctrlKey || event.metaKey) && !event.shiftKey && event.key.toLowerCase() === 'z') {
      if (!chartState || !chartState.isAdmin) return;
      event.preventDefault();
      performUndo();
      return;
    }
    if (event.key !== 'Delete' && event.key !== 'Backspace') return;
    if (selectedArrowId) {
      event.preventDefault();
      deleteSelectedArrow();
    } else if (selectedNodeId) {
      event.preventDefault();
      deleteNode(selectedNodeId);
    }
  });

  global.mtclRenderOrgCanvas = renderOrgCanvas;
  global.mtclMountOrgCanvas = mountOrgCanvas;
  global.mtclRedrawOrgCanvas = drawLines;
})(window);
