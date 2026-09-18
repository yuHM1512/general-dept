(() => {
  "use strict";

  const boot = window.PCCC_BOOTSTRAP || { user: {}, isAdmin: false };
  const app = document.getElementById("pccc-app");
  const toast = document.getElementById("toast");
  const adminHomeButton = document.getElementById("admin-home-button");
  const offlineBanner = document.getElementById("offline-banner");

  const BASE_PATH = "/internal-audit/pccc";

  const state = {
    screen: "loading",
    drill: null,
    units: [],
    unitId: null,
    detail: null,
    overview: null,
    baseline: {},
    actual: {},
    reasons: {},
    acknowledged: false,
    adminFilter: "ALL",
    assignments: [],
    drills: [],
  };

  function buildUrl() {
    if (!state.drill) return BASE_PATH;
    return `${BASE_PATH}/${state.drill.id}`;
  }

  function pushUrl(replace = false) {
    const url = buildUrl();
    if (url === location.pathname) return;
    if (replace) history.replaceState({ drillId: state.drill?.id, screen: state.screen }, "", url);
    else history.pushState({ drillId: state.drill?.id, screen: state.screen }, "", url);
  }

  const statusLabels = {
    NHAP: "Đang mở báo số",
    MO_SI_SO: "Đang mở báo số",
    DANG_KIEM_DEM: "Đang kiểm đếm",
    DA_KET_THUC: "Đã kết thúc",
  };

  const e = (value) => String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");

  const number = (value) => new Intl.NumberFormat("vi-VN").format(Number(value || 0));
  const dateText = (value) => value
    ? new Intl.DateTimeFormat("vi-VN", { day: "2-digit", month: "2-digit", year: "numeric" }).format(new Date(`${value}T00:00:00`))
    : "—";
  const TZ = "Asia/Ho_Chi_Minh";
  const timeText = (value) => {
    if (!value) return "—";
    const raw = String(value);
    const dt = new Date(raw.includes("T") && !raw.endsWith("Z") && !raw.includes("+") ? raw + "Z" : raw);
    return new Intl.DateTimeFormat("vi-VN", { hour: "2-digit", minute: "2-digit", day: "2-digit", month: "2-digit", year: "numeric", timeZone: TZ }).format(dt);
  };

  async function api(path, options = {}) {
    const headers = { ...(options.headers || {}) };
    if (options.body && !headers["Content-Type"]) headers["Content-Type"] = "application/json";
    const response = await fetch(path, { ...options, headers });
    const data = response.status === 204 ? null : await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data?.detail || "Không thể kết nối máy chủ");
    return data;
  }

  function showToast(message, type = "info") {
    toast.textContent = message;
    toast.className = `toast ${type === "error" ? "error" : ""}`;
    toast.hidden = false;
    clearTimeout(showToast.timer);
    showToast.timer = setTimeout(() => { toast.hidden = true; }, 3600);
  }

  function setBusy(button, busy, label = "Đang xử lý...") {
    if (!button) return;
    if (busy) {
      button.dataset.original = button.innerHTML;
      button.disabled = true;
      button.innerHTML = `<span class="material-symbols-outlined">progress_activity</span>${e(label)}`;
    } else {
      button.disabled = false;
      if (button.dataset.original) button.innerHTML = button.dataset.original;
    }
  }

  const draftKey = (mode) => `pccc:${mode}:${state.drill?.id || 0}:${state.unitId || 0}:${boot.user?.ma_nv || "user"}`;

  function saveLocalDraft(mode) {
    const payload = mode === "baseline"
      ? state.baseline
      : { actual: state.actual, reasons: state.reasons };
    localStorage.setItem(draftKey(mode), JSON.stringify({ savedAt: new Date().toISOString(), payload }));
    showToast("Đã lưu nháp trên thiết bị");
    render();
  }

  function restoreLocalDraft(mode) {
    try {
      const raw = JSON.parse(localStorage.getItem(draftKey(mode)) || "null");
      if (!raw?.payload) return null;
      return raw;
    } catch (_) {
      return null;
    }
  }

  function clearDrafts() {
    localStorage.removeItem(draftKey("baseline"));
    localStorage.removeItem(draftKey("actual"));
  }

  function drillIdFromUrl() {
    const match = location.pathname.match(/\/internal-audit\/pccc\/(\d+)/);
    return match ? Number(match[1]) : null;
  }

  async function init() {
    adminHomeButton.hidden = !boot.isAdmin;
    const requestedId = boot.drillId || drillIdFromUrl();
    try {
      const [drill, units, drills] = await Promise.all([
        api("/api/pccc/drills/active"),
        api("/api/pccc/units"),
        boot.isAdmin ? api("/api/pccc/drills") : api("/api/pccc/drills?active_only=true"),
      ]);
      state.units = units || [];
      state.drills = drills || [];
      if (requestedId) {
        state.drill = state.drills.find((d) => d.id === requestedId) || drill;
      } else {
        state.drill = drill;
      }
      if (boot.isAdmin) {
        state.screen = "admin";
        await loadAdmin(false);
      } else if (!state.drill) {
        state.screen = "no-drill";
        render();
      } else if (!state.units.length) {
        state.screen = "no-access";
        render();
      } else {
        state.unitId = state.units[0].id;
        await loadUnit("overview");
      }
      pushUrl(true);
    } catch (error) {
      state.screen = "error";
      renderError(error.message);
    }
  }

  async function loadUnit(screen = "overview") {
    if (!state.drill || !state.unitId) return;
    state.screen = "loading";
    render();
    try {
      state.detail = await api(`/api/pccc/drills/${state.drill.id}/units/${state.unitId}`);
      state.drill = state.detail.dot;
      state.baseline = Object.fromEntries(
        state.detail.records.map((row) => [row.bo_phan_id, row.si_so_dau_ngay ?? row.si_so_tham_khao ?? 0]),
      );
      state.actual = Object.fromEntries(
        state.detail.records.map((row) => [row.bo_phan_id, row.thuc_te_kiem_dem]),
      );
      state.reasons = Object.fromEntries(
        state.detail.records.map((row) => [row.bo_phan_id, { code: row.ma_ly_do || "", detail: row.ly_do_chi_tiet || "" }]),
      );
      state.screen = screen;
      pushUrl();
      render();
    } catch (error) {
      state.screen = "error";
      renderError(error.message);
    }
  }

  async function loadAdmin(showLoading = true) {
    try {
      state.drills = await api("/api/pccc/drills");
      if (!state.drill && state.drills.length) state.drill = state.drills[0];
      const picker = document.getElementById("drill-picker");
      if (picker) {
        picker.innerHTML = `<option value="">Chọn đợt diễn tập</option>${state.drills.map(drill => `<option value="${drill.id}">${e(drill.ten)} · ${dateText(drill.ngay_dien_tap)} · ${e(statusLabels[drill.trang_thai])}</option>`).join("")}`;
        picker.value = state.drill?.id || "";
      }
    } catch (error) { showToast(error.message, "error"); return; }
    if (!state.drill) {
      state.overview = null;
      state.screen = "admin";
      render();
      return;
    }
    if (showLoading) {
      state.screen = "loading";
      render();
    }
    try {
      state.overview = await api(`/api/pccc/drills/${state.drill.id}/overview`);
      state.drill = state.overview.dot;
      state.screen = "admin";
      pushUrl();
      render();
    } catch (error) {
      state.screen = "error";
      renderError(error.message);
    }
  }

  function statusBadge(record) {
    if (record.da_xac_nhan) return ["Đã xác nhận", "verified", "success"];
    if (record.thuc_te_kiem_dem === null) {
      return record.si_so_dau_ngay === null
        ? ["Chưa khai báo", "circle", "neutral"]
        : ["Đã có sĩ số", "schedule", "info"];
    }
    if (record.chenh_lech === 0) return ["Đủ", "check_circle", "success"];
    if (record.chenh_lech < 0) return [`Thiếu ${Math.abs(record.chenh_lech)}`, "warning", "error"];
    return [`Dư ${record.chenh_lech}`, "error", "warning"];
  }

  function badge(label, icon, kind) {
    return `<span class="status ${kind}"><span class="material-symbols-outlined fill">${icon}</span>${e(label)}</span>`;
  }

  function renderOverview() {
    const d = state.detail;
    const completed = d.records.filter((row) => row.thuc_te_kiem_dem !== null).length;
    const pct = d.department_count ? Math.round(completed * 100 / d.department_count) : 0;
    const unitPicker = state.units.length > 1
      ? `<select id="unit-picker" class="chip" aria-label="Chọn đơn vị">${state.units.map((unit) => `<option value="${unit.id}" ${unit.id === state.unitId ? "selected" : ""}>${e(unit.ten)}</option>`).join("")}</select>`
      : `<span class="chip"><span class="material-symbols-outlined">apartment</span>${e(d.don_vi.ten)}</span>`;
    const statusKind = d.dot.trang_thai === "DANG_KIEM_DEM" ? "success" : "primary";
    const drillPicker = !boot.isAdmin && state.drills.length > 1
      ? `<section class="drill-selector" aria-labelledby="drill-selector-title">
          <div class="drill-selector-head">
            <span class="drill-selector-icon material-symbols-outlined" aria-hidden="true">event_available</span>
            <div><div class="drill-selector-label" id="drill-selector-title">Chọn đợt diễn tập</div><div class="drill-selector-hint">Có ${state.drills.length} đợt đang mở — hãy chọn đúng đợt cần báo số</div></div>
          </div>
          <div class="drill-select-wrap">
            <select id="staff-drill-picker" class="drill-select" aria-label="Chọn đợt diễn tập">
              ${state.drills.map((drill) => `<option value="${drill.id}" ${drill.id === d.dot.id ? "selected" : ""}>${e(drill.ten)} — ${dateText(drill.ngay_dien_tap)}</option>`).join("")}
            </select>
            <span class="material-symbols-outlined" aria-hidden="true">expand_more</span>
          </div>
        </section>`
      : "";
    const rows = d.records.map((row) => {
      const [label, icon, kind] = statusBadge(row);
      const counts = row.si_so_dau_ngay === null
        ? `Tham khảo: ${number(row.si_so_tham_khao)}`
        : `Sĩ số: ${number(row.si_so_dau_ngay)}${row.thuc_te_kiem_dem !== null ? ` · Kiểm đếm: ${number(row.thuc_te_kiem_dem)}` : ""}`;
      const note = row.ly_do_chi_tiet ? `<div class="data-note">${e(row.ly_do_chi_tiet)}</div>` : "";
      return `<div class="data-row"><div class="data-copy"><div class="data-name">${e(row.bo_phan_ten)}</div><div class="data-meta">${counts}</div>${note}</div>${badge(label, icon, kind)}</div>`;
    }).join("");

    const isOpen = ["NHAP", "MO_SI_SO", "DANG_KIEM_DEM"].includes(d.dot.trang_thai);
    const baselineComplete = d.records.length > 0 && d.records.every((row) => row.si_so_dau_ngay !== null);
    const actualComplete = d.records.length > 0 && d.records.every((row) => row.thuc_te_kiem_dem !== null);
    let cta = `<button class="primary-button" disabled><span class="material-symbols-outlined">schedule</span>Chưa mở báo số</button>`;
    if (isOpen && (d.da_xac_nhan || actualComplete)) {
      cta = `<button class="primary-button" data-action="success"><span class="material-symbols-outlined">verified</span>Xem kết quả${d.da_xac_nhan ? " đã gửi" : ""}</button>`;
    } else if (isOpen) {
      cta = `<button class="secondary-button staff-action-button" data-action="baseline"><span class="material-symbols-outlined">group_add</span>Khai sĩ số đầu ngày</button><button class="primary-button staff-action-button" data-action="actual" ${baselineComplete ? "" : "disabled"} title="${baselineComplete ? "" : "Cần khai đủ sĩ số đầu ngày trước"}"><span class="material-symbols-outlined">fact_check</span>Kiểm đếm thực tế</button>`;
    } else if (d.dot.trang_thai === "DA_KET_THUC") {
      cta = `<button class="primary-button" data-action="success"><span class="material-symbols-outlined">history</span>Xem kết quả</button>`;
    }

    app.innerHTML = `<section class="screen">
      ${drillPicker}
      <div class="section-heading"><span class="eyebrow">Đợt đang hoạt động</span><h2>${e(d.dot.ten)}</h2></div>
      <div class="chip-row"><span class="chip"><span class="material-symbols-outlined">event</span>${dateText(d.dot.ngay_dien_tap)}</span>${unitPicker}<span class="chip ${statusKind}">${e(statusLabels[d.dot.trang_thai] || d.dot.trang_thai)}</span></div>
      <div class="hero-card"><div class="hero-label">Tiến độ kiểm đếm</div><div class="hero-value">${completed}/${d.department_count}</div><div class="hero-caption">bộ phận trong phạm vi của bạn đã kiểm đếm</div><div class="progress-track"><div class="progress-bar" style="width:${pct}%"></div></div></div>
      <div class="kpi-grid">
        <div class="kpi-card"><div class="kpi-label">Sĩ số</div><div class="kpi-value">${number(d.tong_si_so_dau_ngay)}</div><div class="kpi-note">đầu ngày</div></div>
        <div class="kpi-card"><div class="kpi-label">Kiểm đếm</div><div class="kpi-value success">${number(d.tong_thuc_te_kiem_dem)}</div><div class="kpi-note">thực tế</div></div>
        <div class="kpi-card"><div class="kpi-label">Chênh lệch</div><div class="kpi-value ${d.chenh_lech ? "error" : "success"}">${d.chenh_lech > 0 ? "+" : ""}${number(d.chenh_lech)}</div><div class="kpi-note">người</div></div>
      </div>
      <div class="list-title">Trạng thái bộ phận (${d.department_count})</div><div class="data-list">${rows || `<div class="notice info">Không có bộ phận trong phạm vi được phân quyền.</div>`}</div>
      <div class="sticky-actions"><div class="action-inner">${boot.isAdmin ? `<button class="secondary-button" data-action="admin"><span class="material-symbols-outlined">arrow_back</span>Dashboard</button>` : ""}${cta}</div></div>
    </section>`;
  }

  function baselineValues() {
    const local = restoreLocalDraft("baseline");
    if (local) state.baseline = { ...state.baseline, ...local.payload };
    return local;
  }

  function renderBaseline() {
    const draft = baselineValues();
    const total = Object.values(state.baseline).reduce((sum, value) => sum + Number(value || 0), 0);
    const cards = state.detail.records.map((row) => `<article class="entry-card">
      <div class="entry-head"><div><div class="entry-title">${e(row.bo_phan_ten)}</div><div class="entry-subtitle">Sĩ số tham khảo: ${number(row.si_so_tham_khao)}</div></div>${row.da_xac_nhan ? badge("Đã khóa", "lock", "neutral") : ""}</div>
      <div class="stepper">
        <button type="button" data-action="adjust-baseline" data-id="${row.bo_phan_id}" data-delta="-1" aria-label="Giảm sĩ số ${e(row.bo_phan_ten)}">−</button>
        <input class="number-input baseline-input" data-id="${row.bo_phan_id}" type="number" inputmode="numeric" min="0" value="${Number(state.baseline[row.bo_phan_id] ?? 0)}" aria-label="Sĩ số ${e(row.bo_phan_ten)}" ${row.da_xac_nhan ? "disabled" : ""}/>
        <button type="button" data-action="adjust-baseline" data-id="${row.bo_phan_id}" data-delta="1" aria-label="Tăng sĩ số ${e(row.bo_phan_ten)}">+</button>
      </div>
    </article>`).join("");
    app.innerHTML = `<section class="screen"><div class="sub-header"><div class="sub-header-line"><button class="icon-button" data-action="overview" aria-label="Quay lại"><span class="material-symbols-outlined">arrow_back</span></button><h2>Khai báo sĩ số đầu ngày</h2><button class="ghost-button" data-action="save-baseline-local">Lưu nháp</button></div><div class="total-strip"><span>Tổng sĩ số đang nhập</span><strong id="baseline-total">${number(total)}</strong></div></div>
      ${draft ? `<div class="notice success"><span class="material-symbols-outlined">save</span>Đã khôi phục bản nháp lưu lúc ${timeText(draft.savedAt)}</div>` : ""}
      <div class="entry-list">${cards}</div>
      <div class="sticky-actions"><div class="action-inner"><button class="primary-button" data-action="save-baseline"><span class="material-symbols-outlined">check_circle</span>Xác nhận sĩ số đầu ngày</button></div></div>
    </section>`;
  }

  function actualState(row) {
    const baseline = Number(state.baseline[row.bo_phan_id] ?? row.si_so_dau_ngay ?? 0);
    const raw = state.actual[row.bo_phan_id];
    const hasValue = raw !== null && raw !== undefined && raw !== "";
    const diff = hasValue ? Number(raw) - baseline : null;
    return { baseline, hasValue, diff };
  }

  function renderActual() {
    const local = restoreLocalDraft("actual");
    if (local) {
      state.actual = { ...state.actual, ...(local.payload.actual || {}) };
      state.reasons = { ...state.reasons, ...(local.payload.reasons || {}) };
    }
    const filled = state.detail.records.filter((row) => actualState(row).hasValue).length;
    const pct = state.detail.department_count ? Math.round(filled * 100 / state.detail.department_count) : 0;
    const cards = state.detail.records.map((row) => {
      const current = actualState(row);
      const reason = state.reasons[row.bo_phan_id] || {};
      const kind = current.diff === 0 ? "success" : current.diff < 0 ? "error" : current.diff > 0 ? "warning" : "";
      const label = current.diff === 0 ? "Đủ" : current.diff < 0 ? `Thiếu ${Math.abs(current.diff)}` : current.diff > 0 ? `Dư ${current.diff}` : "";
      const icon = current.diff === 0 ? "check_circle" : current.diff < 0 ? "warning" : "error";
      return `<article class="entry-card actual-card ${kind ? `state-${kind}` : ""}" data-card-id="${row.bo_phan_id}">
        <div class="entry-head"><div><div class="entry-title">${e(row.bo_phan_ten)}</div><div class="entry-subtitle">Sĩ số đầu ngày: ${number(current.baseline)}</div></div><span class="actual-status">${current.hasValue ? badge(label, icon, kind) : ""}</span></div>
        <div class="actual-line"><label for="actual-${row.bo_phan_id}">Thực tế</label><input id="actual-${row.bo_phan_id}" class="number-input actual-input" data-id="${row.bo_phan_id}" type="number" inputmode="numeric" min="0" value="${current.hasValue ? Number(state.actual[row.bo_phan_id]) : ""}" placeholder="0" ${row.da_xac_nhan ? "disabled" : ""}/><div class="difference ${kind}">${current.diff === null ? "—" : current.diff > 0 ? `+${current.diff}` : current.diff}</div></div>
        <div class="reason-panel" ${current.diff && current.diff !== 0 ? "" : "hidden"}><div class="reason-label"><span class="material-symbols-outlined">edit_note</span>Ghi chú chênh lệch</div><textarea class="quiet-input reason-detail" data-id="${row.bo_phan_id}" rows="3" placeholder="Nhập lý do dư hoặc thiếu...">${e(reason.detail || "")}</textarea></div>
      </article>`;
    }).join("");
    app.innerHTML = `<section class="screen"><div style="position:fixed;top:0;left:0;right:0;height:3px;background:var(--surface-high);z-index:70"><div style="height:100%;width:${pct}%;background:var(--tertiary);transition:width .2s" id="actual-progress"></div></div><div class="sub-header"><div class="sub-header-line"><button class="icon-button" data-action="overview" aria-label="Quay lại"><span class="material-symbols-outlined">arrow_back</span></button><div style="flex:1"><h2>Kiểm đếm thực tế</h2><div class="data-meta" id="actual-progress-text">${filled}/${state.detail.department_count} bộ phận đã nhập</div></div></div></div>
      ${local ? `<div class="notice success"><span class="material-symbols-outlined">save</span>Đã khôi phục bản nháp lưu lúc ${timeText(local.savedAt)}</div>` : ""}<div class="entry-list">${cards}</div>
      <div class="sticky-actions"><div class="action-inner"><button class="secondary-button" data-action="save-actual-local"><span class="material-symbols-outlined">save</span>Lưu nháp</button><button class="primary-button" data-action="review-actual"><span class="material-symbols-outlined">assignment_turned_in</span>Xác nhận kết quả</button></div></div></section>`;
  }

  function syncActualCard(id) {
    const row = state.detail.records.find((item) => item.bo_phan_id === id);
    const card = app.querySelector(`[data-card-id="${id}"]`);
    if (!row || !card) return;
    const current = actualState(row);
    const kind = current.diff === 0 ? "success" : current.diff < 0 ? "error" : current.diff > 0 ? "warning" : "";
    const label = current.diff === 0 ? "Đủ" : current.diff < 0 ? `Thiếu ${Math.abs(current.diff)}` : current.diff > 0 ? `Dư ${current.diff}` : "";
    const icon = current.diff === 0 ? "check_circle" : current.diff < 0 ? "warning" : "error";
    card.className = `entry-card actual-card ${kind ? `state-${kind}` : ""}`;
    card.querySelector(".actual-status").innerHTML = current.hasValue ? badge(label, icon, kind) : "";
    const diff = card.querySelector(".difference");
    diff.className = `difference ${kind}`;
    diff.textContent = current.diff === null ? "—" : current.diff > 0 ? `+${current.diff}` : String(current.diff);
    card.querySelector(".reason-panel").hidden = !(current.diff && current.diff !== 0);
    const filled = state.detail.records.filter((item) => actualState(item).hasValue).length;
    const pct = state.detail.department_count ? Math.round(filled * 100 / state.detail.department_count) : 0;
    const bar = document.getElementById("actual-progress");
    if (bar) bar.style.width = `${pct}%`;
    const text = document.getElementById("actual-progress-text");
    if (text) text.textContent = `${filled}/${state.detail.department_count} bộ phận đã nhập`;
  }

  function validateActual() {
    const missing = [];
    const unexplained = [];
    for (const row of state.detail.records) {
      const current = actualState(row);
      if (!current.hasValue) missing.push(row.bo_phan_ten);
      if (current.diff && current.diff !== 0) {
        const reason = state.reasons[row.bo_phan_id] || {};
        if (!String(reason.detail || "").trim()) unexplained.push(row.bo_phan_ten);
      }
    }
    if (missing.length) throw new Error(`Chưa nhập kiểm đếm: ${missing.join(", ")}`);
    if (unexplained.length) throw new Error(`Chưa giải trình đầy đủ: ${unexplained.join(", ")}`);
  }

  function actualItems(onlyFilled = false) {
    return state.detail.records
      .filter((row) => !onlyFilled || actualState(row).hasValue)
      .map((row) => ({
        bo_phan_id: row.bo_phan_id,
        so_luong: Number(state.actual[row.bo_phan_id]),
        ma_ly_do: "",
        ly_do_chi_tiet: String(state.reasons[row.bo_phan_id]?.detail || "").trim(),
      }));
  }

  function renderConfirm() {
    const records = state.detail.records.map((row) => ({ row, ...actualState(row) }));
    const baselineTotal = records.reduce((sum, item) => sum + item.baseline, 0);
    const actualTotal = records.reduce((sum, item) => sum + Number(state.actual[item.row.bo_phan_id] || 0), 0);
    const diff = actualTotal - baselineTotal;
    const diffs = records.filter((item) => item.diff !== 0).map((item) => {
      const reason = state.reasons[item.row.bo_phan_id] || {};
      return `<div class="data-row"><div class="data-copy"><div class="data-name">${e(item.row.bo_phan_ten)}</div><div class="data-meta">${e(reason.detail)}</div></div>${badge(item.diff < 0 ? `Thiếu ${Math.abs(item.diff)}` : `Dư ${item.diff}`, item.diff < 0 ? "warning" : "error", item.diff < 0 ? "error" : "warning")}</div>`;
    }).join("");
    app.innerHTML = `<section class="screen"><div class="sub-header"><div class="sub-header-line"><button class="icon-button" data-action="actual" aria-label="Quay lại"><span class="material-symbols-outlined">arrow_back</span></button><h2>Xác nhận kết quả</h2></div></div>
      <div class="confirm-card"><div class="list-title" style="margin-top:0">Tóm tắt kết quả</div><div class="summary-grid"><div><div class="summary-label">Sĩ số đầu ngày</div><div class="summary-value" style="color:var(--primary)">${number(baselineTotal)}</div></div><div><div class="summary-label">Thực tế kiểm đếm</div><div class="summary-value" style="color:var(--secondary)">${number(actualTotal)}</div></div><div><div class="summary-label">Chênh lệch</div><div class="summary-value" style="color:${diff ? "var(--error)" : "var(--secondary)"}">${diff > 0 ? "+" : ""}${number(diff)}</div></div><div><div class="summary-label">Hoàn thành</div><div class="summary-value">${records.length}/${records.length}</div></div></div></div>
      ${diffs ? `<div class="confirm-card"><div class="list-title" style="margin-top:0">Bộ phận có chênh lệch</div><div class="data-list">${diffs}</div></div>` : ""}
      <label class="confirm-card acknowledgement"><input id="acknowledge" type="checkbox" ${state.acknowledged ? "checked" : ""}/><span>Tôi xác nhận số liệu kiểm đếm trong phạm vi được phân quyền là chính xác.</span></label>
      <div class="sticky-actions"><div class="action-inner"><button id="confirm-final" class="primary-button" data-action="confirm-final" ${state.acknowledged ? "" : "disabled"}><span class="material-symbols-outlined">send</span>Hoàn tất kiểm đếm</button></div></div></section>`;
  }

  function renderSuccess() {
    const d = state.detail;
    app.innerHTML = `<section class="success-state screen"><div class="success-icon"><span class="material-symbols-outlined fill">check_circle</span></div><h2>Hoàn tất kiểm đếm</h2><p>${e(d.don_vi.ten)} · ${e(d.dot.ten)}<br/>Đã gửi lúc ${timeText(d.xac_nhan_at || new Date().toISOString())}</p><div class="confirm-card" style="margin:24px 0;text-align:left"><div class="summary-grid"><div><div class="summary-label">Sĩ số đầu ngày</div><div class="summary-value" style="color:var(--primary)">${number(d.tong_si_so_dau_ngay)}</div></div><div><div class="summary-label">Thực tế</div><div class="summary-value" style="color:var(--secondary)">${number(d.tong_thuc_te_kiem_dem)}</div></div><div><div class="summary-label">Chênh lệch</div><div class="summary-value" style="color:${d.chenh_lech ? "var(--error)" : "var(--secondary)"}">${d.chenh_lech > 0 ? "+" : ""}${number(d.chenh_lech)}</div></div><div><div class="summary-label">Hoàn thành</div><div class="summary-value">${d.completed_count}/${d.department_count}</div></div></div></div><button class="secondary-button" data-action="overview">Quay về tổng quan</button></section>`;
  }

  function overviewStatus(item) {
    const mapping = {
      DA_XAC_NHAN: ["Đã xác nhận", "verified", "success"],
      CO_CHENH_LECH: ["Có chênh lệch", "warning", "error"],
      DU: ["Đủ", "check_circle", "success"],
      DANG_KIEM_DEM: ["Đang kiểm đếm", "pending", "info"],
      DA_CO_SI_SO: ["Có sĩ số", "schedule", "neutral"],
      CHUA_KHAI_BAO: ["Chưa khai báo", "circle", "neutral"],
    };
    return mapping[item.trang_thai] || [item.trang_thai, "circle", "neutral"];
  }

  async function loadAssignments() {
    try {
      const [assignments, units] = await Promise.all([api("/api/pccc/assignments"), api("/api/pccc/units")]);
      state.assignments = assignments;
      state.units = units;
      state.screen = "assignments";
      renderAssignments();
    } catch (error) { showToast(error.message, "error"); }
  }

  function renderAssignments() {
    updateAdminNavigation("assignments");
    const rows = state.assignments.filter(row => row.active).map(row => {
      const unit = state.units.find(unit => unit.id === row.don_vi_id);
      return `<article class="entry-card"><div class="entry-title">${e(row.ho_ten || row.ma_nv)}</div><p>Mã nhân viên: <strong>${e(row.ma_nv)}</strong><br>Đơn vị: ${e(unit?.ten || row.don_vi_id)}${row.bo_phan_ten ? `<br>Bộ phận: <strong>${e(row.bo_phan_ten)}</strong>` : "<br>Phạm vi: Toàn đơn vị"}<br>${row.vai_tro === "CHINH" ? "Phụ trách chính" : "Dự phòng"}</p><button class="secondary-button" data-action="revoke-assignment" data-id="${row.id}">Ngừng phân công</button></article>`;
    }).join("");
    app.innerHTML = `<section class="screen"><div class="section-heading"><h2>Phân công nhân sự</h2><p>Mã nhân viên là mã dùng để đăng nhập. Chỉ nhân sự được phân công mới được báo số trong phạm vi đơn vị/bộ phận của mình.</p></div>
      <form id="assignment-form" class="entry-card empty-form">
        <label>Mã nhân viên<input class="quiet-input" name="ma_nv" maxlength="16" required autocomplete="off" placeholder="Nhập mã nhân viên hiện có"></label>
        <button class="secondary-button" type="button" data-action="lookup-employee">Tra cứu nhân viên</button>
        <p id="employee-preview" role="status">Tra cứu để xem tên và đơn vị trước khi phân công.</p>
        <label>Đơn vị phụ trách<select id="assignment-unit" class="quiet-input" name="don_vi_id" required><option value="">Chọn đơn vị</option>${state.units.map(unit => `<option value="${unit.id}">${e(unit.ten)}</option>`).join("")}</select></label>
        <label>Bộ phận báo số<select id="assignment-department" class="quiet-input" name="bo_phan_id" disabled><option value="">Toàn đơn vị</option></select></label>
        <label>Vai trò<select class="quiet-input" name="vai_tro"><option value="CHINH">Phụ trách chính</option><option value="DU_PHONG">Dự phòng</option></select></label>
        <p>Có thể phân công toàn đơn vị hoặc từng bộ phận. Lưu vào cùng phạm vi và vai trò sẽ thay thế người đang phụ trách.</p>
        <button class="primary-button" type="submit">Lưu phân công</button>
      </form><h3>Nhân sự đang được phân công</h3><div class="entry-list">${rows || '<p>Chưa có nhân sự được phân công.</p>'}</div></section>`;
  }

  function syncAssignmentDepartments() {
    const unitSelect = document.getElementById("assignment-unit");
    const departmentSelect = document.getElementById("assignment-department");
    if (!unitSelect || !departmentSelect) return;
    const unit = state.units.find(item => item.id === Number(unitSelect.value));
    const departments = unit?.bo_phans || [];
    departmentSelect.innerHTML = `<option value="">Toàn đơn vị</option>${departments.map(item => `<option value="${item.id}">${e(item.ten)}</option>`).join("")}`;
    departmentSelect.disabled = !unit;
  }

  function updateAdminNavigation(screen) {
    const selected = screen === "assignments" ? "assignments-nav" : screen === "new-drill" ? "new-drill-nav" : "drills-nav";
    for (const id of ["drills-nav", "new-drill-nav", "assignments-nav"]) {
      const button = document.getElementById(id);
      if (id === selected) button?.setAttribute("aria-current", "page");
      else button?.removeAttribute("aria-current");
    }
  }

  function renderAdmin() {
    updateAdminNavigation(state.screen);
    if (!state.drill || state.screen === "new-drill") {
      const today = new Date().toISOString().slice(0, 10);
      app.innerHTML = `<section class="empty-state screen"><div class="empty-icon"><span class="material-symbols-outlined">emergency_home</span></div><h2>Chưa có đợt diễn tập</h2><p>Tạo đợt mới để mở khai báo sĩ số cho các đơn vị.</p><form id="create-drill-form" class="empty-form"><label>Tên đợt<input class="quiet-input" name="ten" value="Diễn tập PCCC ${today.slice(0, 4)}" required/></label><label>Ngày diễn tập<input class="quiet-input" name="ngay_dien_tap" type="date" value="${today}" required/></label><label>Giờ dự kiến<input class="quiet-input" name="bat_dau_du_kien" type="time"/></label><button class="primary-button" type="submit"><span class="material-symbols-outlined">add_circle</span>Tạo đợt diễn tập</button></form></section>`;
      if (state.screen === "new-drill") {
        app.querySelector("h2").textContent = "Tạo đợt diễn tập mới";
        app.querySelector("p").textContent = "Tạo xong là các đơn vị có thể khai sĩ số đầu ngày và kiểm đếm thực tế ngay. Hãy kết thúc đợt cũ để tránh chọn nhầm.";
      }
      return;
    }
    const o = state.overview;
    if (!o) return;
    const units = o.units.filter((item) => state.adminFilter === "ALL" || item.trang_thai === state.adminFilter);
    const rows = units.map((item) => {
      const [label, icon, kind] = overviewStatus(item);
      return `<button class="data-row clickable" data-action="admin-unit" data-id="${item.don_vi_id}" type="button"><div class="data-copy" style="text-align:left"><div class="data-name">${e(item.don_vi_ten)}</div><div class="data-meta">Sĩ số: ${number(item.tong_si_so_dau_ngay)} · Kiểm đếm: ${number(item.tong_thuc_te_kiem_dem)}${item.xac_nhan_at ? ` · ${timeText(item.xac_nhan_at)}` : ""}</div></div>${badge(label, icon, kind)}</button>`;
    }).join("");
    app.innerHTML = `<section class="screen"><div class="section-heading"><span class="eyebrow">Admin</span><h2>Dashboard kiểm đếm</h2><p>${e(o.dot.ten)} · ${dateText(o.dot.ngay_dien_tap)}</p></div><div class="admin-toolbar"><select id="admin-filter" class="quiet-input"><option value="ALL">Tất cả trạng thái</option><option value="CHUA_KHAI_BAO">Chưa khai báo</option><option value="DA_CO_SI_SO">Có sĩ số</option><option value="DANG_KIEM_DEM">Đang kiểm đếm</option><option value="CO_CHENH_LECH">Có chênh lệch</option><option value="DA_XAC_NHAN">Đã xác nhận</option></select><span class="chip ${o.dot.trang_thai === "DANG_KIEM_DEM" ? "success" : "primary"}">${e(statusLabels[o.dot.trang_thai])}</span></div>
      <div class="admin-layout"><div class="kpi-grid admin-kpis"><div class="kpi-card"><div class="kpi-label">Tổng sĩ số</div><div class="kpi-value">${number(o.tong_si_so_dau_ngay)}</div></div><div class="kpi-card"><div class="kpi-label">Thực tế</div><div class="kpi-value success">${number(o.tong_thuc_te_kiem_dem)}</div></div><div class="kpi-card"><div class="kpi-label">Chênh lệch</div><div class="kpi-value ${o.chenh_lech ? "error" : "success"}">${o.chenh_lech > 0 ? "+" : ""}${number(o.chenh_lech)}</div></div><div class="kpi-card"><div class="kpi-label">Hoàn tất</div><div class="kpi-value">${o.unit_confirmed_count}/${o.unit_count}</div></div></div><div><div class="list-title" style="margin-top:0">Đơn vị (${units.length}/${o.unit_count})</div><div class="data-list admin-list">${rows || `<div class="notice info">Không có đơn vị ở trạng thái đã chọn.</div>`}</div></div></div>
      <div class="sticky-actions"><div class="action-inner"><button class="secondary-button" data-action="export"><span class="material-symbols-outlined">download</span>Xuất CSV</button>${o.dot.trang_thai === "DA_KET_THUC" ? `<button class="primary-button" disabled><span class="material-symbols-outlined">verified</span>Đợt đã kết thúc</button>` : `<button class="primary-button" disabled><span class="material-symbols-outlined">sensors</span>Đợt đang mở</button>`}</div></div></section>`;
    const filter = document.getElementById("admin-filter");
    if (filter) filter.value = state.adminFilter;
    if (["NHAP", "MO_SI_SO", "DANG_KIEM_DEM"].includes(o.dot.trang_thai)) {
      app.querySelector(".section-heading").insertAdjacentHTML("afterend", `<div class="drill-action-row"><button class="admin-end-button" type="button" data-action="drill-status" data-status="DA_KET_THUC"><span class="material-symbols-outlined" aria-hidden="true">stop_circle</span>Kết thúc</button><button class="ghost-button" type="button" data-action="edit-drill"><span class="material-symbols-outlined" aria-hidden="true">edit</span>Sửa</button><button class="ghost-button danger" type="button" data-action="delete-drill"><span class="material-symbols-outlined" aria-hidden="true">delete</span>Xóa</button></div>`);
    }
  }

  function renderEditDrill() {
    updateAdminNavigation("drills");
    const d = state.drill;
    const dateVal = d.ngay_dien_tap;
    const timeVal = d.bat_dau_du_kien ? new Date(d.bat_dau_du_kien).toTimeString().slice(0, 5) : "";
    app.innerHTML = `<section class="screen"><div class="sub-header"><div class="sub-header-line"><button class="icon-button" data-action="admin" aria-label="Quay lại"><span class="material-symbols-outlined">arrow_back</span></button><h2>Sửa thông tin đợt</h2></div></div>
      <form id="edit-drill-form" class="empty-form"><label>Tên đợt<input class="quiet-input" name="ten" value="${e(d.ten)}" required/></label><label>Ngày diễn tập<input class="quiet-input" name="ngay_dien_tap" type="date" value="${dateVal}" required/></label><label>Giờ dự kiến<input class="quiet-input" name="bat_dau_du_kien" type="time" value="${timeVal}"/></label><label>Ghi chú<textarea class="quiet-input" name="ghi_chu" rows="3">${e(d.ghi_chu)}</textarea></label><div class="action-inner"><button class="secondary-button" type="button" data-action="admin">Hủy</button><button class="primary-button" type="submit"><span class="material-symbols-outlined">save</span>Lưu thay đổi</button></div></form></section>`;
  }

  function renderEmpty(kind) {
    const data = kind === "no-access"
      ? ["lock_person", "Chưa được phân công", "Bạn cần được admin phân công báo số cho đơn vị phù hợp với hồ sơ nhân sự. Vui lòng liên hệ quản trị viên."]
      : ["event_busy", "Chưa có đợt đang hoạt động", "Khi quản trị viên tạo và mở đợt diễn tập, bạn có thể khai báo sĩ số tại đây."];
    app.innerHTML = `<section class="empty-state screen"><div class="empty-icon"><span class="material-symbols-outlined">${data[0]}</span></div><h2>${data[1]}</h2><p>${data[2]}</p><a href="/internal-audit" class="secondary-button" style="text-decoration:none">Quay lại Đánh giá nội bộ</a></section>`;
  }

  function renderError(message) {
    app.innerHTML = `<section class="empty-state screen"><div class="empty-icon"><span class="material-symbols-outlined">cloud_off</span></div><h2>Không tải được dữ liệu</h2><p>${e(message)}</p><button class="primary-button" data-action="retry">Thử lại</button></section>`;
  }

  function render() {
    if (state.screen === "loading") return;
    if (state.screen === "overview") renderOverview();
    else if (state.screen === "baseline") renderBaseline();
    else if (state.screen === "actual") renderActual();
    else if (state.screen === "confirm") renderConfirm();
    else if (state.screen === "success") renderSuccess();
    else if (state.screen === "admin") renderAdmin();
    else if (state.screen === "new-drill") renderAdmin();
    else if (state.screen === "edit-drill") renderEditDrill();
    else if (state.screen === "assignments") renderAssignments();
    else if (["no-access", "no-drill"].includes(state.screen)) renderEmpty(state.screen);
    scheduleRefresh();
  }

  async function saveBaseline(button) {
    const items = state.detail.records.map((row) => ({
      bo_phan_id: row.bo_phan_id,
      so_luong: Math.max(0, Number(state.baseline[row.bo_phan_id] ?? 0)),
    }));
    setBusy(button, true);
    try {
      state.detail = await api(`/api/pccc/drills/${state.drill.id}/units/${state.unitId}/baseline`, { method: "PUT", body: JSON.stringify({ items }) });
      localStorage.removeItem(draftKey("baseline"));
      state.screen = "overview";
      showToast("Đã cập nhật sĩ số đầu ngày");
      render();
    } catch (error) {
      showToast(error.message, "error");
      setBusy(button, false);
    }
  }

  async function saveActual(button, review) {
    try {
      if (review) validateActual();
      const items = actualItems(!review);
      if (!items.length) throw new Error("Chưa có dữ liệu kiểm đếm để lưu");
      setBusy(button, true);
      state.detail = await api(`/api/pccc/drills/${state.drill.id}/units/${state.unitId}/actual`, { method: "PUT", body: JSON.stringify({ items }) });
      localStorage.removeItem(draftKey("actual"));
      state.screen = review ? "confirm" : "actual";
      showToast(review ? "Đã lưu kết quả, vui lòng xác nhận" : "Đã lưu nháp kiểm đếm");
      render();
    } catch (error) {
      showToast(error.message, "error");
      setBusy(button, false);
    }
  }

  async function confirmFinal(button) {
    setBusy(button, true, "Đang gửi...");
    try {
      await api(`/api/pccc/drills/${state.drill.id}/units/${state.unitId}/confirm`, { method: "POST", body: JSON.stringify({ ghi_chu: "" }) });
      clearDrafts();
      state.detail = await api(`/api/pccc/drills/${state.drill.id}/units/${state.unitId}`);
      state.screen = "success";
      render();
    } catch (error) {
      showToast(error.message, "error");
      setBusy(button, false);
    }
  }

  async function deleteDrill(button) {
    if (!state.drill) return;
    if (!window.confirm(`Xóa đợt "${state.drill.ten}"?\nChỉ xóa được đợt chưa có dữ liệu sĩ số/kiểm đếm.`)) return;
    setBusy(button, true);
    try {
      await api(`/api/pccc/drills/${state.drill.id}`, { method: "DELETE" });
      state.drill = null;
      state.overview = null;
      await loadAdmin(false);
      showToast("Đã xóa đợt diễn tập");
    } catch (error) {
      showToast(error.message, "error");
      setBusy(button, false);
    }
  }

  async function changeDrillStatus(button, status) {
    const risky = status === "DANG_KIEM_DEM" || status === "DA_KET_THUC";
    if (risky && !window.confirm(status === "DA_KET_THUC" ? "Kết thúc đợt diễn tập? Dữ liệu sẽ chuyển sang chỉ đọc." : "Bắt đầu giai đoạn kiểm đếm thực tế?")) return;
    setBusy(button, true);
    try {
      state.drill = await api(`/api/pccc/drills/${state.drill.id}/status`, { method: "PATCH", body: JSON.stringify({ trang_thai: status }) });
      await loadAdmin(false);
      showToast("Đã cập nhật trạng thái đợt diễn tập");
    } catch (error) {
      showToast(error.message, "error");
      setBusy(button, false);
    }
  }

  function exportOverview() {
    if (!state.overview) return;
    const rows = [["Đơn vị", "Sĩ số đầu ngày", "Thực tế kiểm đếm", "Chênh lệch", "Trạng thái"]];
    state.overview.units.forEach((item) => rows.push([item.don_vi_ten, item.tong_si_so_dau_ngay, item.tong_thuc_te_kiem_dem, item.chenh_lech, item.trang_thai]));
    const csv = "\ufeff" + rows.map((row) => row.map((cell) => `"${String(cell ?? "").replaceAll('"', '""')}"`).join(",")).join("\r\n");
    const url = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = `kiem-dem-pccc-${state.drill.ngay_dien_tap}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  }

  app.addEventListener("click", async (event) => {
    const target = event.target.closest("[data-action]");
    if (!target) return;
    const action = target.dataset.action;
    if (action === "lookup-employee") {
      const form = document.getElementById("assignment-form");
      const code = form.elements.ma_nv.value.trim().toUpperCase();
      if (!code) { showToast("Vui lòng nhập mã nhân viên", "error"); return; }
      setBusy(target, true);
      try {
        const employee = await api(`/api/pccc/employees/${encodeURIComponent(code)}`);
        if (form.elements.ma_nv.value.trim().toUpperCase() !== code) return;
        document.getElementById("employee-preview").textContent = `${employee.ho_ten} · ${employee.ma_nv} · ${employee.don_vi}${employee.bo_phan ? ` / ${employee.bo_phan}` : ""}`;
        form.elements.don_vi_id.value = employee.unit_ids.length === 1 ? employee.unit_ids[0] : "";
        syncAssignmentDepartments();
      } catch (error) { document.getElementById("employee-preview").textContent = error.message; }
      finally { setBusy(target, false); }
      return;
    }
    if (action === "revoke-assignment") {
      if (!window.confirm("Ngừng phân công nhân sự này?")) return;
      setBusy(target, true);
      try { await api(`/api/pccc/assignments/${target.dataset.id}`, { method: "DELETE" }); await loadAssignments(); showToast("Đã ngừng phân công"); }
      catch (error) { showToast(error.message, "error"); setBusy(target, false); }
      return;
    }
    if (action === "overview") { state.screen = "overview"; render(); }
    else if (action === "baseline") { state.screen = "baseline"; render(); }
    else if (action === "actual") { state.screen = "actual"; render(); }
    else if (action === "success") { state.screen = "success"; render(); }
    else if (action === "admin") await loadAdmin();
    else if (action === "save-baseline-local") saveLocalDraft("baseline");
    else if (action === "save-actual-local") await saveActual(target, false);
    else if (action === "save-baseline") await saveBaseline(target);
    else if (action === "review-actual") await saveActual(target, true);
    else if (action === "confirm-final") await confirmFinal(target);
    else if (action === "adjust-baseline") {
      const id = Number(target.dataset.id);
      const delta = Number(target.dataset.delta);
      state.baseline[id] = Math.max(0, Number(state.baseline[id] || 0) + delta);
      const input = app.querySelector(`.baseline-input[data-id="${id}"]`);
      if (input) input.value = state.baseline[id];
      const total = Object.values(state.baseline).reduce((sum, value) => sum + Number(value || 0), 0);
      document.getElementById("baseline-total").textContent = number(total);
    } else if (action === "admin-unit") {
      state.unitId = Number(target.dataset.id);
      await loadUnit("overview");
    } else if (action === "edit-drill") { state.screen = "edit-drill"; render(); }
    else if (action === "delete-drill") await deleteDrill(target);
    else if (action === "drill-status") await changeDrillStatus(target, target.dataset.status);
    else if (action === "export") exportOverview();
    else if (action === "retry") { state.screen = "loading"; app.innerHTML = `<section class="loading-state"><div class="skeleton skeleton-hero"></div></section>`; await init(); }
  });

  app.addEventListener("input", (event) => {
    if (event.target.matches(".baseline-input")) {
      state.baseline[Number(event.target.dataset.id)] = Math.max(0, Number(event.target.value || 0));
      const total = Object.values(state.baseline).reduce((sum, value) => sum + Number(value || 0), 0);
      document.getElementById("baseline-total").textContent = number(total);
    } else if (event.target.matches(".actual-input")) {
      const id = Number(event.target.dataset.id);
      state.actual[id] = event.target.value === "" ? null : Math.max(0, Number(event.target.value));
      syncActualCard(id);
    } else if (event.target.matches(".reason-detail")) {
      const id = Number(event.target.dataset.id);
      state.reasons[id] = { ...(state.reasons[id] || {}), detail: event.target.value };
    }
  });

  app.addEventListener("change", async (event) => {
    if (event.target.id === "unit-picker") {
      state.unitId = Number(event.target.value);
      await loadUnit("overview");
    } else if (event.target.id === "staff-drill-picker") {
      const selected = state.drills.find((item) => item.id === Number(event.target.value));
      if (!selected || selected.id === state.drill?.id) return;
      state.drill = selected;
      pushUrl();
      await loadUnit("overview");
    } else if (event.target.id === "assignment-unit") {
      syncAssignmentDepartments();
    } else if (event.target.id === "acknowledge") {
      state.acknowledged = event.target.checked;
      document.getElementById("confirm-final").disabled = !state.acknowledged;
    } else if (event.target.id === "admin-filter") {
      state.adminFilter = event.target.value;
      renderAdmin();
    }
  });

  app.addEventListener("submit", async (event) => {
    if (event.target.id === "assignment-form") {
      event.preventDefault();
      const form = new FormData(event.target);
      const button = event.target.querySelector('button[type="submit"]');
      setBusy(button, true);
      try {
        await api("/api/pccc/assignments", { method: "PUT", body: JSON.stringify({
          ma_nv: String(form.get("ma_nv")).trim().toUpperCase(),
          don_vi_id: Number(form.get("don_vi_id")),
          bo_phan_id: form.get("bo_phan_id") ? Number(form.get("bo_phan_id")) : null,
          vai_tro: form.get("vai_tro"), active: true,
        }) });
        await loadAssignments();
        showToast("Đã lưu phân công");
      } catch (error) { showToast(error.message, "error"); setBusy(button, false); }
      return;
    }
    if (event.target.id === "edit-drill-form") {
      event.preventDefault();
      const button = event.target.querySelector("button[type=submit]");
      const form = new FormData(event.target);
      const date = form.get("ngay_dien_tap");
      const time = form.get("bat_dau_du_kien");
      const payload = {
        ten: form.get("ten"),
        ngay_dien_tap: date,
        bat_dau_du_kien: time ? `${date}T${time}:00` : null,
        ghi_chu: form.get("ghi_chu") || "",
      };
      setBusy(button, true);
      try {
        state.drill = await api(`/api/pccc/drills/${state.drill.id}`, { method: "PATCH", body: JSON.stringify(payload) });
        await loadAdmin(false);
        showToast("Đã cập nhật thông tin đợt");
      } catch (error) {
        showToast(error.message, "error");
        setBusy(button, false);
      }
      return;
    }
    if (event.target.id !== "create-drill-form") return;
    event.preventDefault();
    const button = event.target.querySelector("button[type=submit]");
    const form = new FormData(event.target);
    const date = form.get("ngay_dien_tap");
    const time = form.get("bat_dau_du_kien");
    const payload = {
      ten: form.get("ten"),
      ngay_dien_tap: date,
      bat_dau_du_kien: time ? `${date}T${time}:00` : null,
      ghi_chu: "",
    };
    setBusy(button, true);
    try {
      state.drill = await api("/api/pccc/drills", { method: "POST", body: JSON.stringify(payload) });
      pushUrl();
      await loadAdmin(false);
      showToast("Đã tạo đợt diễn tập");
    } catch (error) {
      showToast(error.message, "error");
      setBusy(button, false);
    }
  });

  adminHomeButton.addEventListener("click", () => loadAdmin());
  document.getElementById("assignments-nav")?.addEventListener("click", loadAssignments);
  document.getElementById("drills-nav")?.addEventListener("click", () => loadAdmin());
  document.getElementById("new-drill-nav")?.addEventListener("click", () => {
    state.screen = "new-drill";
    render();
  });
  document.getElementById("drill-picker")?.addEventListener("change", async (event) => {
    const selected = state.drills.find((drill) => drill.id === Number(event.target.value));
    if (!selected) return;
    state.drill = selected;
    state.detail = null;
    state.adminFilter = "ALL";
    await loadAdmin();
  });
  window.addEventListener("offline", () => { offlineBanner.hidden = false; });
  window.addEventListener("online", () => { offlineBanner.hidden = true; showToast("Đã kết nối lại mạng"); });
  offlineBanner.hidden = navigator.onLine;

  const REFRESH_INTERVAL = 30_000;
  let refreshTimer = null;

  function scheduleRefresh() {
    clearTimeout(refreshTimer);
    const liveScreens = new Set(["admin", "overview", "success"]);
    if (!liveScreens.has(state.screen) || !state.drill) return;
    refreshTimer = setTimeout(autoRefresh, REFRESH_INTERVAL);
  }

  async function autoRefresh() {
    if (document.hidden || !navigator.onLine) { scheduleRefresh(); return; }
    try {
      if (state.screen === "admin" && state.drill) {
        state.overview = await api(`/api/pccc/drills/${state.drill.id}/overview`);
        state.drill = state.overview.dot;
        renderAdmin();
      } else if ((state.screen === "overview" || state.screen === "success") && state.drill && state.unitId) {
        state.detail = await api(`/api/pccc/drills/${state.drill.id}/units/${state.unitId}`);
        state.drill = state.detail.dot;
        render();
      }
    } catch (_) { /* silent — next tick retries */ }
    scheduleRefresh();
  }

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) scheduleRefresh();
  });

  window.addEventListener("popstate", async (event) => {
    const drillId = event.state?.drillId || drillIdFromUrl();
    if (drillId && drillId !== state.drill?.id) {
      const target = state.drills.find((d) => d.id === drillId);
      if (target) {
        state.drill = target;
        if (boot.isAdmin) await loadAdmin(false);
        else if (state.unitId) await loadUnit("overview");
      }
    } else if (!drillId) {
      if (boot.isAdmin) {
        state.drill = state.drills[0] || null;
        await loadAdmin(false);
      }
    }
  });

  init();
})();
