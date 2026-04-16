// FILE: web/static/js/aap_settings/sending_campaign.js
// DATE: 2026-01-21
// PURPOSE: Campaigns add/edit: окно отправки + валидация формы на submit.
// CHANGE:
// - Валидация под flow-форму: required для sending_list + mailbox (без template).
// - Ничего в логике окон/дропдаунов/сериализации не менялось.

(function () {
  const form = document.getElementById("yyCampaignForm");
  const ta = document.getElementById("yyValueJson"); // name="window"
  const taGlobal = document.getElementById("yyGlobalWindowJson");
  const body = document.getElementById("yySendingBody");
  const cb = document.getElementById("yyUseGlobalWindow");
  if (!form || !ta || !taGlobal || !body || !cb) return;

  // --- FIX: remember last clicked submit action (Enter submit has no submitter) ---
  let lastAction = "";
  form.addEventListener("click", function (e) {
    const b = e.target && e.target.closest ? e.target.closest('button[type="submit"][name="action"]') : null;
    if (!b) return;
    lastAction = String(b.value || "").trim();
  });

  const DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun", "hol"];

  const INPUT_BASE =
    "w-14 text-center px-1 py-1 rounded-md border border-[#71d0f4] bg-white placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-[#71d0f4]/10 focus:border-[#71d0f4]";

  function pad2(v) {
    const s = String(v == null ? "" : v).trim();
    if (!s) return "";
    if (!/^\d+$/.test(s)) return s;
    const n = parseInt(s, 10);
    if (Number.isNaN(n)) return s;
    return n < 10 ? "0" + String(n) : String(n);
  }

  function toInt(v) {
    const s = String(v == null ? "" : v).trim();
    if (!s) return null;
    if (!/^\d+$/.test(s)) return null;
    const n = parseInt(s, 10);
    return Number.isNaN(n) ? null : n;
  }

  function parseHHMM(s) {
    const m = String(s || "").trim().match(/^(\d{1,2})\s*:\s*(\d{1,2})$/);
    if (!m) return null;
    const hh = toInt(m[1]);
    const mm = toInt(m[2]);
    if (hh == null || mm == null) return null;
    if (hh < 0 || hh > 23) return null;
    if (mm < 0 || mm > 59) return null;
    return { hh, mm };
  }

  function toMinutes(hh, mm) {
    if (hh == null || mm == null) return null;
    if (hh < 0 || hh > 23) return null;
    if (mm < 0 || mm > 59) return null;
    return hh * 60 + mm;
  }

  function safeParseJson(s) {
    try {
      return JSON.parse(s || "{}");
    } catch (e) {
      return null;
    }
  }

  function ensureState(raw) {
    const st = raw && typeof raw === "object" && !Array.isArray(raw) ? raw : {};
    DAY_KEYS.forEach((k) => {
      if (!Array.isArray(st[k])) st[k] = [];
      st[k] = st[k].map((w) => {
        const from = w && typeof w === "object" ? (w.from || "") : "";
        const to = w && typeof w === "object" ? (w.to || "") : "";
        return { from: String(from || ""), to: String(to || "") };
      });
    });
    return st;
  }

  function setInputRed(inp) {
    if (!inp) return;
    inp.style.borderColor = "#ef4444";
  }

  function mappedButtonClass(key) {
    const classMap = window.yyClassMap || (document.documentElement && document.documentElement.yyClassMap) || {};
    const mapped = String(classMap[key] || "").trim();
    return mapped ? (key + " " + mapped) : key;
  }

  // --- dates (ISO inputs) ---
  const startDateInput = form.querySelector('input[name="start_date"]');
  const endDateInput = form.querySelector('input[name="end_date"]');

  function parseISODate(value) {
    const raw = String(value || "").trim();
    if (!raw) return null;
    const d = new Date(raw + "T00:00:00");
    if (Number.isNaN(d.getTime())) return null;
    return d;
  }

  // --- Dropdown (single global, same behavior as sending.js: mousedown) ---

  const DD = (function () {
    let el = null;
    let activeInput = null;

    function ensure() {
      if (el) return el;
      el = document.createElement("div");
      el.id = "yyTimeDropdown";
      el.style.position = "fixed";
      el.style.zIndex = "9999";
      el.style.minWidth = "72px";
      el.style.maxHeight = "220px";
      el.style.overflowY = "auto";
      el.style.background = "white";
      el.style.border = "1px solid #71d0f4";
      el.style.borderRadius = "8px";
      el.style.boxShadow = "0 12px 30px rgba(0,0,0,0.12)";
      el.style.padding = "6px";
      el.style.display = "none";
      document.body.appendChild(el);

      document.addEventListener("mousedown", function (e) {
        if (!el || el.style.display === "none") return;
        if (activeInput && e.target === activeInput) return;
        if (el.contains(e.target)) return;
        hide();
      });

      document.addEventListener("keydown", function (e) {
        if (e.key === "Escape") hide();
      });

      window.addEventListener("resize", hide);
      return el;
    }

    function buildOptions(values, onPick) {
      const root = ensure();
      root.innerHTML = "";
      values.forEach((v) => {
        const item = document.createElement("button");
        item.type = "button";
        item.textContent = String(v);
        item.style.display = "block";
        item.style.width = "100%";
        item.style.textAlign = "left";
        item.style.padding = "8px 10px";
        item.style.borderRadius = "6px";
        item.style.border = "0";
        item.style.background = "transparent";
        item.style.cursor = "pointer";
        item.addEventListener("mouseenter", () => { item.style.background = "rgba(0,0,0,0.05)"; });
        item.addEventListener("mouseleave", () => { item.style.background = "transparent"; });

        item.addEventListener("mousedown", function (e) {
          e.preventDefault();
          onPick(String(v));
        });

        root.appendChild(item);
      });
    }

    function positionNearInput(inp) {
      const root = ensure();
      const r = inp.getBoundingClientRect();

      const margin = 6;
      const viewportH = window.innerHeight || document.documentElement.clientHeight;
      const viewportW = window.innerWidth || document.documentElement.clientWidth;

      root.style.display = "block";
      root.style.left = "0px";
      root.style.top = "0px";

      const rr = root.getBoundingClientRect();
      const h = rr.height || 180;
      const w = rr.width || 180;

      const spaceBelow = viewportH - r.bottom;
      const useUp = spaceBelow < (h + 16);

      const left = Math.max(8, Math.min(r.left, (viewportW - 8 - w)));
      const top = useUp ? (r.top - margin - h) : (r.bottom + margin);

      root.style.left = left + "px";
      root.style.top = Math.max(8, top) + "px";
    }

    function show(inp, values, onPick) {
      activeInput = inp;
      buildOptions(values, function (v) {
        onPick(v);
        hide();
        inp.focus();
      });
      positionNearInput(inp);
    }

    function hide() {
      if (!el) return;
      el.style.display = "none";
      activeInput = null;
    }

    return { show, hide };
  })();

  function mkTimeInput(kind, placeholder) {
    const inp = document.createElement("input");
    inp.type = "text";
    inp.inputMode = "numeric";
    inp.autocomplete = "off";
    inp.className = INPUT_BASE;
    inp.maxLength = 2;
    inp.placeholder = placeholder || "";
    inp.dataset.yyKind = kind;

    inp.addEventListener("input", function () {
      const s = String(inp.value || "");
      const cleaned = s.replace(/[^\d]/g, "").slice(0, 2);
      if (cleaned !== s) inp.value = cleaned;
      inp.style.borderColor = "";
    });

    inp.addEventListener("mousedown", function () {
      const values =
        kind === "h"
          ? Array.from({ length: 24 }, (_, i) => pad2(i))
          : ["00", "15", "30", "45"];

      DD.show(inp, values, function (v) {
        inp.value = String(v);
        inp.dispatchEvent(new Event("input", { bubbles: true }));
      });
    });

    inp.addEventListener("blur", function () {
      inp.value = pad2(inp.value);
    });

    return inp;
  }

  function mkSep(text) {
    const s = document.createElement("span");
    s.textContent = text;
    s.className = "select-none";
    return s;
  }

  function mkBtn(kind) {
    const b = document.createElement("button");
    b.type = "button";
    b.className = (kind === "plus") ? "YY-BUTTON_TAB_MAIN !px-3 !py-1" : "YY-BUTTON_TAB_RED !px-3 !py-1";
    b.textContent = (kind === "plus") ? "+" : "−";
    return b;
  }

  function splitToParts(s) {
    const p = parseHHMM(s);
    if (!p) return { hh: "", mm: "" };
    return { hh: pad2(p.hh), mm: pad2(p.mm) };
  }

  function setUiDisabled(disabled) {
    const box = document.getElementById("yyWindowBox");
    if (!box) return;
    box.style.opacity = disabled ? "0.45" : "1";
    box.style.pointerEvents = disabled ? "none" : "auto";
  }

  // --- state sources ---

  const globalState = ensureState(safeParseJson(taGlobal.value || "{}"));
  let state = ensureState(safeParseJson(ta.value || "{}"));

  function firstWindowFromGlobalTuesday() {
    const tue = Array.isArray(globalState.tue) ? globalState.tue : [];
    const first = tue[0] && typeof tue[0] === "object" ? tue[0] : null;
    if (!first) return { from: "", to: "" };
    const from = String(first.from || "").trim();
    const to = String(first.to || "").trim();
    if (!from || !to) return { from: "", to: "" };
    return { from: from, to: to };
  }

  function useGlobalNow() {
    state = ensureState(JSON.parse(JSON.stringify(globalState)));
    setUiDisabled(true);
    ta.value = "";
  }

  function useCustomNow() {
    const raw = safeParseJson(ta.value || "{}");
    const custom = ensureState(raw);
    const hasAny = DAY_KEYS.some((k) => Array.isArray(custom[k]) && custom[k].length);
    state = hasAny ? custom : ensureState(JSON.parse(JSON.stringify(globalState)));
    setUiDisabled(false);
    ta.value = JSON.stringify(state);
  }

  // --- Render (as sending.js: "+" only last / empty) ---

  function renderDay(day) {
    const row = body.querySelector(`tr[data-yy-day="${day}"]`);
    if (!row) return;

    const box = row.querySelector(".yy-day-windows");
    if (!box) return;

    box.innerHTML = "";
    box.className = "yy-day-windows space-y-2";

    if (!Array.isArray(state[day])) state[day] = [];
    const windows = state[day];

    function addWindowRow(idx, w) {
      const line = document.createElement("div");
      line.className = "flex items-center gap-2";

      const btnMinus = mkBtn("minus");
      btnMinus.addEventListener("click", function () {
        state[day].splice(idx, 1);
        renderDay(day);
      });

      const fp = splitToParts(w.from);
      const tp = splitToParts(w.to);

      const fh = mkTimeInput("h", "HH");
      const fm = mkTimeInput("m", "MM");
      const th = mkTimeInput("h", "HH");
      const tm = mkTimeInput("m", "MM");

      fh.value = fp.hh;
      fm.value = fp.mm;
      th.value = tp.hh;
      tm.value = tp.mm;

      function syncToState() {
        const fromStr = (fh.value || fm.value) ? (pad2(fh.value) + ":" + pad2(fm.value)) : "";
        const toStr = (th.value || tm.value) ? (pad2(th.value) + ":" + pad2(tm.value)) : "";
        state[day][idx] = { from: fromStr, to: toStr };
      }

      [fh, fm, th, tm].forEach((inp) => {
        inp.addEventListener("input", syncToState);
        inp.addEventListener("blur", syncToState);
      });

      line.appendChild(btnMinus);
      line.appendChild(fh);
      line.appendChild(mkSep(":"));
      line.appendChild(fm);
      line.appendChild(mkSep("—"));
      line.appendChild(th);
      line.appendChild(mkSep(":"));
      line.appendChild(tm);

      if (idx === windows.length - 1) {
        const btnPlus = mkBtn("plus");
        btnPlus.addEventListener("click", function () {
          state[day].push({ from: "", to: "" });
          renderDay(day);
        });
        line.appendChild(btnPlus);
      }

      box.appendChild(line);
      syncToState();
    }

    if (windows.length === 0) {
      const line = document.createElement("div");
      line.className = "flex items-center gap-2";

      const btnPlus = mkBtn("plus");
      btnPlus.addEventListener("click", function () {
        state[day].push(firstWindowFromGlobalTuesday());
        renderDay(day);
      });

      line.appendChild(btnPlus);
      box.appendChild(line);
      return;
    }

    for (let i = 0; i < windows.length; i++) addWindowRow(i, windows[i]);
  }

  function renderAll() {
    DAY_KEYS.forEach(renderDay);
  }

  if (cb.checked) useGlobalNow();
  else useCustomNow();
  renderAll();

  cb.addEventListener("change", function () {
    if (cb.checked) useGlobalNow();
    else useCustomNow();
    renderAll();
  });

  // --- Submit validation (whole form) ---

  function clearReds() {
    form.querySelectorAll("input, select").forEach((el) => { el.style.borderColor = ""; });
    body.querySelectorAll(".yy-day-windows input").forEach((inp) => { inp.style.borderColor = ""; });
    const linkedTitle = document.querySelector('input[name="title"][form="yyCampaignForm"]');
    if (linkedTitle) linkedTitle.style.borderColor = "";
  }

  function getNamedControl(name) {
    if (!name) return null;
    const named = form.elements && form.elements.namedItem ? form.elements.namedItem(name) : null;
    if (named && named.nodeType === 1) return named;
    if (named && typeof named.length === "number" && named.length > 0 && named[0] && named[0].nodeType === 1) {
      return named[0];
    }
    return (
      form.querySelector(`[name="${name}"]`) ||
      document.querySelector(`[name="${name}"][form="${form.id}"]`)
    );
  }

  function validateRequiredText(name) {
    const el = getNamedControl(name);
    if (!el) return true;
    const v = String(el.value || "").trim();
    if (!v) {
      setInputRed(el);
      return false;
    }
    return true;
  }

  function validateRequiredSelect(name) {
    const el = getNamedControl(name);
    if (!el) return true;
    const v = String(el.value || "").trim();
    if (!v) {
      setInputRed(el);
      return false;
    }
    return true;
  }

  function isSendingListRequired() {
    return !!form.querySelector('select[name="sending_list"]');
  }

  function validateStartDate() {
    if (!startDateInput) return true;
    const v = String(startDateInput.value || "").trim();
    if (!v) return true;
    if (!parseISODate(v)) {
      setInputRed(startDateInput);
      return false;
    }
    return true;
  }

  function validateEndDateAndOrder() {
    if (!endDateInput) return true;

    const sRaw = String(startDateInput && startDateInput.value ? startDateInput.value : "").trim();
    const eRaw = String(endDateInput.value || "").trim();
    if (!eRaw) return true;

    const sd = parseISODate(sRaw);
    const ed = parseISODate(eRaw);
    if (!ed) {
      setInputRed(endDateInput);
      return false;
    }

    if (sd && ed.getTime() < sd.getTime()) {
      setInputRed(endDateInput);
      return false;
    }
    return true;
  }

  function validateWindowsAndSerialize() {
    if (cb.checked) {
      ta.value = "";
      return true;
    }
    ta.value = JSON.stringify(ensureState(state));
    return true;
  }

  form.addEventListener("submit", function (e) {
    const submitter = e.submitter || null;
    let action = submitter && submitter.value ? String(submitter.value).trim() : "";
    if (!action) action = lastAction;

    // Enter submit -> treat as save-on-current-step
    if (!action) {
      const hasId = !!form.querySelector('input[name="id"]');
      action = hasId ? "save_campaign_stay" : "add_campaign_stay";
    }

    if (
      action !== "add_campaign" &&
      action !== "save_campaign" &&
      action !== "add_campaign_stay" &&
      action !== "save_campaign_stay" &&
      action !== "add_campaign_close" &&
      action !== "save_campaign_close"
    ) {
      return;
    }

    clearReds();

    let ok = true;
    ok = validateRequiredText("title") && ok;
    if (isSendingListRequired()) {
      ok = validateRequiredSelect("sending_list") && ok;
    }
    ok = validateRequiredSelect("mailbox") && ok;

    ok = validateWindowsAndSerialize() && ok;

    if (!ok) {
      e.preventDefault();
      e.stopPropagation();
      return false;
    }
  });

  // --- "Шаблон" button state: enabled only when saved + no unsaved edits ---
  const templateBtn = document.getElementById("yyGoTemplateBtn");
  const titleInput = document.getElementById("yyCampaignTitleInput");
  const topSaveBtn = document.getElementById("yyTopSaveBtn");
  const saveLeftBtn = document.getElementById("yySaveLeftBtn");
  const saveRightBtn = document.getElementById("yySaveRightBtn");
  const hasCampaignId = !!form.querySelector('input[name="id"]');

  function readWindowSnapshot() {
    if (cb.checked) return "";
    return JSON.stringify(ensureState(state));
  }

  function readFormSnapshot() {
    return JSON.stringify({
      title: String(titleInput && titleInput.value ? titleInput.value : "").trim(),
      sending_list: String((form.querySelector('select[name="sending_list"]') || {}).value || "").trim(),
      mailbox: String((form.querySelector('select[name="mailbox"]') || {}).value || "").trim(),
      start_date: String((form.querySelector('input[name="start_date"]') || {}).value || "").trim(),
      end_date: String((form.querySelector('input[name="end_date"]') || {}).value || "").trim(),
      send_after_parent_days: String((form.querySelector('input[name="send_after_parent_days"]') || {}).value || "").trim(),
      use_global_window: cb.checked ? "1" : "0",
      window: readWindowSnapshot(),
    });
  }

  function hasRequiredFieldsFilled() {
    const titleVal = String(titleInput && titleInput.value ? titleInput.value : "").trim();
    const sendingEl = form.querySelector('select[name="sending_list"]');
    const mailboxEl = form.querySelector('select[name="mailbox"]');
    const sendingVal = String((sendingEl || {}).value || "").trim();
    const mailboxVal = String((mailboxEl || {}).value || "").trim();
    if (titleInput && titleVal) titleInput.classList.remove("!border-red-500");
    if (sendingEl && sendingVal) sendingEl.classList.remove("!border-red-500");
    if (mailboxEl && mailboxVal) mailboxEl.classList.remove("!border-red-500");
    const sendingOk = sendingEl ? !!sendingVal : true;
    return !!(titleVal && sendingOk && mailboxVal);
  }

  const initialSnapshot = readFormSnapshot();
  const initialTitle = String(titleInput && titleInput.value ? titleInput.value : "").trim();

  function setButtonState(btn, enabled, enabledClass, disabledClass, extraClass) {
    if (!btn) return;
    btn.disabled = !enabled;
    if (enabledClass && disabledClass) {
      const base = mappedButtonClass(enabled ? enabledClass : disabledClass);
      const extra = String(extraClass || "").trim();
      btn.setAttribute("class", extra ? (base + " " + extra) : base);
    }
  }

  function syncTemplateButtonState() {
    const isClean = readFormSnapshot() === initialSnapshot;
    const requiredFilled = hasRequiredFieldsFilled();
    const templateEnabled = hasCampaignId && requiredFilled && isClean;
    setButtonState(templateBtn, templateEnabled, "YY-BUTTON_MAIN_FULL", "YY-BUTTON_GRAY_FULL");

    const anyChanged = !isClean;
    const titleChanged = String(titleInput && titleInput.value ? titleInput.value : "").trim() !== initialTitle;
    setButtonState(saveLeftBtn, anyChanged && requiredFilled, "YY-BUTTON_MAIN", "YY-BUTTON_GRAY");
    setButtonState(saveRightBtn, anyChanged && requiredFilled, "YY-BUTTON_MAIN_FULL", "YY-BUTTON_GRAY_FULL");
    setButtonState(topSaveBtn, titleChanged && requiredFilled, "YY-BUTTON_MAIN", "YY-BUTTON_GRAY", "!w-fit !mb-0");
  }

  if (templateBtn) {
    templateBtn.addEventListener("click", function () {
      if (templateBtn.disabled) return;
      const url = String(templateBtn.dataset.url || "").trim();
      if (!url) return;
      window.location.href = url;
    });
  }

  form.addEventListener("input", syncTemplateButtonState);
  form.addEventListener("change", syncTemplateButtonState);
  if (titleInput) {
    titleInput.addEventListener("input", syncTemplateButtonState);
    titleInput.addEventListener("change", syncTemplateButtonState);
  }
  body.addEventListener("input", syncTemplateButtonState);
  body.addEventListener("change", syncTemplateButtonState);
  syncTemplateButtonState();
})();
