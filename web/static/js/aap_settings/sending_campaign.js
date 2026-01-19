// FILE: web/static/js/aap_settings/sending_campaign.js
// DATE: 2026-01-19
// PURPOSE: Campaigns add/edit: окно отправки + валидация формы на submit.
// CHANGE: Fix submit without button-click (Enter/etc) -> всегда определяем action и сериализуем window.

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

  // --- dates (client defaults + helpers) ---

  const startDD = form.querySelector('input[name="start_dd"]');
  const startMM = form.querySelector('input[name="start_mm"]');
  const startYY = form.querySelector('input[name="start_yy"]');
  const endDD = form.querySelector('input[name="end_dd"]');
  const endMM = form.querySelector('input[name="end_mm"]');
  const endYY = form.querySelector('input[name="end_yy"]');

  function todayParts() {
    const d = new Date();
    return { dd: d.getDate(), mm: d.getMonth() + 1, yy: d.getFullYear() };
  }

  function addDaysParts(dd, mm, yy, addDays) {
    const d = new Date(yy, mm - 1, dd);
    if (Number.isNaN(d.getTime())) return null;
    d.setDate(d.getDate() + addDays);
    return { dd: d.getDate(), mm: d.getMonth() + 1, yy: d.getFullYear() };
  }

  function readDateParts(inpDD, inpMM, inpYY) {
    const dd = toInt(inpDD && inpDD.value);
    const mm = toInt(inpMM && inpMM.value);
    const yy = toInt(inpYY && inpYY.value);
    return { dd, mm, yy };
  }

  function isAllEmptyDate(inpDD, inpMM, inpYY) {
    return !String((inpDD && inpDD.value) || "").trim()
      && !String((inpMM && inpMM.value) || "").trim()
      && !String((inpYY && inpYY.value) || "").trim();
  }

  function isAnyEmptyDate(inpDD, inpMM, inpYY) {
    const a = String((inpDD && inpDD.value) || "").trim();
    const b = String((inpMM && inpMM.value) || "").trim();
    const c = String((inpYY && inpYY.value) || "").trim();
    return (!a || !b || !c) && !(!a && !b && !c);
  }

  function buildDate(dd, mm, yy) {
    if (dd == null || mm == null || yy == null) return null;
    const d = new Date(yy, mm - 1, dd);
    if (Number.isNaN(d.getTime())) return null;
    if (d.getFullYear() !== yy || (d.getMonth() + 1) !== mm || d.getDate() !== dd) return null;
    return d;
  }

  function applyDateDefaultsIfEmpty() {
    if (startDD && startMM && startYY && isAllEmptyDate(startDD, startMM, startYY)) {
      const t = todayParts();
      startDD.value = String(t.dd);
      startMM.value = String(t.mm);
      startYY.value = String(t.yy);
    }

    if (endDD && endMM && endYY && isAllEmptyDate(endDD, endMM, endYY)) {
      const s = readDateParts(startDD, startMM, startYY);
      const base = (s.dd && s.mm && s.yy) ? s : todayParts();
      const e = addDaysParts(base.dd, base.mm, base.yy, 90);
      if (e) {
        endDD.value = String(e.dd);
        endMM.value = String(e.mm);
        endYY.value = String(e.yy);
      }
    }
  }

  applyDateDefaultsIfEmpty();

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
        state[day].push({ from: "", to: "" });
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
  }

  function validateRequiredText(name) {
    const el = form.querySelector(`[name="${name}"]`);
    if (!el) return true;
    const v = String(el.value || "").trim();
    if (!v) {
      setInputRed(el);
      return false;
    }
    return true;
  }

  function validateRequiredSelect(name) {
    const el = form.querySelector(`select[name="${name}"]`);
    if (!el) return true;
    const v = String(el.value || "").trim();
    if (!v) {
      setInputRed(el);
      return false;
    }
    return true;
  }

  function validateStartDate() {
    if (!startDD || !startMM || !startYY) return true;

    const anyEmpty = isAnyEmptyDate(startDD, startMM, startYY);
    if (anyEmpty) {
      if (!String(startDD.value || "").trim()) setInputRed(startDD);
      if (!String(startMM.value || "").trim()) setInputRed(startMM);
      if (!String(startYY.value || "").trim()) setInputRed(startYY);
      return false;
    }

    const p = readDateParts(startDD, startMM, startYY);
    const d = buildDate(p.dd, p.mm, p.yy);
    if (!d) {
      setInputRed(startDD); setInputRed(startMM); setInputRed(startYY);
      return false;
    }

    startDD.value = pad2(startDD.value);
    startMM.value = pad2(startMM.value);
    return true;
  }

  function validateEndDateAndOrder() {
    if (!endDD || !endMM || !endYY) return true;

    if (isAllEmptyDate(endDD, endMM, endYY)) {
      return true; // optional
    }

    const anyEmpty = isAnyEmptyDate(endDD, endMM, endYY);
    if (anyEmpty) {
      if (!String(endDD.value || "").trim()) setInputRed(endDD);
      if (!String(endMM.value || "").trim()) setInputRed(endMM);
      if (!String(endYY.value || "").trim()) setInputRed(endYY);
      return false;
    }

    const s = readDateParts(startDD, startMM, startYY);
    const sd = buildDate(s.dd, s.mm, s.yy);
    const e = readDateParts(endDD, endMM, endYY);
    const ed = buildDate(e.dd, e.mm, e.yy);

    if (!ed) {
      setInputRed(endDD); setInputRed(endMM); setInputRed(endYY);
      return false;
    }

    if (sd && ed.getTime() < sd.getTime()) {
      setInputRed(endDD); setInputRed(endMM); setInputRed(endYY);
      return false;
    }

    endDD.value = pad2(endDD.value);
    endMM.value = pad2(endMM.value);
    return true;
  }

  function validateWindowsAndSerialize() {
    if (cb.checked) {
      ta.value = "";
      return true;
    }

    let hasInvalid = false;
    const out = {};
    for (const day of DAY_KEYS) out[day] = [];

    for (const day of DAY_KEYS) {
      const row = body.querySelector(`tr[data-yy-day="${day}"]`);
      if (!row) continue;

      const box = row.querySelector(".yy-day-windows");
      if (!box) continue;

      const lines = Array.from(box.children || []);
      for (const line of lines) {
        const inputs = Array.from(line.querySelectorAll("input"));
        if (inputs.length !== 4) continue; // "+" row

        const [fh, fm, th, tm] = inputs;
        const vfh = String(fh.value || "").trim();
        const vfm = String(fm.value || "").trim();
        const vth = String(th.value || "").trim();
        const vtm = String(tm.value || "").trim();

        const empty = [];
        if (!vfh) empty.push(fh);
        if (!vfm) empty.push(fm);
        if (!vth) empty.push(th);
        if (!vtm) empty.push(tm);

        if (empty.length > 0) {
          hasInvalid = true;
          empty.forEach(setInputRed);
          continue;
        }

        const H1 = toInt(vfh);
        const M1 = toInt(vfm);
        const H2 = toInt(vth);
        const M2 = toInt(vtm);

        const fMin = toMinutes(H1, M1);
        const tMin = toMinutes(H2, M2);

        if (fMin == null || tMin == null || fMin >= tMin) {
          hasInvalid = true;
          inputs.forEach(setInputRed);
          continue;
        }

        out[day].push({
          from: pad2(H1) + ":" + pad2(M1),
          to: pad2(H2) + ":" + pad2(M2),
        });
      }
    }

    if (hasInvalid) return false;

    state = ensureState(out);
    ta.value = JSON.stringify(state);
    return true;
  }

  form.addEventListener("submit", function (e) {
    const submitter = e.submitter || null;
    let action = submitter && submitter.value ? String(submitter.value).trim() : "";
    if (!action) action = lastAction;

    // --- FIX: Enter submit -> treat as Save/Add (and serialize window) ---
    if (!action) {
      const hasId = !!form.querySelector('input[name="id"]');
      action = hasId ? "save_campaign" : "add_campaign";
    }

    if (
      action !== "add_campaign" &&
      action !== "save_campaign" &&
      action !== "add_campaign_close" &&
      action !== "save_campaign_close"
    ) {
      return;
    }

    clearReds();

    let ok = true;
    ok = validateRequiredText("title") && ok;
    ok = validateRequiredSelect("mailing_list") && ok;
    ok = validateRequiredSelect("mailbox") && ok;

    ok = validateStartDate() && ok;
    ok = validateEndDateAndOrder() && ok;

    ok = validateWindowsAndSerialize() && ok;

    if (!ok) {
      e.preventDefault();
      e.stopPropagation();
      return false;
    }
  });
})();
