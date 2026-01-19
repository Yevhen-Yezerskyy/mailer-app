// FILE: web/static/js/aap_settings/sending.js
// DATE: 2026-01-19
// PURPOSE: Settings → Sending: time dropdown, окна по дням, +/−, сбор JSON перед submit.
// CHANGE:
// - "+" больше НЕ валидирует (всегда добавляет строку).
// - Валидация ТОЛЬКО на submit: пустые инпуты красим (только пустые) и блокируем; from>=to красим всю строку и блокируем.
// - Без alert. Тайпинг/дропдаун не валидируем, только лёгкий pad2 на blur.

(function () {
  const form = document.getElementById("yySendingForm");
  const ta = document.getElementById("yyValueJson");
  const body = document.getElementById("yySendingBody");
  if (!form || !ta || !body) return;

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

  function safeParseJson() {
    try {
      return JSON.parse(ta.value || "{}");
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

  let state = ensureState(safeParseJson());

  // --- Dropdown (single global) ---

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

  // --- UI helpers ---

  function mkTimeInput(kind /* 'h'|'m' */, placeholder) {
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
      // при вводе снимаем красную рамку только с этого инпута
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

  function clearLineOutline(line) {
    if (!line) return;
    line.style.outline = "";
    line.style.outlineOffset = "";
    line.style.borderRadius = "";
  }

  function setLineOutlineRed(line) {
    if (!line) return;
    line.style.outline = "2px solid #ef4444";
    line.style.outlineOffset = "2px";
    line.style.borderRadius = "10px";
  }

  function setInputRed(inp) {
    if (!inp) return;
    inp.style.borderColor = "#ef4444";
  }

  // --- Render ---

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
        // храним как есть (частично тоже), submit валидирует по DOM
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

      // "+" только у последней строки, БЕЗ валидации
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

    // если окон 0 — показываем только "+"
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

  renderAll();

  // --- Submit validation (primitive) ---

  form.addEventListener("submit", function (e) {
    const btn = e.submitter || document.activeElement;
    const action = btn && btn.value ? String(btn.value).trim() : "";
    if (action && action !== "save") return;

    // сброс красного
    body.querySelectorAll(".yy-day-windows input").forEach((inp) => { inp.style.borderColor = ""; });
    body.querySelectorAll(".yy-day-windows > div").forEach((line) => { clearLineOutline(line); });

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
        if (inputs.length !== 4) continue; // это плюс-строка

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

    if (hasInvalid) {
      e.preventDefault();
      e.stopPropagation();
      return false;
    }

    state = ensureState(out);
    ta.value = JSON.stringify(state);
  });
})();
