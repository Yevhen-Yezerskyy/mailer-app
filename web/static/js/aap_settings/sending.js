// FILE: web/static/js/aap_settings/sending.js
// DATE: 2026-01-18
// PURPOSE: Settings → Sending: таблица окон по дням, кнопки +/−, сбор JSON перед submit.
// CHANGE: Время вводится ТОЛЬКО через text-input. На focus показываем кастомный dropdown (dropup при нехватке места).
//         Никаких datalist/number/spinner. Выбор из dropdown вставляет значение, вручную писать можно, на blur pad2 до 2 цифр.

(function () {
  const form = document.getElementById("yySendingForm");
  const ta = document.getElementById("yyValueJson");
  const body = document.getElementById("yySendingBody");
  if (!form || !ta || !body) return;

  const DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun", "hol"];

  const INPUT_BASE =
    "YY-INPUT !mb-0 !w-14 text-center px-4 py-2 rounded-md border border-[#71d0f4] bg-white " +
    "placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-[#007c09]/10 focus:border-[#007c09]";

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
    const st = (raw && typeof raw === "object" && !Array.isArray(raw)) ? raw : {};
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
        if (activeInput && (e.target === activeInput)) return;
        if (el.contains(e.target)) return;
        hide();
      });

      document.addEventListener("keydown", function (e) {
        if (e.key === "Escape") hide();
      });

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
          // mousedown чтобы не потерять focus раньше времени
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

      // сначала показываем, чтобы измерить высоту
      root.style.display = "block";
      root.style.left = "0px";
      root.style.top = "0px";

      const h = root.getBoundingClientRect().height || 180;

      const spaceBelow = viewportH - r.bottom;
      const useUp = spaceBelow < (h + 16);

      const left = Math.max(8, Math.min(r.left, (window.innerWidth - 8 - 180)));
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

    // только цифры при вводе (но не ломаем вставку)
    inp.addEventListener("input", function () {
      const s = String(inp.value || "");
      const cleaned = s.replace(/[^\d]/g, "").slice(0, 2);
      if (cleaned !== s) inp.value = cleaned;
    });

    inp.addEventListener("focus", function () {
      const values =
        kind === "h"
          ? Array.from({ length: 24 }, (_, i) => pad2(i))
          : ["00", "15", "30", "45"];

      DD.show(inp, values, function (v) {
        inp.value = String(v);
        // trigger input consumers
        inp.dispatchEvent(new Event("input", { bubbles: true }));
        inp.dispatchEvent(new Event("blur", { bubbles: true }));
      });
    });

    inp.addEventListener("blur", function () {
      inp.value = pad2(inp.value);
      // небольшой таймаут, чтобы клик по dropdown успел отработать
      setTimeout(() => DD.hide(), 0);
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

  // --- Render ---

  function renderDay(day) {
    const row = body.querySelector(`tr[data-yy-day="${day}"]`);
    if (!row) return;

    const box = row.querySelector(".yy-day-windows");
    if (!box) return;

    box.innerHTML = "";
    box.className = "yy-day-windows space-y-2";

    const windows = Array.isArray(state[day]) ? state[day] : [];

    function addPlusRow() {
      const line = document.createElement("div");
      line.className = "flex items-center gap-2";

      const btn = mkBtn("plus");
      btn.addEventListener("click", function () {
        state[day].push({ from: "", to: "" });
        renderDay(day);
      });

      line.appendChild(btn);
      box.appendChild(line);
    }

    function addWindowRow(idx, w) {
      const line = document.createElement("div");
      line.className = "flex items-center gap-2";

      const btn = mkBtn("minus");
      btn.addEventListener("click", function () {
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

      function sync(doPad) {
        if (doPad) {
          fh.value = pad2(fh.value);
          fm.value = pad2(fm.value);
          th.value = pad2(th.value);
          tm.value = pad2(tm.value);
        }

        const H1 = toInt(fh.value);
        const M1 = toInt(fm.value);
        const H2 = toInt(th.value);
        const M2 = toInt(tm.value);

        const fOk = (H1 != null && M1 != null && H1 >= 0 && H1 <= 23 && M1 >= 0 && M1 <= 59);
        const tOk = (H2 != null && M2 != null && H2 >= 0 && H2 <= 23 && M2 >= 0 && M2 <= 59);

        const fMin = fOk ? toMinutes(H1, M1) : null;
        const tMin = tOk ? toMinutes(H2, M2) : null;

        const ok = (fMin != null && tMin != null && fMin < tMin);
        line.style.opacity = (ok || (!fh.value && !fm.value && !th.value && !tm.value)) ? "1" : "0.85";

        const fromStr = fOk ? (pad2(H1) + ":" + pad2(M1)) : (String(fh.value || "") + ":" + String(fm.value || "")).trim();
        const toStr = tOk ? (pad2(H2) + ":" + pad2(M2)) : (String(th.value || "") + ":" + String(tm.value || "")).trim();

        state[day][idx] = { from: fromStr, to: toStr };
      }

      [fh, fm, th, tm].forEach((inp) => {
        inp.addEventListener("input", function () { sync(false); });
        inp.addEventListener("blur", function () { sync(true); });
      });

      line.appendChild(btn);
      line.appendChild(fh);
      line.appendChild(mkSep(":"));
      line.appendChild(fm);
      line.appendChild(mkSep("—"));
      line.appendChild(th);
      line.appendChild(mkSep(":"));
      line.appendChild(tm);

      box.appendChild(line);
      sync(true);
    }

    for (let i = 0; i < windows.length; i++) addWindowRow(i, windows[i]);
    addPlusRow();
  }

  function renderAll() {
    DAY_KEYS.forEach(renderDay);
  }

  renderAll();

  form.addEventListener("submit", function (e) {
    const btn = e.submitter || document.activeElement;
    const action = btn && btn.value ? String(btn.value).trim() : "";
    if (action && action !== "save") return;

    for (const day of DAY_KEYS) {
      const arr = Array.isArray(state[day]) ? state[day] : [];
      for (const w of arr) {
        const from = String(w.from || "").trim();
        const to = String(w.to || "").trim();

        if (!from || !to) {
          alert("Есть пустое окно. Заполни время или удали окно.");
          e.preventDefault();
          e.stopPropagation();
          return false;
        }

        const fp = parseHHMM(from);
        const tp = parseHHMM(to);
        if (!fp || !tp) {
          alert("Неверное время. Формат HH:MM.");
          e.preventDefault();
          e.stopPropagation();
          return false;
        }

        const fMin = toMinutes(fp.hh, fp.mm);
        const tMin = toMinutes(tp.hh, tp.mm);
        if (fMin == null || tMin == null) {
          alert("Неверное время. Часы 0-23, минуты 0-59.");
          e.preventDefault();
          e.stopPropagation();
          return false;
        }
        if (fMin >= tMin) {
          alert("В окне время 'от' должно быть меньше 'до'.");
          e.preventDefault();
          e.stopPropagation();
          return false;
        }

        w.from = pad2(fp.hh) + ":" + pad2(fp.mm);
        w.to = pad2(tp.hh) + ":" + pad2(tp.mm);
      }
    }

    DAY_KEYS.forEach((k) => { if (!Array.isArray(state[k])) state[k] = []; });
    ta.value = JSON.stringify(state);
  });
})();
