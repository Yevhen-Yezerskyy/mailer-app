// FILE: web/static/js/aap_settings/sending.js
// DATE: 2026-01-19
// PURPOSE: Fix time dropdown повторное открытие; "+" только в конце последней строки; убрать alert, подсветка ошибок.

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

      // закрываем ТОЛЬКО кликом вне или Esc
      document.addEventListener("mousedown", function (e) {
        if (!el || el.style.display === "none") return;
        if (activeInput && e.target === activeInput) return;
        if (el.contains(e.target)) return;
        hide();
      });

      document.addEventListener("keydown", function (e) {
        if (e.key === "Escape") hide();
      });

      window.addEventListener("scroll", hide, true);
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
          e.preventDefault(); // не теряем фокус
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

  // --- Validation helpers (no alerts) ---

  function getWindowValidity(fh, fm, th, tm) {
    const H1 = toInt(fh.value);
    const M1 = toInt(fm.value);
    const H2 = toInt(th.value);
    const M2 = toInt(tm.value);

    const allEmpty = (!fh.value && !fm.value && !th.value && !tm.value);

    const fOk = (H1 != null && M1 != null && H1 >= 0 && H1 <= 23 && M1 >= 0 && M1 <= 59);
    const tOk = (H2 != null && M2 != null && H2 >= 0 && H2 <= 23 && M2 >= 0 && M2 <= 59);

    const fMin = fOk ? toMinutes(H1, M1) : null;
    const tMin = tOk ? toMinutes(H2, M2) : null;

    const ok = (fMin != null && tMin != null && fMin < tMin);
    const complete = (fh.value && fm.value && th.value && tm.value);

    return { ok, allEmpty, complete };
  }

  function setInvalidStyle(line, inputs, invalid) {
    if (!line) return;
    if (invalid) {
      line.style.outline = "2px solid #ef4444";
      line.style.outlineOffset = "2px";
      line.style.borderRadius = "10px";
      (inputs || []).forEach((inp) => { inp.style.borderColor = "#ef4444"; });
    } else {
      line.style.outline = "";
      line.style.outlineOffset = "";
      line.style.borderRadius = "";
      (inputs || []).forEach((inp) => { inp.style.borderColor = ""; });
    }
  }

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
    });

    // показываем dropdown по mousedown (всегда), без искусственных blur
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
      // DD.hide() тут НЕ трогаем — закрытие только по вне-клику/Esc/scroll/resize
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
    const lastIdx = windows.length - 1;

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

      const inputs = [fh, fm, th, tm];

      function sync(doPad) {
        if (doPad) {
          fh.value = pad2(fh.value);
          fm.value = pad2(fm.value);
          th.value = pad2(th.value);
          tm.value = pad2(tm.value);
        }

        const val = getWindowValidity(fh, fm, th, tm);

        const fromStr = (toInt(fh.value) != null && toInt(fm.value) != null)
          ? (pad2(toInt(fh.value)) + ":" + pad2(toInt(fm.value)))
          : "";

        const toStr = (toInt(th.value) != null && toInt(tm.value) != null)
          ? (pad2(toInt(th.value)) + ":" + pad2(toInt(tm.value)))
          : "";

        // хранение: либо пусто, либо HH:MM
        state[day][idx] = {
          from: (val.complete ? fromStr : ""),
          to: (val.complete ? toStr : ""),
        };

        // подсветка: неверно только если пытаются заполнить (complete) и ok=false
        setInvalidStyle(line, inputs, (val.complete && !val.ok));
      }

      inputs.forEach((inp) => {
        inp.addEventListener("input", function () { sync(false); });
        inp.addEventListener("blur", function () { sync(true); });
      });

      line.appendChild(btnMinus);
      line.appendChild(fh);
      line.appendChild(mkSep(":"));
      line.appendChild(fm);
      line.appendChild(mkSep("—"));
      line.appendChild(th);
      line.appendChild(mkSep(":"));
      line.appendChild(tm);

      // "+" только в конце последней строки
      if (idx === lastIdx) {
        const btnPlus = mkBtn("plus");
        btnPlus.addEventListener("click", function () {
          sync(true);
          const val = getWindowValidity(fh, fm, th, tm);
          if (!val.complete || !val.ok) {
            setInvalidStyle(line, inputs, true);
            // фокус на первое пустое/невалидное
            for (const inp of inputs) {
              if (!inp.value) { inp.focus(); return; }
            }
            fh.focus();
            return;
          }
          state[day].push({ from: "", to: "" });
          renderDay(day);
        });
        line.appendChild(btnPlus);
      }

      box.appendChild(line);
      sync(true);
    }

    // если вообще нет окон — создаём одну пустую строку (чтобы было куда жать "+")
    if (windows.length === 0) {
      state[day].push({ from: "", to: "" });
    }

    for (let i = 0; i < state[day].length; i++) addWindowRow(i, state[day][i]);
  }

  function renderAll() {
    DAY_KEYS.forEach(renderDay);
  }

  renderAll();

  form.addEventListener("submit", function (e) {
    const btn = e.submitter || document.activeElement;
    const action = btn && btn.value ? String(btn.value).trim() : "";
    if (action && action !== "save") return;

    let hasInvalid = false;

    for (const day of DAY_KEYS) {
      const arr = Array.isArray(state[day]) ? state[day] : [];
      const cleaned = [];

      for (const w of arr) {
        const from = String(w.from || "").trim();
        const to = String(w.to || "").trim();

        // полностью пустые строки игнорируем (не сохраняем)
        if (!from && !to) continue;

        const fp = parseHHMM(from);
        const tp = parseHHMM(to);
        const fMin = fp ? toMinutes(fp.hh, fp.mm) : null;
        const tMin = tp ? toMinutes(tp.hh, tp.mm) : null;

        if (!fp || !tp || fMin == null || tMin == null || fMin >= tMin) {
          hasInvalid = true;
        } else {
          cleaned.push({ from: pad2(fp.hh) + ":" + pad2(fp.mm), to: pad2(tp.hh) + ":" + pad2(tp.mm) });
        }
      }

      state[day] = cleaned;
    }

    if (hasInvalid) {
      e.preventDefault();
      e.stopPropagation();
      // перерендер — подсветит строки по live-логике; добавим пустую строку где надо
      renderAll();
      return false;
    }

    DAY_KEYS.forEach((k) => { if (!Array.isArray(state[k])) state[k] = []; });
    ta.value = JSON.stringify(state);
  });
})();
