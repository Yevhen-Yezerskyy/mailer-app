// FILE: web/static/js/campaign_templates/main.js
// DATE: 2026-01-18
// PURPOSE: Клики по overlays + выбор GlobalTemplate (gl_tpl) с confirm и redirect.
// CHANGE: Добавлен предпросмотр кнопок overlays:
// - colors: фон кнопки из .bg-2 (fallback: background/background-color из блока)
// - fonts: font-family кнопки из style-блока выбранного шрифта.

(function () {
  "use strict";

  const _overlayCssCache = new Map();
  const _overlayHeaderTokensCache = new Map();
  const OVERLAY_BORDER_GRAY = "#c2c9d3";
  const OVERLAY_BORDER_ACTIVE = "#86c8ff";
  const OVERLAY_RADIUS = "8px";
  const OVERLAY_SHADOW_ACTIVE = "0 0 0 1.61px rgba(60, 77, 90, 0.28)";
  let templateFlowHeightRaf = 0;

  async function fetchText(url) {
    const r = await fetch(url, { credentials: "same-origin" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.text();
  }

  function overlayCssUrl(gid, type, name) {
    return (
      `/panel/campaigns/templates/_global-style-css/?gid=${encodeURIComponent(gid)}` +
      `&type=${encodeURIComponent(type)}&name=${encodeURIComponent(name)}`
    );
  }

  async function fetchOverlayCss(gid, type, name) {
    const key = `${gid}|${type}|${name}`;
    if (_overlayCssCache.has(key)) return _overlayCssCache.get(key);

    const p = fetchText(overlayCssUrl(gid, type, name)).catch(() => "");
    _overlayCssCache.set(key, p);
    return p;
  }

  function getParam(name) {
    try {
      return String(new URLSearchParams(window.location.search).get(name) || "").trim();
    } catch (_) {
      return "";
    }
  }

  async function fetchInitialEditorCss() {
    const glTpl = getParam("gl_tpl");
    if (glTpl) {
      return fetchText(`/panel/campaigns/templates/_render-user-css/?gl_tpl=${encodeURIComponent(glTpl)}`).catch(
        () => ""
      );
    }

    const state = getParam("state");
    const id = getParam("id");
    if (state === "edit" && id) {
      return fetchText(`/panel/campaigns/templates/_render-user-css/?id=${encodeURIComponent(id)}`).catch(() => "");
    }

    const tplState = getParam("tpl_state");
    const tplId = getParam("tpl_id");
    if (tplState === "edit" && tplId) {
      return fetchText(`/panel/campaigns/templates/_render-user-css/?id=${encodeURIComponent(tplId)}`).catch(
        () => ""
      );
    }
    return "";
  }

  async function fetchOverlayHeaderTokens(gid) {
    if (_overlayHeaderTokensCache.has(gid)) return _overlayHeaderTokensCache.get(gid);

    const p = fetchText(
      `/panel/campaigns/templates/_render-user-html/?gl_tpl=${encodeURIComponent(gid)}`
    )
      .then((html) => {
        const h = String(html || "");
        const bg = rxVal(h, /\bclass\s*=\s*["'][^"']*\b(bg-\d+)\b[^"']*["']/i).toLowerCase();
        const fg = rxVal(h, /\bclass\s*=\s*["'][^"']*\b(color-\d+)\b[^"']*["']/i).toLowerCase();
        return { bgClass: bg || "", fgClass: fg || "" };
      })
      .catch(() => ({ bgClass: "", fgClass: "" }));

    _overlayHeaderTokensCache.set(gid, p);
    return p;
  }

  function rxVal(css, re) {
    const m = re.exec(css || "");
    return m && m[1] ? String(m[1]).trim() : "";
  }

  function escapeRe(s) {
    return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function pickColorBg(css, preferredBgClass) {
    if (preferredBgClass) {
      const direct = rxVal(
        css,
        new RegExp(
          `\\.${escapeRe(preferredBgClass)}\\s*\\{[^}]*\\bbackground(?:-color)?\\s*:\\s*([^;}{]+)\\s*;?`,
          "i"
        )
      );
      if (direct) return direct;
    }

    return (
      rxVal(css, /\.bg-2\s*\{[^}]*\bbackground(?:-color)?\s*:\s*([^;}{]+)\s*;?/i) ||
      rxVal(css, /\.bg-1\s*\{[^}]*\bbackground(?:-color)?\s*:\s*([^;}{]+)\s*;?/i) ||
      rxVal(css, /\bbackground-color\s*:\s*([^;}{]+)\s*;?/i) ||
      rxVal(css, /\bbackground\s*:\s*([^;}{]+)\s*;?/i)
    );
  }

  function pickColorFg(css, preferredFgClass) {
    if (preferredFgClass) {
      const direct = rxVal(
        css,
        new RegExp(`\\.${escapeRe(preferredFgClass)}\\s*\\{[^}]*\\bcolor\\s*:\\s*([^;}{]+)\\s*;?`, "i")
      );
      if (direct) return direct;
    }

    return (
      rxVal(css, /\.color-2\s*\{[^}]*\bcolor\s*:\s*([^;}{]+)\s*;?/i) ||
      rxVal(css, /\.color-1\s*\{[^}]*\bcolor\s*:\s*([^;}{]+)\s*;?/i) ||
      rxVal(css, /\bcolor\s*:\s*([^;}{]+)\s*;?/i)
    );
  }

  function pickFontFamily(css) {
    return rxVal(css, /\bfont-family\s*:\s*([^;}{]+)\s*;?/i);
  }

  function normalizeCssValue(v) {
    return String(v || "")
      .replace(/["']/g, "")
      .replace(/\s+/g, "")
      .trim()
      .toLowerCase();
  }

  function normalizePrimaryFont(v) {
    const first = String(v || "").split(",")[0] || "";
    return normalizeCssValue(first);
  }

  function parseHexColor(value) {
    const v = String(value || "").trim();
    const m = /^#([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(v);
    if (!m) return null;
    let h = m[1].toLowerCase();
    if (h.length === 3) h = h.split("").map((c) => c + c).join("");
    const n = parseInt(h, 16);
    if (!Number.isFinite(n)) return null;
    return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255 };
  }

  function autoContrast(bgColor) {
    const rgb = parseHexColor(bgColor);
    if (!rgb) return "";
    const yiq = (rgb.r * 299 + rgb.g * 587 + rgb.b * 114) / 1000;
    return yiq >= 145 ? "#1f2937" : "#ffffff";
  }

  function setOverlayButtonFrame(btn, active) {
    if (!btn) return;
    btn.style.borderStyle = "solid";
    btn.style.borderWidth = "1px";
    btn.style.borderRadius = OVERLAY_RADIUS;
    btn.style.borderColor = active ? OVERLAY_BORDER_ACTIVE : OVERLAY_BORDER_GRAY;
    btn.style.boxShadow = active ? OVERLAY_SHADOW_ACTIVE : "none";
  }

  function setOverlayActive(btn) {
    if (!btn) return;
    const type = String(btn.dataset.type || "").trim();
    if (!(type === "colors" || type === "fonts")) return;

    const all = Array.from(document.querySelectorAll(".yy-tpl-overlay-btn"));
    for (const it of all) {
      if (String(it.dataset.type || "").trim() !== type) continue;
      it.classList.remove("yy-tpl-overlay-active");
      setOverlayButtonFrame(it, false);
    }
    btn.classList.add("yy-tpl-overlay-active");
    setOverlayButtonFrame(btn, true);
  }

  async function paintOverlayButton(btn) {
    const gid = String(btn?.dataset?.gid || "").trim();
    const type = String(btn?.dataset?.type || "").trim();
    const name = String(btn?.dataset?.name || "").trim();
    if (!(gid && (type === "colors" || type === "fonts") && name)) return;

    const css = await fetchOverlayCss(gid, type, name);
    if (!css) return;

    if (type === "colors") {
      const tokens = await fetchOverlayHeaderTokens(gid);
      const bg = pickColorBg(css, tokens.bgClass);
      if (bg) {
        btn.style.background = bg;
      }
      const fg = pickColorFg(css, tokens.fgClass) || autoContrast(bg);
      if (fg) btn.style.color = fg;
      setOverlayButtonFrame(btn, btn.classList.contains("yy-tpl-overlay-active"));
      return;
    }

    const family = pickFontFamily(css);
    if (family) btn.style.fontFamily = family;
    setOverlayButtonFrame(btn, btn.classList.contains("yy-tpl-overlay-active"));
  }

  async function detectInitialActiveButtons() {
    const btns = Array.from(document.querySelectorAll(".yy-tpl-overlay-btn"));
    if (!btns.length) return;

    const colorBtns = btns.filter((b) => String(b.dataset.type || "").trim() === "colors");
    const fontBtns = btns.filter((b) => String(b.dataset.type || "").trim() === "fonts");
    const gid = String((btns[0] && btns[0].dataset && btns[0].dataset.gid) || "").trim();
    if (!gid) return;

    const currentCss = await fetchInitialEditorCss();
    if (!currentCss) return;

    const tokens = await fetchOverlayHeaderTokens(gid);
    const currentBg = normalizeCssValue(pickColorBg(currentCss, tokens.bgClass));
    const currentFg = normalizeCssValue(pickColorFg(currentCss, tokens.fgClass));
    const currentFont = normalizePrimaryFont(pickFontFamily(currentCss));

    let activeColorBtn = null;
    if (currentBg || currentFg) {
      for (const btn of colorBtns) {
        const css = await fetchOverlayCss(
          String(btn.dataset.gid || "").trim(),
          "colors",
          String(btn.dataset.name || "").trim()
        );
        if (!css) continue;

        const bg = normalizeCssValue(pickColorBg(css, tokens.bgClass));
        const fg = normalizeCssValue(pickColorFg(css, tokens.fgClass));
        if (currentBg && bg && bg === currentBg) {
          activeColorBtn = btn;
          break;
        }
        if (!activeColorBtn && currentFg && fg && fg === currentFg) {
          activeColorBtn = btn;
        }
      }
    }
    if (activeColorBtn) setOverlayActive(activeColorBtn);

    let activeFontBtn = null;
    if (currentFont) {
      for (const btn of fontBtns) {
        const css = await fetchOverlayCss(
          String(btn.dataset.gid || "").trim(),
          "fonts",
          String(btn.dataset.name || "").trim()
        );
        if (!css) continue;
        const family = normalizePrimaryFont(pickFontFamily(css));
        if (family && family === currentFont) {
          activeFontBtn = btn;
          break;
        }
      }
    }
    if (activeFontBtn) setOverlayActive(activeFontBtn);
  }

  function initOverlayButtonPreviews() {
    const btns = Array.from(document.querySelectorAll(".yy-tpl-overlay-btn"));
    if (!btns.length) return;

    for (const btn of btns) setOverlayButtonFrame(btn, false);
    Promise.all(btns.map((btn) => paintOverlayButton(btn)))
      .then(() => detectInitialActiveButtons())
      .catch(() => {
        detectInitialActiveButtons().catch(() => {});
      });
  }

  function fitTemplateFlowHeight() {
    const rootNode = document.querySelector("[data-campaign-template-flow-root='1']");
    if (!rootNode) return;

    const leftCard = rootNode.querySelector("[data-campaign-template-left-card='1']");
    const rightCard = rootNode.querySelector("[data-campaign-template-right-card='1']");
    if (!leftCard || !rightCard) return;

    leftCard.style.removeProperty("height");
    rightCard.style.removeProperty("height");

    const top = Math.min(leftCard.getBoundingClientRect().top, rightCard.getBoundingClientRect().top);
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
    if (!viewportHeight) return;

    const topGap = Math.max(0, Math.floor(top));
    const mirroredGap = Math.min(topGap, 120);
    const bottomGap = Math.floor(mirroredGap * 0.3);
    const minHeight = 420;
    const height = Math.max(minHeight, Math.floor(viewportHeight - top - bottomGap));
    const px = String(height) + "px";
    leftCard.style.setProperty("height", px, "important");
    rightCard.style.setProperty("height", px, "important");

    let editorHeight = 0;
    const visibleHostSelectors = ["#yyUserModeLeft", "#yyAdvancedModeLeft"];
    for (const sel of visibleHostSelectors) {
      const el = rootNode.querySelector(sel);
      if (!el) continue;
      if (el.classList.contains("hidden")) continue;
      if (el.offsetParent === null) continue;
      editorHeight = Math.floor(el.getBoundingClientRect().height);
      if (editorHeight > 0) break;
    }
    if (!editorHeight) {
      editorHeight = Math.max(320, Math.floor(height * 0.78));
    }

    if (typeof window.yyTplSetEditorHeight === "function") {
      window.yyTplSetEditorHeight(editorHeight);
    } else {
      window.yyTplFlowEditorHeight = editorHeight;
    }
  }

  function scheduleTemplateFlowHeightSync() {
    if (templateFlowHeightRaf) return;
    templateFlowHeightRaf = window.requestAnimationFrame(function () {
      templateFlowHeightRaf = 0;
      fitTemplateFlowHeight();
    });
  }

  window.yyTplScheduleFlowHeightSync = scheduleTemplateFlowHeightSync;

  async function onOverlayClick(ev) {
    const btn = ev?.target?.closest?.(".yy-tpl-overlay-btn");
    if (!btn) return;

    const gid = String(btn.dataset.gid || "").trim();
    const type = String(btn.dataset.type || "").trim(); // colors|fonts
    const name = String(btn.dataset.name || "").trim();

    if (!(gid && (type === "colors" || type === "fonts") && name)) return;

    try {
      const css = await fetchOverlayCss(gid, type, name);
      if (typeof window.yyTplApplyOverlay === "function") window.yyTplApplyOverlay(type, css || "");
      setOverlayActive(btn);
    } catch (_) {}
  }

  function onGlobalTemplateClick(ev) {
    const btn = ev?.target?.closest?.(".yy-tpl-global-btn");
    if (!btn) return;

    const gid = String(btn.dataset.gid || "").trim();
    if (!(gid && /^\d+$/.test(gid))) return;

    const ok = window.confirm(
      "Вы уверены? Текущая работа над шаблоном будет частично или полностью потеряна."
    );
    if (!ok) return;

    try {
      const u = new URL(window.location.href);
      const q = u.searchParams;
      q.set("gl_tpl", gid);
      window.location.search = "?" + q.toString();
    } catch (_) {
      // fallback
      window.location.href = "?gl_tpl=" + encodeURIComponent(gid);
    }
  }

  function init() {
    initOverlayButtonPreviews();
    scheduleTemplateFlowHeightSync();
    document.addEventListener("click", onOverlayClick, true);
    document.addEventListener("click", onGlobalTemplateClick, true);
    window.addEventListener("resize", scheduleTemplateFlowHeightSync);
    window.addEventListener("orientationchange", scheduleTemplateFlowHeightSync);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
