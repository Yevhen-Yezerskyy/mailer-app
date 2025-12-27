// FILE: web/static/js/clar_items.js  (обновлено — 2025-12-27)
// PURPOSE: локальный JS для страницы clar (state=edit): вкладки Города/Категории, выбор элемента, поиск, заполнение формы rate.
// NOTE: аккуратно с classList — добавляем/убираем только одиночные токены.

(function () {
  function q(root, sel) { return root.querySelector(sel); }
  function qa(root, sel) { return Array.from(root.querySelectorAll(sel)); }

  function splitTokens(s) {
    return (s || "").toString().trim().split(/\s+/).filter(Boolean);
  }

  function setButtonActive(btn, isActive) {
    const active = splitTokens(btn.dataset.activeClass);
    const idle = splitTokens(btn.dataset.idleClass);

    active.forEach(t => btn.classList.remove(t));
    idle.forEach(t => btn.classList.remove(t));

    (isActive ? active : idle).forEach(t => btn.classList.add(t));
  }

  function showPanel(box, tab) {
    qa(box, "[data-panel]").forEach(p => {
      const key = (p.getAttribute("data-panel") || "").trim();
      if (key === tab) p.classList.remove("hidden");
      else p.classList.add("hidden");
    });

    qa(box, "[data-tab]").forEach(b => {
      setButtonActive(b, (b.dataset.tab || "") === tab);
    });
  }

  function normalize(s) {
    return (s || "").toString().trim().toLowerCase();
  }

  function applySearch(box, kind, needle) {
    const n = normalize(needle);
    qa(box, `[data-clar-item="${kind}"]`).forEach(el => {
      const text = normalize(el.dataset.valueText || "");
      el.style.display = (!n || text.includes(n)) ? "" : "none";
    });
  }

  function clearSelected(box, kind) {
    qa(box, `[data-clar-item="${kind}"]`).forEach(el => {
      el.classList.remove("bg-[#f2fff3]");
      el.classList.remove("border-slate-400");
      el.classList.add("border-transparent");
    });
  }

  function updateRateButtons(root) {
    qa(root, "[data-rate-form]").forEach(form => {
      const valueId = (q(form, 'input[name="value_id"]')?.value || "").trim();
      const rate = (q(form, 'input[name="rate"]')?.value || "").trim();
      const btn = q(form, 'button[type="submit"]');
      if (!btn) return;
      btn.disabled = !(valueId && rate);
    });
  }

function selectItem(box, kind, el) {
  clearSelected(box, kind);

  el.classList.add("bg-[#f2fff3]");
  el.classList.add("border-slate-400");
  el.classList.remove("border-transparent");

  const valueId = (el.dataset.valueId || "").trim();
  const valueText = (el.dataset.valueText || "").trim();
  const rate = (el.dataset.rate || "").trim(); // <-- FIX: берём rate

  const form = q(box, `[data-rate-form="${kind}"]`);
  if (!form) return;

  const inpValue = q(form, 'input[name="value_id"]');
  if (inpValue) inpValue.value = valueId;

  const label = q(form, `[data-selected-label="${kind}"]`);
  if (label && "value" in label) label.value = valueText;

  const inpRate = q(form, 'input[name="rate"]');
  if (inpRate) inpRate.value = rate; // <-- FIX: подставляем rate, а не ""

  updateRateButtons(box);
}

  function initOne(box) {
    qa(box, "[data-tab]").forEach(btn => {
      btn.addEventListener("click", function () {
        const tab = (btn.dataset.tab || "").trim();
        if (!tab) return;
        showPanel(box, tab);
      });
    });

    showPanel(box, "branches");

    qa(box, '[data-clar-item="branch"]').forEach(el => {
      el.addEventListener("click", function () {
        selectItem(box, "branch", el);
      });
    });

    qa(box, '[data-clar-item="city"]').forEach(el => {
      el.addEventListener("click", function () {
        selectItem(box, "city", el);
      });
    });

    const searchBranch = q(box, '[data-clar-search="branch"]');
    if (searchBranch) {
      searchBranch.addEventListener("input", function () {
        applySearch(box, "branch", searchBranch.value);
      });
    }

    const searchCity = q(box, '[data-clar-search="city"]');
    if (searchCity) {
      searchCity.addEventListener("input", function () {
        applySearch(box, "city", searchCity.value);
      });
    }

    box.addEventListener("input", function (e) {
      const t = e.target;
      if (!t) return;
      if (t.closest("[data-rate-form]")) updateRateButtons(box);
    });

    box.addEventListener("change", function (e) {
      const t = e.target;
      if (!t) return;
      if (t.closest("[data-rate-form]")) updateRateButtons(box);
    });

    updateRateButtons(box);
  }

  document.addEventListener("DOMContentLoaded", function () {
    qa(document, '[data-yy-clar-items="1"]').forEach(initOne);
  });
})();
