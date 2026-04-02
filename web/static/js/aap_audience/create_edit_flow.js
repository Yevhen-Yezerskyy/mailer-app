// FILE: web/static/js/aap_audience/create_edit_flow.js
// DATE: 2026-04-01
// PURPOSE: Shared unsaved-changes and flow-step navigation logic for create/edit buy/sell pages.

(function () {
  const root = document.getElementById("yy-create-edit-flow-config");
  if (!root) return;

  let config = {};
  try {
    config = JSON.parse(root.textContent || "{}");
  } catch (error) {
    config = {};
  }

  const labels = config.labels || {};
  const forms = Array.from(document.querySelectorAll("[data-dirty-form]"));
  const bypassSubmit = new WeakMap();
  let cityRatingPollTimer = 0;
  let cityRatingPollInFlight = false;
  let contactsTotalPollTimer = 0;
  let contactsTotalPollInFlight = false;
  let contactsSectionPollTimer = 0;
  let contactsSectionPollInFlight = false;

  function isWaitAction(submitter) {
    const action = submitter && submitter.name === "action" ? String(submitter.value || "") : "";
    return (
      action === "process_product" ||
      action === "process_company" ||
      action === "process_geo" ||
      action === "branches_pick" ||
      action === "branches_expand_adjacent" ||
      action === "branches_expand_middlemen" ||
      action === "branches_expand_custom" ||
      action === "branches_save" ||
      action === "branches_recalc_ratings" ||
      action === "branches_refill" ||
      action === "cities_pick_refine" ||
      action === "cities_refill"
    );
  }

  function norm(value) {
    return String(value || "").replace(/\r\n/g, "\n").trim();
  }

  function comparableField(form) {
    return form.querySelector("textarea[name='source_product'], textarea[name='source_company'], textarea[name='source_geo']");
  }

  function summaryValue(form) {
    const key = String(form && form.getAttribute("data-dirty-form") || "").trim();
    if (!key) return "";
    const summaryField = document.querySelector("[data-summary-section='" + key + "']");
    if (!summaryField) return "";
    return norm(summaryField.getAttribute("data-summary-value") || "");
  }

  function fieldDirty(field, savedValue) {
    return !!field && norm(field.value) !== norm(savedValue || "");
  }

  function mappedButtonClass(key) {
    const classMap = window.yyClassMap || document.documentElement.yyClassMap || {};
    const mapped = String(classMap[key] || "").trim();
    return mapped ? (key + " " + mapped) : key;
  }

  function ensureButtonLabel(button) {
    if (!button) return;
    const label = String(button.getAttribute("data-button-label") || "").trim();
    if (label && !String(button.textContent || "").trim()) {
      button.textContent = label;
    }
  }

  function setStandardButtonState(button, enabled) {
    if (!button) return;
    ensureButtonLabel(button);
    button.disabled = !enabled;
    button.setAttribute("class", mappedButtonClass(enabled ? "YY-BUTTON_MAIN_FULL" : "YY-BUTTON_GRAY_FULL"));
  }

  function setStandardInlineButtonState(button, enabled) {
    if (!button) return;
    ensureButtonLabel(button);
    button.disabled = !enabled;
    button.setAttribute("class", mappedButtonClass(enabled ? "YY-BUTTON_MAIN" : "YY-BUTTON_GRAY"));
  }

  function setStandardFullButtonState(button, enabled) {
    if (!button) return;
    ensureButtonLabel(button);
    button.disabled = !enabled;
    button.setAttribute("class", mappedButtonClass(enabled ? "YY-BUTTON_MAIN_FULL" : "YY-BUTTON_GRAY_FULL"));
  }

  function syncOptionalMainButtons(form, currentMainValue) {
    const grid = form ? form.querySelector("[data-main-actions='1']") : null;
    const insertButton = form ? form.querySelector("[data-insert-company='1']") : null;
    const geoDefaultButton = form ? form.querySelector("[data-geo-default='1']") : null;
    const insertAvailable = grid && grid.getAttribute("data-insert-company-available") === "1";
    const empty = String(currentMainValue || "") === "";
    const insertVisible = !!insertAvailable && empty;
    const geoDefaultVisible = !!geoDefaultButton && empty;
    const hasOptionalVisible = insertVisible || geoDefaultVisible;

    if (grid) {
      grid.classList.toggle("grid-cols-3", hasOptionalVisible);
      grid.classList.toggle("grid-cols-2", !hasOptionalVisible);
    }

    if (insertButton) {
      insertButton.classList.toggle("hidden", !insertVisible);
    }

    if (geoDefaultButton) {
      geoDefaultButton.classList.toggle("hidden", !geoDefaultVisible);
    }
  }

  function titleModalErrorNode(form) {
    return form ? form.querySelector("[data-edit-title-error='1']") : null;
  }

  function titleModalErrorWrap(form) {
    return form ? form.querySelector("[data-edit-title-error-wrap='1']") : null;
  }

  function setTitleModalError(form, message) {
    const errorNode = titleModalErrorNode(form);
    const wrapNode = titleModalErrorWrap(form);
    const text = String(message || "").trim();
    if (errorNode) {
      errorNode.textContent = text;
    }
    if (wrapNode) {
      wrapNode.classList.toggle("hidden", !text);
    }
  }

  function updateAudienceTitle(title) {
    const value = String(title || "").trim();
    if (!value) return;
    document.querySelectorAll("[data-audience-title='1']").forEach(function (node) {
      node.textContent = value;
    });
  }

  function branchRateModalErrorNode(form) {
    return form ? form.querySelector("[data-edit-branch-rate-error='1']") : null;
  }

  function branchRateModalErrorWrap(form) {
    return form ? form.querySelector("[data-edit-branch-rate-error-wrap='1']") : null;
  }

  function setBranchRateModalError(form, message) {
    const errorNode = branchRateModalErrorNode(form);
    const wrapNode = branchRateModalErrorWrap(form);
    const text = String(message || "").trim();
    if (errorNode) {
      errorNode.textContent = text;
    }
    if (wrapNode) {
      wrapNode.classList.toggle("hidden", !text);
    }
  }

  function postEditTitle(form, action) {
    const data = new FormData(form);
    data.set("action", action);
    if (window.YYWaitModal && typeof window.YYWaitModal.open === "function") {
      window.YYWaitModal.open();
    }
    return window.fetch(form.action, {
      method: "POST",
      body: data,
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    }).then(function (response) {
      return response.json().catch(function () {
        return { ok: false, error: "Request failed" };
      }).then(function (payload) {
        return { response: response, payload: payload };
      });
    }).finally(function () {
      if (window.YYWaitModal && typeof window.YYWaitModal.close === "function") {
        window.YYWaitModal.close();
      }
    });
  }

  function formDirty(form) {
    return fieldDirty(comparableField(form), summaryValue(form));
  }

  function syncActionButtons(form) {
    const mainField = form.querySelector("textarea[name='source_product'], textarea[name='source_company'], textarea[name='source_geo']");
    const instructionField = form.querySelector("textarea[name='product_ai_command'], textarea[name='company_ai_command'], textarea[name='geo_ai_command']");
    const processMainBtn = form.querySelector("[data-process-main='1']");
    const processInstructionBtn = form.querySelector("[data-process-instruction='1']");
    const saveMainBtn = form.querySelector("[data-save-main='1']");
    const resetMainBtn = form.querySelector("button[name='action'][value='reset_product_context'], button[name='action'][value='reset_company_context'], button[name='action'][value='reset_geo_context']");
    const nextStageBtn = form.querySelector("[data-next-stage='1']");
    const currentMainValue = norm(mainField ? mainField.value : "");
    const savedMainValue = summaryValue(form);
    const mainChanged = !!mainField && currentMainValue !== "" && currentMainValue !== savedMainValue;
    const mainEdited = form.getAttribute("data-main-field-edited") === "1";

    syncOptionalMainButtons(form, currentMainValue);

    if (processMainBtn) {
      setStandardButtonState(processMainBtn, mainEdited && mainChanged);
    }

    if (processInstructionBtn && instructionField) {
      setStandardInlineButtonState(processInstructionBtn, norm(instructionField.value) !== "");
    }

    if (saveMainBtn) {
      saveMainBtn.type = "submit";
      setStandardButtonState(saveMainBtn, mainChanged);
    }

    if (resetMainBtn) {
      setStandardFullButtonState(resetMainBtn, currentMainValue !== "");
    }

    if (nextStageBtn) {
      setStandardFullButtonState(nextStageBtn, savedMainValue !== "");
    }
  }

  function dirtyFormNames(exceptForm) {
    return forms
      .filter(function (form) {
        return form !== exceptForm && formDirty(form);
      })
      .map(function (form) {
        const key = form.getAttribute("data-dirty-form") || "";
        return labels[key] || key;
      });
  }

  function dirtyFormNamesForSubmit(form, submitter) {
    const action = submitter && submitter.name === "action" ? String(submitter.value || "") : "";

    if (
      action === "process_product" ||
      action === "process_company" ||
      action === "process_geo" ||
      action === "save_product" ||
      action === "save_company" ||
      action === "save_geo" ||
      action === "reset_product_context" ||
      action === "reset_company_context" ||
      action === "reset_geo_context"
    ) {
      return dirtyFormNames(form);
    }

    return dirtyFormNames(null);
  }

  function esc(text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function openDirtyModal(names, onContinue, options) {
    const opts = options || {};
    const tpl = document.getElementById("yy-unsaved-changes-template");
    if (!tpl) {
      if (!onContinue) {
        window.alert([opts.title || "", opts.text || "", names.join("\n")].filter(Boolean).join("\n"));
        return;
      }
      if (window.confirm(names.join("\n"))) {
        onContinue();
      }
      return;
    }

    const wrapper = document.createElement("div");
    wrapper.innerHTML = tpl.innerHTML;
    const title = wrapper.querySelector("[data-unsaved-title]");
    const text = wrapper.querySelector("[data-unsaved-text]");
    const list = wrapper.querySelector("[data-unsaved-list]");
    const continueBtn = wrapper.querySelector("[data-unsaved-continue]");
    const closeBtn = wrapper.querySelector("[data-yy-modal-close]");

    if (title && opts.title) {
      title.textContent = opts.title;
    }

    if (text && opts.text) {
      text.textContent = opts.text;
    }

    if (list) {
      list.innerHTML = names.map(function (name) {
        return "<li>" + esc(name) + "</li>";
      }).join("");
    }

    if (!onContinue && continueBtn) {
      continueBtn.remove();
    }

    if (closeBtn && opts.closeLabel) {
      closeBtn.textContent = opts.closeLabel;
    }

    if (window.YYModal && typeof window.YYModal.open === "function") {
      window.YYModal.open("text=" + wrapper.innerHTML);
      if (!onContinue) {
        return;
      }
      const bind = function () {
        const btn = document.querySelector("#yy-modal-body [data-unsaved-continue]");
        if (!btn) {
          window.setTimeout(bind, 0);
          return;
        }
        btn.addEventListener("click", function () {
          if (window.YYModal && typeof window.YYModal.close === "function") {
            window.YYModal.close();
          }
          onContinue();
        }, { once: true });
      };
      bind();
      return;
    }

    if (!onContinue) {
      window.alert([opts.title || "", opts.text || "", names.join("\n")].filter(Boolean).join("\n"));
      return;
    }

    if (window.confirm(names.join("\n"))) {
      onContinue();
    }
  }

  forms.forEach(function (form) {
    syncActionButtons(form);

    const mainField = comparableField(form);
    if (mainField) {
      mainField.addEventListener("input", function () {
        form.setAttribute("data-main-field-edited", "1");
        syncActionButtons(form);
      });
      mainField.addEventListener("change", function () {
        form.setAttribute("data-main-field-edited", "1");
        syncActionButtons(form);
      });
    }

    form.querySelectorAll("textarea").forEach(function (field) {
      if (field === mainField) return;
      field.addEventListener("input", function () {
        syncActionButtons(form);
      });
      field.addEventListener("change", function () {
        syncActionButtons(form);
      });
    });

    form.addEventListener("submit", function (event) {
      const submitter = event.submitter;
      if (bypassSubmit.has(form)) {
        const bypassSubmitter = bypassSubmit.get(form);
        bypassSubmit.delete(form);
        if (isWaitAction(bypassSubmitter) && window.YYWaitModal) window.YYWaitModal.open();
        return;
      }
      const names = dirtyFormNamesForSubmit(form, event.submitter);
      if (!names.length) {
        if (isWaitAction(submitter) && window.YYWaitModal) window.YYWaitModal.open();
        return;
      }
      event.preventDefault();
      openDirtyModal(names, function () {
        bypassSubmit.set(form, submitter || null);
        if (submitter && typeof form.requestSubmit === "function") {
          form.requestSubmit(submitter);
          return;
        }
        form.submit();
      });
    });
  });

  document.querySelectorAll("[data-submit-role='nav']").forEach(function (form) {
    form.addEventListener("submit", function (event) {
      const submitter = event.submitter;
      const names = dirtyFormNames(null);
      if (!names.length) return;
      event.preventDefault();
      openDirtyModal(names, function () {
        if (submitter && typeof form.requestSubmit === "function") {
          form.requestSubmit(submitter);
          return;
        }
        form.submit();
      });
    });
  });

  document.querySelectorAll("[data-dirty-nav]").forEach(function (button) {
    button.addEventListener("click", function () {
      const names = dirtyFormNames(null);
      if (!names.length) {
        window.location.href = button.getAttribute("data-dirty-nav");
        return;
      }
      openDirtyModal(names, function () {
        window.location.href = button.getAttribute("data-dirty-nav");
      });
    });
  });

  document.addEventListener("click", function (event) {
    const suggestBtn = event.target.closest("[data-edit-title-suggest='1']");
    if (!suggestBtn) return;
    const form = suggestBtn.closest("[data-edit-title-form='1']");
    if (!form || suggestBtn.disabled) return;

    event.preventDefault();
    setTitleModalError(form, "");

    postEditTitle(form, "suggest_title").then(function (result) {
      const payload = result.payload || {};
      if (!result.response.ok || !payload.ok) {
        setTitleModalError(form, payload.error || "Request failed");
        return;
      }
      const input = form.querySelector("input[name='title']");
      if (input) {
        input.value = String(payload.title || "");
      }
    }).catch(function () {
      setTitleModalError(form, "Request failed");
    });
  });

  document.addEventListener("submit", function (event) {
    const form = event.target.closest("[data-edit-title-form='1']");
    if (!form) return;

    event.preventDefault();
    setTitleModalError(form, "");

    postEditTitle(form, "save_title").then(function (result) {
      const payload = result.payload || {};
      if (!result.response.ok || !payload.ok) {
        setTitleModalError(form, payload.error || "Request failed");
        return;
      }
      updateAudienceTitle(payload.title || "");
      if (window.YYModal && typeof window.YYModal.close === "function") {
        window.YYModal.close();
      }
    }).catch(function () {
      setTitleModalError(form, "Request failed");
    });
  });

  document.addEventListener("submit", function (event) {
    const form = event.target.closest("[data-edit-branch-rate-form='1']");
    if (!form) return;

    event.preventDefault();
    setBranchRateModalError(form, "");

    const data = new FormData(form);
    if (window.YYWaitModal && typeof window.YYWaitModal.open === "function") {
      window.YYWaitModal.open();
    }

    window.fetch(form.action, {
      method: "POST",
      body: data,
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    }).then(function (response) {
      return response.json().catch(function () {
        return { ok: false, error: "Request failed" };
      }).then(function (payload) {
        return { response: response, payload: payload };
      });
    }).then(function (result) {
      const payload = result.payload || {};
      if (!result.response.ok || !payload.ok) {
        setBranchRateModalError(form, payload.error || "Request failed");
        return;
      }
      window.location.reload();
    }).catch(function () {
      setBranchRateModalError(form, "Request failed");
    }).finally(function () {
      if (window.YYWaitModal && typeof window.YYWaitModal.close === "function") {
        window.YYWaitModal.close();
      }
    });
  });

  document.addEventListener("click", function (event) {
    const insertBtn = event.target.closest("[data-insert-company-select='1']");
    if (!insertBtn) return;

    event.preventDefault();

    const card = insertBtn.closest(".YY-CARD_WHITE");
    const sourceNode = card ? card.querySelector("[data-insert-company-value='1']") : null;
    const mainField = document.querySelector("[data-dirty-form='company'] textarea[name='source_company']");
    const form = mainField ? mainField.closest("[data-dirty-form]") : null;
    const value = sourceNode && "value" in sourceNode ? String(sourceNode.value || "") : "";

    if (!mainField || !form || !value.trim()) {
      return;
    }

    mainField.value = value;
    form.setAttribute("data-main-field-edited", "1");
    mainField.dispatchEvent(new Event("input", { bubbles: true }));
    mainField.dispatchEvent(new Event("change", { bubbles: true }));

    if (window.YYModal && typeof window.YYModal.close === "function") {
      window.YYModal.close();
    }
  });

  document.addEventListener("click", function (event) {
    const geoBtn = event.target.closest("[data-geo-default='1']");
    if (!geoBtn) return;

    event.preventDefault();

    const form = geoBtn.closest("[data-dirty-form='geo']");
    const mainField = form ? form.querySelector("textarea[name='source_geo']") : null;
    const processBtn = form ? form.querySelector("[data-process-main='1']") : null;
    const fillValue = String(geoBtn.getAttribute("data-geo-default-value") || "").trim();

    if (!form || !mainField || !processBtn || !fillValue) {
      return;
    }

    mainField.value = fillValue;
    form.setAttribute("data-main-field-edited", "1");
    mainField.dispatchEvent(new Event("input", { bubbles: true }));
    mainField.dispatchEvent(new Event("change", { bubbles: true }));

    if (typeof form.requestSubmit === "function") {
      form.requestSubmit(processBtn);
      return;
    }
    processBtn.click();
  });

  function syncBranchDeleteState() {
    const deleteInput = document.querySelector("[data-branches-delete-ids='1']");
    const deleteSubmit = document.querySelector("[data-branches-delete-submit='1']");
    const deleteActions = document.querySelector("[data-branches-delete-actions='1']");
    if (!deleteInput || !deleteSubmit || !deleteActions) return;
    const selected = [];
    const seen = new Set();
    Array.from(document.querySelectorAll("[data-branch-row='1'][data-delete-selected='1']")).forEach(function (row) {
      String(row.getAttribute("data-branch-ids") || "")
        .split(",")
        .map(function (value) { return String(value || "").trim(); })
        .filter(Boolean)
        .forEach(function (value) {
          if (seen.has(value)) return;
          seen.add(value);
          selected.push(value);
        });
    });
    deleteInput.value = selected.join(",");
    deleteActions.classList.toggle("hidden", selected.length === 0);
  }

  function syncCityDeleteState() {
    const deleteInput = document.querySelector("[data-cities-delete-ids='1']");
    const deleteSubmit = document.querySelector("[data-cities-delete-submit='1']");
    const deleteActions = document.querySelector("[data-cities-delete-actions='1']");
    if (!deleteInput || !deleteSubmit || !deleteActions) return;
    const selected = [];
    const seen = new Set();
    Array.from(document.querySelectorAll("[data-city-row='1'][data-delete-selected='1']")).forEach(function (row) {
      String(row.getAttribute("data-city-ids") || "")
        .split(",")
        .map(function (value) { return String(value || "").trim(); })
        .filter(Boolean)
        .forEach(function (value) {
          if (seen.has(value)) return;
          seen.add(value);
          selected.push(value);
        });
    });
    deleteInput.value = selected.join(",");
    deleteActions.classList.toggle("hidden", selected.length === 0);
  }

  function cityRatingPanel() {
    return document.querySelector("[data-city-rating-panel='1']");
  }

  function contactsTotalPanel() {
    return document.querySelector("[data-contacts-total-panel='1']");
  }

  function contactsSectionsRoot() {
    return document.querySelector("[data-contacts-sections-root='1']");
  }

  function contactsSectionButtons() {
    return Array.from(document.querySelectorAll("[data-contacts-section-button]"));
  }

  function contactsActiveSectionKey() {
    const rootNode = contactsSectionsRoot();
    if (!rootNode) return "";
    return String(rootNode.getAttribute("data-contacts-active-section") || "").trim();
  }

  function contactsSectionWrapper(sectionKey) {
    const key = String(sectionKey || contactsActiveSectionKey()).trim();
    if (!key) return null;
    return document.querySelector("[data-contacts-section-wrapper='" + key + "']");
  }

  function syncContactsSectionButtons() {
    const activeKey = contactsActiveSectionKey();
    contactsSectionButtons().forEach(function (button) {
      const buttonKey = String(button.getAttribute("data-contacts-section-button") || "").trim();
      const activeClass = String(button.getAttribute("data-contacts-section-active-class") || "").trim();
      const inactiveClass = String(button.getAttribute("data-contacts-section-inactive-class") || "").trim();
      const className = buttonKey && buttonKey === activeKey ? activeClass : inactiveClass;
      button.disabled = false;
      if (className) {
        button.setAttribute("class", mappedButtonClass(className));
      }
    });
  }

  function showContactsSection(sectionKey) {
    const rootNode = contactsSectionsRoot();
    const nextKey = String(sectionKey || "").trim();
    if (!rootNode || !nextKey) return;
    rootNode.setAttribute("data-contacts-active-section", nextKey);
    Array.from(document.querySelectorAll("[data-contacts-section-wrapper]")).forEach(function (wrapper) {
      const wrapperKey = String(wrapper.getAttribute("data-contacts-section-wrapper") || "").trim();
      wrapper.hidden = wrapperKey !== nextKey;
    });
    syncContactsSectionButtons();
  }

  function formatContactsTotal(value) {
    const number = Number(value || 0);
    if (!Number.isFinite(number)) {
      return String(value || "0");
    }
    return new Intl.NumberFormat("ru-RU").format(number);
  }

  function stopCityRatingPolling() {
    if (!cityRatingPollTimer) return;
    window.clearInterval(cityRatingPollTimer);
    cityRatingPollTimer = 0;
  }

  function stopContactsTotalPolling() {
    if (!contactsTotalPollTimer) return;
    window.clearInterval(contactsTotalPollTimer);
    contactsTotalPollTimer = 0;
  }

  function stopContactsSectionPolling() {
    if (!contactsSectionPollTimer) return;
    window.clearInterval(contactsSectionPollTimer);
    contactsSectionPollTimer = 0;
  }

  function startCityRatingPolling() {
    stopCityRatingPolling();
    const panel = cityRatingPanel();
    if (!panel) return;
    if (panel.getAttribute("data-city-rating-running") !== "1") return;
    cityRatingPollTimer = window.setInterval(refreshCityRatingPanel, 2000);
  }

  function startContactsTotalPolling() {
    stopContactsTotalPolling();
    const panel = contactsTotalPanel();
    if (!panel) return;
    if (!String(panel.getAttribute("data-contacts-total-url") || "").trim()) return;
    contactsTotalPollTimer = window.setInterval(refreshContactsTotalPanel, 10000);
  }

  function startContactsSectionPolling() {
    stopContactsSectionPolling();
    const wrapper = contactsSectionWrapper();
    if (!wrapper) return;
    if (wrapper.getAttribute("data-contacts-section-running") !== "1") return;
    if (!String(wrapper.getAttribute("data-contacts-section-url") || "").trim()) return;
    contactsSectionPollTimer = window.setInterval(refreshContactsSectionPanel, 10000);
  }

  function refreshCityRatingPanel() {
    const panel = cityRatingPanel();
    if (!panel || cityRatingPollInFlight) return;
    const url = String(panel.getAttribute("data-city-rating-partial-url") || "").trim();
    if (!url) return;

    cityRatingPollInFlight = true;
    window.fetch(url, {
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    }).then(function (response) {
      return response.text();
    }).then(function (html) {
      const wrapper = document.createElement("div");
      wrapper.innerHTML = html;
      const nextPanel = wrapper.querySelector("[data-city-rating-panel='1']");
      const currentPanel = cityRatingPanel();
      if (!nextPanel || !currentPanel) return;
      currentPanel.replaceWith(nextPanel);
      syncCityDeleteState();
      startCityRatingPolling();
    }).catch(function () {
    }).finally(function () {
      cityRatingPollInFlight = false;
    });
  }

  function refreshContactsTotalPanel() {
    const panel = contactsTotalPanel();
    const valueNode = panel ? panel.querySelector("[data-contacts-total-value='1']") : null;
    if (!panel || !valueNode || contactsTotalPollInFlight) return;

    const url = String(panel.getAttribute("data-contacts-total-url") || "").trim();
    if (!url) return;

    contactsTotalPollInFlight = true;
    window.fetch(url, {
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    }).then(function (response) {
      return response.json().catch(function () {
        return { ok: false };
      }).then(function (payload) {
        return { response: response, payload: payload };
      });
    }).then(function (result) {
      const payload = result.payload || {};
      if (!result.response.ok || !payload.ok) {
        stopContactsTotalPolling();
        return;
      }
      valueNode.textContent = formatContactsTotal(payload.contacts_total || 0);
      if (payload.is_active === false) {
        const collectWrapper = contactsSectionWrapper("collect");
        if (collectWrapper) {
          collectWrapper.setAttribute("data-contacts-section-running", "0");
        }
        stopContactsTotalPolling();
        stopContactsSectionPolling();
      }
    }).catch(function () {
    }).finally(function () {
      contactsTotalPollInFlight = false;
    });
  }

  function refreshContactsSectionPanel(urlOverride) {
    const wrapper = contactsSectionWrapper();
    if (!wrapper || contactsSectionPollInFlight) return;

    const defaultUrl = String(wrapper.getAttribute("data-contacts-section-url") || "").trim();
    const url = String(urlOverride || "").trim() || defaultUrl;
    if (!url) return;

    contactsSectionPollInFlight = true;
    window.fetch(url, {
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    }).then(function (response) {
      return response.text();
    }).then(function (html) {
      const sectionKey = String(wrapper.getAttribute("data-contacts-section-wrapper") || "").trim();
      const inner = wrapper.querySelector("[data-contacts-section-inner='" + sectionKey + "']");
      if (!sectionKey || !inner) return;
      inner.style.transition = "opacity 140ms ease";
      inner.style.opacity = "0.35";
      window.setTimeout(function () {
        inner.innerHTML = html;
        inner.style.opacity = "1";
        startContactsSectionPolling();
      }, 140);
    }).catch(function () {
    }).finally(function () {
      contactsSectionPollInFlight = false;
    });
  }

  function refreshContactsAllSectionPanel(pageValue, queryValue) {
    const page = String(pageValue || "").trim();
    if (!/^\d+$/.test(page)) return;
    showContactsSection("all");
    const wrapper = contactsSectionWrapper("all");
    if (!wrapper) return;
    const baseUrl = String(wrapper.getAttribute("data-contacts-section-url") || "").trim();
    if (!baseUrl) return;
    const query = String(queryValue || "").trim();
    const args = ["page=" + encodeURIComponent(page)];
    if (query) {
      args.push("q=" + encodeURIComponent(query));
    }
    const separator = baseUrl.indexOf("?") === -1 ? "?" : "&";
    refreshContactsSectionPanel(baseUrl + separator + args.join("&"));
  }

  function scrollBranchesToLastGreen() {
    const box = document.querySelector("[data-branches-scroll-box='1']");
    if (!box) return;
    if (box.getAttribute("data-has-green-branches") !== "1") return;
    if (box.getAttribute("data-has-yellow-branches") !== "1") return;

    const lastGreen = box.querySelector("[data-branch-last-green='1']");
    if (!lastGreen) return;
    const firstYellow = box.querySelector("[data-branch-first-yellow='1']");
    if (!firstYellow) {
      box.scrollTop = Math.max(0, lastGreen.offsetTop);
      return;
    }

    const greenRect = lastGreen.getBoundingClientRect();
    const yellowRect = firstYellow.getBoundingClientRect();
    const gap = Math.max(0, yellowRect.top - greenRect.bottom);
    const targetTop = firstYellow.offsetTop - lastGreen.offsetHeight - gap - 70;
    box.scrollTop = Math.max(0, targetTop);
  }

  document.addEventListener("click", function (event) {
    const allPageLink = event.target.closest("[data-contacts-all-page]");
    if (allPageLink) {
      event.preventDefault();
      const page = String(allPageLink.getAttribute("data-contacts-all-page") || "").trim();
      const query = String(allPageLink.getAttribute("data-contacts-all-q") || "").trim();
      refreshContactsAllSectionPanel(page, query);
      return;
    }

    const allSearchButton = event.target.closest("[data-contacts-all-search-button='1']");
    if (allSearchButton) {
      event.preventDefault();
      const searchWrap = allSearchButton.closest("[data-contacts-all-search-wrap='1']");
      const input = searchWrap ? searchWrap.querySelector("[data-contacts-all-search-input='1']") : null;
      const query = String(input && input.value ? input.value : "").trim();
      refreshContactsAllSectionPanel("1", query);
      return;
    }

    const allSearchClear = event.target.closest("[data-contacts-all-search-clear='1']");
    if (allSearchClear) {
      event.preventDefault();
      refreshContactsAllSectionPanel("1", "");
      return;
    }

    const sectionButton = event.target.closest("[data-contacts-section-button]");
    if (sectionButton) {
      const sectionKey = String(sectionButton.getAttribute("data-contacts-section-button") || "").trim();
      if (sectionKey) {
        showContactsSection(sectionKey);
        refreshContactsSectionPanel();
        startContactsSectionPolling();
      }
      return;
    }

    const button = event.target.closest("[data-branch-delete-toggle='1']");
    if (!button) return;
    const row = button.closest("[data-branch-row='1']");
    if (!row) return;
    const selected = row.getAttribute("data-delete-selected") === "1";
    row.setAttribute("data-delete-selected", selected ? "0" : "1");
    if (!selected) {
      row.classList.remove("bg-[#f0fff0]");
      row.classList.remove("bg-[#FFF7E0]");
      row.classList.add("bg-[#fff3f3]");
    } else {
      row.classList.remove("bg-[#f0fff0]");
      row.classList.remove("bg-[#FFF7E0]");
      row.classList.remove("bg-[#fff3f3]");
      if (row.getAttribute("data-branch-yellow") === "1") {
        row.classList.add("bg-[#FFF7E0]");
      } else {
        row.classList.add("bg-[#f0fff0]");
      }
    }
    syncBranchDeleteState();
  });

  document.addEventListener("click", function (event) {
    const deleteCancel = event.target.closest("[data-branches-delete-cancel='1']");
    if (!deleteCancel) return;
      const deleteInput = document.querySelector("[data-branches-delete-ids='1']");
      Array.from(document.querySelectorAll("[data-branch-row='1'][data-delete-selected='1']")).forEach(function (row) {
        row.setAttribute("data-delete-selected", "0");
        row.classList.remove("bg-[#fff3f3]");
        row.classList.remove("bg-[#f0fff0]");
        row.classList.remove("bg-[#FFF7E0]");
        if (row.getAttribute("data-branch-yellow") === "1") {
          row.classList.add("bg-[#FFF7E0]");
        } else {
          row.classList.add("bg-[#f0fff0]");
        }
      });
      if (deleteInput) {
        deleteInput.value = "";
      }
      syncBranchDeleteState();
  });

  document.addEventListener("keydown", function (event) {
    if (event.key !== "Enter") return;
    const input = event.target.closest("[data-contacts-all-search-input='1']");
    if (!input) return;
    event.preventDefault();
    const query = String(input.value || "").trim();
    refreshContactsAllSectionPanel("1", query);
  });

  scrollBranchesToLastGreen();
  syncBranchDeleteState();

  document.addEventListener("click", function (event) {
    const button = event.target.closest("[data-city-delete-toggle='1']");
    if (!button) return;
    const row = button.closest("[data-city-row='1']");
    if (!row) return;
    const selected = row.getAttribute("data-delete-selected") === "1";
    row.setAttribute("data-delete-selected", selected ? "0" : "1");
    if (!selected) {
      row.classList.remove("bg-[#f0fff0]");
      row.classList.remove("bg-[#FFF7E0]");
      row.classList.add("bg-[#fff3f3]");
    } else {
      row.classList.remove("bg-[#f0fff0]");
      row.classList.remove("bg-[#FFF7E0]");
      row.classList.remove("bg-[#fff3f3]");
      if (row.getAttribute("data-city-yellow") === "1") {
        row.classList.add("bg-[#FFF7E0]");
      } else {
        row.classList.add("bg-[#f0fff0]");
      }
    }
    syncCityDeleteState();
  });

  document.addEventListener("click", function (event) {
    const deleteCancel = event.target.closest("[data-cities-delete-cancel='1']");
    if (!deleteCancel) return;
      const deleteInput = document.querySelector("[data-cities-delete-ids='1']");
      Array.from(document.querySelectorAll("[data-city-row='1'][data-delete-selected='1']")).forEach(function (row) {
        row.setAttribute("data-delete-selected", "0");
        row.classList.remove("bg-[#fff3f3]");
        row.classList.remove("bg-[#f0fff0]");
        row.classList.remove("bg-[#FFF7E0]");
        if (row.getAttribute("data-city-yellow") === "1") {
          row.classList.add("bg-[#FFF7E0]");
        } else {
          row.classList.add("bg-[#f0fff0]");
        }
      });
      if (deleteInput) {
        deleteInput.value = "";
      }
      syncCityDeleteState();
  });

  syncCityDeleteState();
  syncContactsSectionButtons();
  showContactsSection(contactsActiveSectionKey() || "collect");
  startCityRatingPolling();
  startContactsTotalPolling();
  startContactsSectionPolling();
})();
