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
  let contactsPollTimer = 0;
  let contactsTotalPollInFlight = false;
  let contactsSectionPollInFlight = false;
  let contactsSectionRefreshQueued = false;
  let contactsQueuedSectionUrl = "";
  let flowToggleReadyPollTimer = 0;
  let flowToggleReadyPollInFlight = false;
  const CONTACTS_POLL_INTERVAL_MS = 5000;
  const geoTitleAutogenPending = !!config.geoTitleAutogenPending;
  const i18n = window.yyI18n || (document.documentElement && document.documentElement.yyI18n) || {};
  const t = (key, fallback) => {
    const v = i18n[key];
    return typeof v === "string" && v.trim() ? v : fallback;
  };

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

  function isGeoStepUrl(url) {
    const raw = String(url || "").trim();
    if (!raw) return false;
    try {
      const parsed = new URL(raw, window.location.origin);
      return parsed.pathname.indexOf("/geo/") !== -1 || /\/geo$/.test(parsed.pathname);
    } catch (e) {
      return raw.indexOf("/geo/") !== -1 || /\/geo$/.test(raw);
    }
  }

  function maybeOpenGeoEnterWait(url) {
    if (!geoTitleAutogenPending) return;
    if (!isGeoStepUrl(url)) return;
    if (window.YYWaitModal && typeof window.YYWaitModal.open === "function") {
      window.YYWaitModal.open();
    }
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

  function pauseInfoModalErrorNode(form) {
    return form ? form.querySelector("[data-pause-info-error='1']") : null;
  }

  function pauseInfoModalErrorWrap(form) {
    return form ? form.querySelector("[data-pause-info-error-wrap='1']") : null;
  }

  function setPauseInfoModalError(form, message) {
    const errorNode = pauseInfoModalErrorNode(form);
    const wrapNode = pauseInfoModalErrorWrap(form);
    const text = String(message || "").trim();
    if (errorNode) {
      errorNode.textContent = text;
    }
    if (wrapNode) {
      wrapNode.classList.toggle("hidden", !text);
    }
  }

  function readCookie(name) {
    const key = String(name || "").trim();
    if (!key) return "";
    const source = String(document.cookie || "");
    if (!source) return "";
    const parts = source.split(";");
    for (let i = 0; i < parts.length; i += 1) {
      const part = String(parts[i] || "").trim();
      if (!part) continue;
      const eq = part.indexOf("=");
      const rawName = eq === -1 ? part : part.slice(0, eq);
      if (rawName !== key) continue;
      const rawValue = eq === -1 ? "" : part.slice(eq + 1);
      try {
        return decodeURIComponent(rawValue);
      } catch (e) {
        return rawValue;
      }
    }
    return "";
  }

  function resolveFormActionUrl(form) {
    const fromAttr = form && typeof form.getAttribute === "function"
      ? String(form.getAttribute("action") || "").trim()
      : "";
    if (fromAttr) return fromAttr;
    try {
      return String((form && form.action) || "").trim();
    } catch (e) {
      return "";
    }
  }

  function postEditTitle(form, action) {
    const data = new FormData(form);
    data.set("action", action);
    const actionUrl = resolveFormActionUrl(form) || window.location.href;
    if (window.YYWaitModal && typeof window.YYWaitModal.open === "function") {
      window.YYWaitModal.open();
    }
    return window.fetch(actionUrl, {
      method: "POST",
      body: data,
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    }).then(function (response) {
      return response.json().catch(function () {
        return { ok: false, error: t("request_failed", "Request failed") };
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

  function handleGptUnavailablePayload(payload) {
    const data = payload && typeof payload === "object" ? payload : null;
    if (!data || !data.gpt_unavailable) return false;
    window.location.reload();
    return true;
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
      const targetUrl = button.getAttribute("data-dirty-nav");
      const names = dirtyFormNames(null);
      if (!names.length) {
        maybeOpenGeoEnterWait(targetUrl);
        window.location.href = targetUrl;
        return;
      }
      openDirtyModal(names, function () {
        maybeOpenGeoEnterWait(targetUrl);
        window.location.href = targetUrl;
      });
    });
  });

  document.addEventListener("click", function (event) {
    const conRateHelpBtn = event.target.closest("[data-con-rate-help-open='1']");
    if (conRateHelpBtn) {
      event.preventDefault();
      const tpl = document.getElementById("yy-con-rate-help-template");
      if (tpl && window.YYModal && typeof window.YYModal.open === "function") {
        window.YYModal.open("text=" + tpl.innerHTML);
      }
      return;
    }

    const pairRateHelpBtn = event.target.closest("[data-pair-rate-help-open='1']");
    if (pairRateHelpBtn) {
      event.preventDefault();
      const tpl = document.getElementById("yy-pair-rate-help-template");
      if (tpl && window.YYModal && typeof window.YYModal.open === "function") {
        window.YYModal.open("text=" + tpl.innerHTML);
      }
      return;
    }

    const citiesHashHelpBtn = event.target.closest("[data-cities-hash-help-open='1']");
    if (citiesHashHelpBtn) {
      event.preventDefault();
      const tpl = document.getElementById("yy-cities-hash-help-template");
      if (tpl && window.YYModal && typeof window.YYModal.open === "function") {
        window.YYModal.open("text=" + tpl.innerHTML);
      }
      return;
    }

    const branchesHashHelpBtn = event.target.closest("[data-branches-hash-help-open='1']");
    if (branchesHashHelpBtn) {
      event.preventDefault();
      const tpl = document.getElementById("yy-branches-hash-help-template");
      if (tpl && window.YYModal && typeof window.YYModal.open === "function") {
        window.YYModal.open("text=" + tpl.innerHTML);
      }
      return;
    }

    const stepHelpBtn = event.target.closest("[data-step-help-open='1']");
    if (stepHelpBtn) {
      event.preventDefault();
      const tpl = document.getElementById("yy-step-help-template");
      if (tpl && window.YYModal && typeof window.YYModal.open === "function") {
        window.YYModal.open("text=" + tpl.innerHTML);
      }
      return;
    }

    const suggestBtn = event.target.closest("[data-edit-title-suggest='1']");
    if (!suggestBtn) return;
    const form = suggestBtn.closest("[data-edit-title-form='1']");
    if (!form || suggestBtn.disabled) return;

    event.preventDefault();
    setTitleModalError(form, "");

    postEditTitle(form, "suggest_title").then(function (result) {
      const payload = result.payload || {};
      if (!result.response.ok || !payload.ok) {
        if (handleGptUnavailablePayload(payload)) {
          return;
        }
        setTitleModalError(form, payload.error || t("request_failed", "Request failed"));
        return;
      }
      const input = form.querySelector("input[name='title']");
      if (input) {
        input.value = String(payload.title || "");
      }
    }).catch(function () {
      setTitleModalError(form, t("request_failed", "Request failed"));
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
        if (handleGptUnavailablePayload(payload)) {
          return;
        }
        setTitleModalError(form, payload.error || t("request_failed", "Request failed"));
        return;
      }
      updateAudienceTitle(payload.title || "");
      if (window.YYModal && typeof window.YYModal.close === "function") {
        window.YYModal.close();
      }
    }).catch(function () {
      setTitleModalError(form, t("request_failed", "Request failed"));
    });
  });

  if (!window.__YY_PAUSE_INFO_HANDLER_BOUND__) {
    document.addEventListener("submit", function (event) {
      const form = event.target.closest("[data-pause-info-continue-form='1']");
      if (!form) return;

      event.preventDefault();
      setPauseInfoModalError(form, "");

      const data = new FormData(form);
      const actionUrl = resolveFormActionUrl(form) || window.location.href;
      const csrfInput = form.querySelector("input[name='csrfmiddlewaretoken']");
      const csrfFromInput = csrfInput ? String(csrfInput.value || "").trim() : "";
      const csrfFromCookie = String(readCookie("csrftoken") || "").trim();
      const csrfToken = csrfFromInput || csrfFromCookie;
      if (!String(data.get("csrfmiddlewaretoken") || "").trim() && csrfToken) {
        data.set("csrfmiddlewaretoken", csrfToken);
      }

      if (window.YYWaitModal && typeof window.YYWaitModal.open === "function") {
        window.YYWaitModal.open();
      }

      window.fetch(actionUrl, {
        method: "POST",
        body: data,
        credentials: "same-origin",
        headers: {
          "X-Requested-With": "XMLHttpRequest",
          "X-CSRFToken": csrfToken,
        },
      }).then(function (response) {
        return response.text().then(function (text) {
          let payload = {};
          try {
            payload = JSON.parse(String(text || ""));
          } catch (e) {
            payload = {};
          }
          return { response: response, payload: payload, text: String(text || "") };
        });
      }).then(function (result) {
        const payload = result.payload || {};
        if (result.response.ok) {
          if (payload.ok === false) {
            if (handleGptUnavailablePayload(payload)) {
              return;
            }
            setPauseInfoModalError(form, payload.error || t("request_failed", "Request failed"));
            return;
          }
          window.location.reload();
          return;
        }

        const bodyText = String(result.text || "");
        if (handleGptUnavailablePayload(payload)) {
          return;
        }
        if (payload && payload.error) {
          setPauseInfoModalError(form, payload.error);
          return;
        }
        if (bodyText && /csrf/i.test(bodyText)) {
          setPauseInfoModalError(form, t("csrf_validation_failed", "CSRF validation failed"));
          return;
        }
        setPauseInfoModalError(form, t("request_failed", "Request failed"));
      }).catch(function () {
        setPauseInfoModalError(form, t("request_failed", "Request failed"));
      }).finally(function () {
        if (window.YYWaitModal && typeof window.YYWaitModal.close === "function") {
          window.YYWaitModal.close();
        }
      });
    });
  }

  document.addEventListener("submit", function (event) {
    const form = event.target.closest("[data-edit-branch-rate-form='1']");
    if (!form) return;

    event.preventDefault();
    setBranchRateModalError(form, "");

    const data = new FormData(form);
    const actionUrl = resolveFormActionUrl(form) || window.location.href;
    if (window.YYWaitModal && typeof window.YYWaitModal.open === "function") {
      window.YYWaitModal.open();
    }

    window.fetch(actionUrl, {
      method: "POST",
      body: data,
      credentials: "same-origin",
      headers: {
        "X-Requested-With": "XMLHttpRequest",
      },
    }).then(function (response) {
      return response.json().catch(function () {
        return { ok: false, error: t("request_failed", "Request failed") };
      }).then(function (payload) {
        return { response: response, payload: payload };
      });
    }).then(function (result) {
      const payload = result.payload || {};
      if (!result.response.ok || !payload.ok) {
        if (handleGptUnavailablePayload(payload)) {
          return;
        }
        setBranchRateModalError(form, payload.error || t("request_failed", "Request failed"));
        return;
      }
      window.location.reload();
    }).catch(function () {
      setBranchRateModalError(form, t("request_failed", "Request failed"));
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

  function contactsExhausted() {
    const panel = contactsTotalPanel();
    if (!panel) return false;
    return String(panel.getAttribute("data-contacts-exhausted") || "").trim() === "1";
  }

  function contactsSectionsRoot() {
    return document.querySelector("[data-contacts-sections-root='1']");
  }

  function contactsFlowActive() {
    const rootNode = contactsSectionsRoot();
    if (!rootNode) return false;
    return String(rootNode.getAttribute("data-contacts-flow-active") || "").trim() === "1";
  }

  function contactsTopStateNode(stateKey) {
    const key = String(stateKey || "").trim();
    if (!key) return null;
    return document.querySelector("[data-contacts-top-state='" + key + "']");
  }

  function syncContactsExhaustedUi(isExhausted) {
    const exhausted = !!isExhausted;
    const collectingLabel = document.querySelector("[data-contacts-active-label='collecting']");
    const collectedLabel = document.querySelector("[data-contacts-active-label='collected']");
    const spinner = document.querySelector("[data-contacts-active-spinner='1']");
    setNodeForceVisible(collectingLabel, !exhausted);
    setNodeForceVisible(collectedLabel, exhausted);
    setNodeForceVisible(spinner, !exhausted);
  }

  function setNodeForceVisible(node, visible) {
    if (!node) return;
    if (visible) {
      node.removeAttribute("hidden");
      node.style.removeProperty("display");
      return;
    }
    node.setAttribute("hidden", "hidden");
    node.style.setProperty("display", "none", "important");
  }

  function flowToggleReadyRoot() {
    return document.querySelector("[data-flow-toggle-ready-root='1']");
  }

  function applyFlowToggleState(ready, userActive) {
    const rootNode = flowToggleReadyRoot();
    if (!rootNode) return;

    const isReady = !!ready;
    setNodeForceVisible(rootNode, isReady);
    if (!isReady) return;

    const isActive = !!userActive;
    setNodeForceVisible(rootNode.querySelector("[data-flow-toggle-state='on']"), isActive);
    setNodeForceVisible(rootNode.querySelector("[data-flow-toggle-state='off']"), !isActive);
    setNodeForceVisible(rootNode.querySelector("[data-flow-toggle-icon='pause']"), isActive);
    setNodeForceVisible(rootNode.querySelector("[data-flow-toggle-icon='play']"), !isActive);

    const button = rootNode.querySelector("[data-flow-toggle-button='1']");
    if (!button) return;
    const titleOn = String(button.getAttribute("data-toggle-title-on") || "").trim();
    const titleOff = String(button.getAttribute("data-toggle-title-off") || "").trim();
    const label = isActive ? titleOn : titleOff;
    if (!label) return;
    button.setAttribute("title", label);
    button.setAttribute("aria-label", label);
  }

  function stopFlowToggleReadyPolling() {
    if (!flowToggleReadyPollTimer) return;
    window.clearInterval(flowToggleReadyPollTimer);
    flowToggleReadyPollTimer = 0;
  }

  function refreshFlowToggleReadyState() {
    const rootNode = flowToggleReadyRoot();
    if (!rootNode || flowToggleReadyPollInFlight) return;
    const url = String(rootNode.getAttribute("data-ready-url") || "").trim();
    if (!url) return;

    flowToggleReadyPollInFlight = true;
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
        return;
      }
      applyFlowToggleState(payload.ready === true, payload.user_active === true);
    }).catch(function () {
    }).finally(function () {
      flowToggleReadyPollInFlight = false;
    });
  }

  function startFlowToggleReadyPolling() {
    stopFlowToggleReadyPolling();
    const rootNode = flowToggleReadyRoot();
    if (!rootNode) return;
    const url = String(rootNode.getAttribute("data-ready-url") || "").trim();
    if (!url) return;
    refreshFlowToggleReadyState();
    flowToggleReadyPollTimer = window.setInterval(refreshFlowToggleReadyState, 3000);
  }

  function syncContactsTopState(isActive, isExhausted) {
    const active = !!isActive;
    const exhausted = !!isExhausted;
    const activeNode = contactsTopStateNode("active");
    const inactiveNode = contactsTopStateNode("inactive");
    setNodeForceVisible(activeNode, active);
    setNodeForceVisible(inactiveNode, !active);
    syncContactsExhaustedUi(exhausted);

    const panel = contactsTotalPanel();
    if (panel) {
      panel.setAttribute("data-contacts-exhausted", exhausted ? "1" : "0");
    }

    const rootNode = contactsSectionsRoot();
    if (rootNode) {
      rootNode.setAttribute("data-contacts-flow-active", active ? "1" : "0");
    }

    const collectWrapper = contactsSectionWrapper("collect");
    if (!collectWrapper) return;
    if (active) {
      if (!exhausted && String(collectWrapper.getAttribute("data-contacts-section-url") || "").trim()) {
        collectWrapper.setAttribute("data-contacts-section-running", "1");
      } else {
        collectWrapper.setAttribute("data-contacts-section-running", "0");
      }
      return;
    }
    collectWrapper.setAttribute("data-contacts-section-running", "0");
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

  function queueContactsSectionRefresh(urlOverride) {
    const url = String(urlOverride || "").trim();
    if (url) {
      contactsQueuedSectionUrl = url;
    }
    contactsSectionRefreshQueued = true;
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

  function stopContactsPolling() {
    if (!contactsPollTimer) return;
    window.clearInterval(contactsPollTimer);
    contactsPollTimer = 0;
  }

  function startCityRatingPolling() {
    stopCityRatingPolling();
    const panel = cityRatingPanel();
    if (!panel) return;
    if (panel.getAttribute("data-city-rating-running") !== "1") return;
    cityRatingPollTimer = window.setInterval(refreshCityRatingPanel, 2000);
  }

  function contactsShouldPoll() {
    const panel = contactsTotalPanel();
    if (!panel) return false;
    if (!String(panel.getAttribute("data-contacts-total-url") || "").trim()) return false;
    return true;
  }

  function refreshContactsTick() {
    refreshContactsTotalPanel();

    if (contactsSectionRefreshQueued) {
      if (contactsSectionPollInFlight) return;
      const queuedUrl = contactsQueuedSectionUrl;
      contactsQueuedSectionUrl = "";
      contactsSectionRefreshQueued = false;
      refreshContactsSectionPanel(queuedUrl);
      return;
    }

    if (!contactsFlowActive()) return;
    const activeSection = contactsActiveSectionKey();
    if (activeSection === "all") {
      return;
    }
    if (activeSection && activeSection !== "collect") {
      refreshContactsSectionPanel();
      return;
    }
    const collectWrapper = contactsSectionWrapper("collect");
    if (!collectWrapper) return;
    if (collectWrapper.getAttribute("data-contacts-section-running") !== "1") return;
    if (activeSection !== "collect") return;
    refreshContactsSectionPanel();
  }

  function startContactsPolling() {
    stopContactsPolling();
    if (!contactsShouldPoll()) return;
    contactsPollTimer = window.setInterval(refreshContactsTick, CONTACTS_POLL_INTERVAL_MS);
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
      scheduleCitiesWorkHeightSync();
      window.setTimeout(scheduleCitiesWorkHeightSync, 0);
      startCityRatingPolling();
    }).catch(function () {
    }).finally(function () {
      cityRatingPollInFlight = false;
    });
  }

  function refreshContactsTotalPanel() {
    const panel = contactsTotalPanel();
    const valueNodes = panel ? Array.from(panel.querySelectorAll("[data-contacts-total-value='1']")) : [];
    if (!panel || !valueNodes.length || contactsTotalPollInFlight) return;

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
        return;
      }
      const totalText = formatContactsTotal(payload.contacts_total || 0);
      valueNodes.forEach(function (node) {
        node.textContent = totalText;
      });
      const isExhausted = payload.is_exhausted === true;
      if (payload.is_active === true) {
        syncContactsTopState(true, isExhausted);
      } else if (payload.is_active === false) {
        syncContactsTopState(false, isExhausted);
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
      const previousPairsLayout = sectionKey === "pairs" ? snapshotContactsPairsLayout(inner) : null;
      const useFadeSwap = sectionKey !== "pairs";
      if (useFadeSwap) {
        inner.style.transition = "opacity 140ms ease";
        inner.style.opacity = "0.35";
      } else {
        inner.style.removeProperty("transition");
        inner.style.removeProperty("opacity");
      }
      window.setTimeout(function () {
        inner.innerHTML = html;
        if (useFadeSwap) {
          inner.style.opacity = "1";
        }
        if (sectionKey === "pairs") {
          animatePairsRefresh(inner, previousPairsLayout);
        }
        scheduleContactsBranchCityHeightSync();
        scheduleContactsPairsHeightSync();
        formatPairProcessedTimes();
        window.setTimeout(scheduleContactsBranchCityHeightSync, 0);
        window.setTimeout(scheduleContactsPairsHeightSync, 0);
      }, useFadeSwap ? 140 : 0);
    }).catch(function () {
    }).finally(function () {
      contactsSectionPollInFlight = false;
    });
  }

  function snapshotContactsPairsLayout(container) {
    if (!container) return "";
    const rows = Array.from(container.querySelectorAll("[data-contacts-pair-row-key]"));
    if (!rows.length) return null;
    const positions = {};
    rows.forEach(function (row) {
      const key = String(row.getAttribute("data-contacts-pair-row-key") || "").trim();
      if (!key) return;
      positions[key] = { top: row.getBoundingClientRect().top };
    });
    const topKey = String(rows[0].getAttribute("data-contacts-pair-row-key") || "").trim();
    return { topKey: topKey, positions: positions };
  }

  function animatePairsRefresh(container, previousLayout) {
    if (!container || !previousLayout || !previousLayout.positions) return;

    try {
      if (window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
        return;
      }
    } catch (e) {
    }

    const rows = Array.from(container.querySelectorAll("[data-contacts-pair-row-key]"));
    if (!rows.length) return;
    const currentTopKey = String(rows[0].getAttribute("data-contacts-pair-row-key") || "").trim();
    if (!previousLayout.topKey || !currentTopKey || previousLayout.topKey === currentTopKey) return;

    const animatedRows = [];
    const durationMs = 620;
    const easing = "cubic-bezier(0.22, 0.61, 0.36, 1)";

    rows.forEach(function (row) {
      const key = String(row.getAttribute("data-contacts-pair-row-key") || "").trim();
      if (!key) return;
      const nowTop = row.getBoundingClientRect().top;
      const prev = previousLayout.positions[key];
      if (prev && Number.isFinite(prev.top)) {
        const dy = prev.top - nowTop;
        if (Math.abs(dy) >= 1) {
          row.style.transition = "none";
          row.style.transform = "translateY(" + String(dy) + "px)";
          row.style.willChange = "transform";
          animatedRows.push(row);
        }
        return;
      }
      if (key === currentTopKey) {
        row.style.transition = "none";
        row.style.transform = "translateY(-14px)";
        row.style.opacity = "0";
        row.style.willChange = "transform, opacity";
        animatedRows.push(row);
      }
    });

    if (!animatedRows.length) return;

    void container.offsetHeight;

    animatedRows.forEach(function (row) {
      row.style.transition = "transform " + String(durationMs) + "ms " + easing + ", opacity " + String(durationMs) + "ms ease";
      row.style.transform = "translateY(0)";
      row.style.opacity = "1";
    });

    window.setTimeout(function () {
      animatedRows.forEach(function (row) {
        row.style.removeProperty("transition");
        row.style.removeProperty("transform");
        row.style.removeProperty("opacity");
        row.style.removeProperty("will-change");
      });
    }, durationMs + 40);
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
    queueContactsSectionRefresh(baseUrl + separator + args.join("&"));
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

    const boxRect = box.getBoundingClientRect();
    const yellowRect = firstYellow.getBoundingClientRect();
    const greenRect = lastGreen.getBoundingClientRect();
    const halfLastGreen = Math.max(0, Math.floor(greenRect.height / 2));
    const gapBetweenBlocks = Math.max(0, yellowRect.top - greenRect.bottom);
    const targetTop = box.scrollTop + (yellowRect.top - boxRect.top) - halfLastGreen - gapBetweenBlocks;
    box.scrollTop = Math.max(0, targetTop);
  }

  let branchesWorkHeightRaf = 0;
  let citiesWorkHeightRaf = 0;
  let contactsBranchCityHeightRaf = 0;
  let contactsPairsHeightRaf = 0;

  function fitBranchesWorkHeight() {
    const rootNode = document.querySelector("[data-branches-work-root='1']");
    if (!rootNode) return;

    const leftCard = rootNode.querySelector("[data-branches-work-left-card='1']");
    const scrollBox = rootNode.querySelector("[data-branches-scroll-box='1']");
    if (!leftCard || !scrollBox) return;

    leftCard.style.height = "";

    const top = leftCard.getBoundingClientRect().top;
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
    if (!viewportHeight) return;

    const topGap = Math.max(0, Math.floor(top));
    const mirroredGap = Math.min(topGap, 120);
    const bottomGap = Math.floor(mirroredGap * 0.3);
    const minHeight = 420;
    const height = Math.max(minHeight, Math.floor(viewportHeight - top - bottomGap));
    const px = String(height) + "px";
    leftCard.style.height = px;
  }

  function scheduleBranchesWorkHeightSync() {
    if (branchesWorkHeightRaf) return;
    branchesWorkHeightRaf = window.requestAnimationFrame(function () {
      branchesWorkHeightRaf = 0;
      fitBranchesWorkHeight();
      scrollBranchesToLastGreen();
    });
  }

  function fitCitiesWorkHeight() {
    const rootNode = document.querySelector("[data-cities-work-root='1']");
    if (!rootNode) return;

    const leftCard = rootNode.querySelector("[data-cities-work-left-card='1']");
    if (!leftCard) return;

    leftCard.style.removeProperty("height");

    const top = leftCard.getBoundingClientRect().top;
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
    if (!viewportHeight) return;

    const topGap = Math.max(0, Math.floor(top));
    const mirroredGap = Math.min(topGap, 120);
    const bottomGap = Math.floor(mirroredGap * 0.3);
    const minHeight = 420;
    const height = Math.max(minHeight, Math.floor(viewportHeight - top - bottomGap));
    const px = String(height) + "px";
    leftCard.style.setProperty("height", px, "important");
  }

  function scheduleCitiesWorkHeightSync() {
    if (citiesWorkHeightRaf) return;
    citiesWorkHeightRaf = window.requestAnimationFrame(function () {
      citiesWorkHeightRaf = 0;
      fitCitiesWorkHeight();
    });
  }

  function fitContactsBranchCityHeight() {
    const rootNode = document.querySelector("[data-contacts-branch-city-root='1']");
    if (!rootNode) return;

    const leftCard = rootNode.querySelector("[data-contacts-branch-city-left-card='1']");
    const rightCard = rootNode.querySelector("[data-contacts-branch-city-right-card='1']");
    if (!leftCard || !rightCard) return;

    leftCard.style.removeProperty("height");
    rightCard.style.removeProperty("height");

    const leftTop = leftCard.getBoundingClientRect().top;
    const rightTop = rightCard.getBoundingClientRect().top;
    const top = Math.min(leftTop, rightTop);
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
  }

  function scheduleContactsBranchCityHeightSync() {
    if (contactsBranchCityHeightRaf) return;
    contactsBranchCityHeightRaf = window.requestAnimationFrame(function () {
      contactsBranchCityHeightRaf = 0;
      fitContactsBranchCityHeight();
    });
  }

  function fitContactsPairsHeight() {
    const rootNode = document.querySelector("[data-contacts-pairs-root='1']");
    if (!rootNode) return;

    const card = rootNode.querySelector("[data-contacts-pairs-card='1']");
    if (!card) return;

    card.style.removeProperty("height");

    const top = card.getBoundingClientRect().top;
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
    if (!viewportHeight) return;

    const topGap = Math.max(0, Math.floor(top));
    const mirroredGap = Math.min(topGap, 120);
    const bottomGap = Math.floor(mirroredGap * 0.3);
    const minHeight = 420;
    const height = Math.max(minHeight, Math.floor(viewportHeight - top - bottomGap));
    const px = String(height) + "px";
    card.style.setProperty("height", px, "important");
  }

  function scheduleContactsPairsHeightSync() {
    if (contactsPairsHeightRaf) return;
    contactsPairsHeightRaf = window.requestAnimationFrame(function () {
      contactsPairsHeightRaf = 0;
      fitContactsPairsHeight();
    });
  }

  function twoDigits(value) {
    const number = Number(value || 0);
    if (!Number.isFinite(number)) return "00";
    return String(number).padStart(2, "0");
  }

  function formatPairProcessedTimes() {
    const nodes = Array.from(document.querySelectorAll("[data-contacts-pair-time='1']"));
    if (!nodes.length) return;

    let preferredTz = "UTC";
    try {
      const browserTz = String(Intl.DateTimeFormat().resolvedOptions().timeZone || "").trim();
      if (browserTz) preferredTz = browserTz;
    } catch (e) {
      preferredTz = "UTC";
    }

    nodes.forEach(function (node) {
      const isoRaw = String(node.getAttribute("data-utc-iso") || "").trim();
      if (!isoRaw) return;

      const sourceDate = new Date(isoRaw);
      if (!Number.isFinite(sourceDate.getTime())) return;

      let tz = preferredTz;
      let localDate;
      try {
        localDate = new Date(sourceDate.toLocaleString("en-US", { timeZone: tz }));
      } catch (e) {
        tz = "UTC";
        localDate = new Date(sourceDate.toLocaleString("en-US", { timeZone: "UTC" }));
      }
      if (!Number.isFinite(localDate.getTime())) {
        tz = "UTC";
        localDate = sourceDate;
      }

      const display =
        String(localDate.getFullYear()) +
        "-" +
        twoDigits(localDate.getMonth() + 1) +
        "-" +
        twoDigits(localDate.getDate()) +
        " " +
        twoDigits(localDate.getHours()) +
        ":" +
        twoDigits(localDate.getMinutes()) +
        ":" +
        twoDigits(localDate.getSeconds());

      let offsetLabel = "UTC";
      try {
        const offsetFmt = new Intl.DateTimeFormat("en-US", {
          timeZone: tz,
          timeZoneName: "shortOffset",
          hour: "2-digit",
          minute: "2-digit",
        });
        const tzPart = offsetFmt.formatToParts(sourceDate).find(function (part) {
          return part.type === "timeZoneName";
        });
        const rawOffset = String((tzPart && tzPart.value) || "").trim();
        if (rawOffset) {
          offsetLabel = rawOffset.replace("GMT", "UTC");
        } else if (tz !== "UTC") {
          offsetLabel = tz;
        }
      } catch (e) {
        if (tz !== "UTC") {
          offsetLabel = tz;
        }
      }

      const decoded = offsetLabel || "UTC";
      node.textContent = display + " (" + decoded + ")";
    });
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
        queueContactsSectionRefresh();
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
      row.classList.remove("bg-[#ffffd6]");
      row.classList.remove("bg-[#ffffdc]");
      row.classList.remove("bg-[#ffffe3]");
      row.classList.add("bg-[#fff3f3]");
    } else {
      row.classList.remove("bg-[#f0fff0]");
      row.classList.remove("bg-[#FFF7E0]");
      row.classList.remove("bg-[#ffffd6]");
      row.classList.remove("bg-[#ffffdc]");
      row.classList.remove("bg-[#ffffe3]");
      row.classList.remove("bg-[#fff3f3]");
      if (row.getAttribute("data-branch-yellow") === "1") {
        row.classList.add("bg-[#ffffe3]");
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
        row.classList.remove("bg-[#ffffd6]");
        row.classList.remove("bg-[#ffffdc]");
        row.classList.remove("bg-[#ffffe3]");
        if (row.getAttribute("data-branch-yellow") === "1") {
          row.classList.add("bg-[#ffffe3]");
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

  syncBranchDeleteState();
  scheduleBranchesWorkHeightSync();
  window.setTimeout(scheduleBranchesWorkHeightSync, 0);
  window.setTimeout(scheduleBranchesWorkHeightSync, 140);
  window.addEventListener("resize", scheduleBranchesWorkHeightSync);
  window.addEventListener("orientationchange", scheduleBranchesWorkHeightSync);

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
      row.classList.remove("bg-[#ffffe3]");
      row.classList.add("bg-[#fff3f3]");
    } else {
      row.classList.remove("bg-[#f0fff0]");
      row.classList.remove("bg-[#FFF7E0]");
      row.classList.remove("bg-[#ffffe3]");
      row.classList.remove("bg-[#fff3f3]");
      if (row.getAttribute("data-city-yellow") === "1") {
        row.classList.add("bg-[#ffffe3]");
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
        row.classList.remove("bg-[#ffffe3]");
        if (row.getAttribute("data-city-yellow") === "1") {
          row.classList.add("bg-[#ffffe3]");
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
  scheduleCitiesWorkHeightSync();
  window.setTimeout(scheduleCitiesWorkHeightSync, 0);
  window.setTimeout(scheduleCitiesWorkHeightSync, 140);
  window.addEventListener("resize", scheduleCitiesWorkHeightSync);
  window.addEventListener("orientationchange", scheduleCitiesWorkHeightSync);
  scheduleContactsBranchCityHeightSync();
  window.setTimeout(scheduleContactsBranchCityHeightSync, 0);
  window.setTimeout(scheduleContactsBranchCityHeightSync, 140);
  window.addEventListener("resize", scheduleContactsBranchCityHeightSync);
  window.addEventListener("orientationchange", scheduleContactsBranchCityHeightSync);
  scheduleContactsPairsHeightSync();
  window.setTimeout(scheduleContactsPairsHeightSync, 0);
  window.setTimeout(scheduleContactsPairsHeightSync, 140);
  window.addEventListener("resize", scheduleContactsPairsHeightSync);
  window.addEventListener("orientationchange", scheduleContactsPairsHeightSync);
  formatPairProcessedTimes();
  syncContactsSectionButtons();
  showContactsSection(contactsActiveSectionKey() || "collect");
  syncContactsTopState(contactsFlowActive(), contactsExhausted());
  startCityRatingPolling();
  startContactsPolling();
  startFlowToggleReadyPolling();
})();
