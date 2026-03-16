// FILE: web/static/js/aap_audience/create_edit_buy.js
// DATE: 2026-03-15
// PURPOSE: Unsaved changes warnings for create/edit buy page using YYModal.

(function () {
  const root = document.getElementById("yy-create-edit-buy-config");
  if (!root) return;

  const labels = {
    title: root.dataset.labelTitle || "Title",
    product: root.dataset.labelProduct || "Product",
    company: root.dataset.labelCompany || "Company",
    geo: root.dataset.labelGeo || "Geo",
  };
  const missingTitle = root.dataset.missingTitle || "Required sections are missing";
  const missingText = root.dataset.missingText || "Please fill and save:";
  const closeLabel = root.dataset.closeLabel || "Close";
  const forms = Array.from(document.querySelectorAll("[data-dirty-form]"));
  const bypassSubmit = new WeakMap();

  function isWaitAction(submitter) {
    const action = submitter && submitter.name === "action" ? String(submitter.value || "") : "";
    return action === "suggest_title" || action === "process_product" || action === "process_company" || action === "process_geo";
  }

  function norm(value) {
    return String(value || "").replace(/\r\n/g, "\n").trim();
  }

  function comparableField(form) {
    return form.querySelector(
      "input[name='audience_title'], textarea[name='source_product'], textarea[name='source_company'], textarea[name='source_geo']"
    );
  }

  function fieldDirty(field) {
    return !!field && norm(field.value) !== norm(field.dataset.savedValue || "");
  }

  function setButtonText(button, text, withArrow) {
    if (!button) return;
    button.textContent = text || "";
    if (!withArrow) return;
    button.appendChild(document.createTextNode(" "));
    const arrow = document.createElement("span");
    arrow.innerHTML = "&rarr;";
    button.appendChild(arrow);
  }

  function formDirty(form) {
    return fieldDirty(comparableField(form));
  }

  function syncActionButtons(form) {
    const titleField = form.querySelector("input[name='audience_title']");
    const mainField = form.querySelector("textarea[name='source_product'], textarea[name='source_company'], textarea[name='source_geo']");
    const instructionField = form.querySelector("textarea[name='product_ai_command'], textarea[name='company_ai_command'], textarea[name='geo_ai_command']");
    const processMainBtn = form.querySelector("[data-process-main='1']");
    const processInstructionBtn = form.querySelector("[data-process-instruction='1']");
    const saveMainBtn = form.querySelector("[data-save-main='1']");
    const saveTitleBtn = form.querySelector("[data-save-title='1']");
    const hasMainValue = !!(mainField && norm(mainField.value) !== "");
    const mainChanged = !!(mainField && fieldDirty(mainField));
    const hasSavedValue = !!(mainField && norm(mainField.dataset.savedValue || "") !== "");
    const isSavedMainValue = hasMainValue && hasSavedValue && !mainChanged;
    const hasTitleValue = !!(titleField && norm(titleField.value) !== "");
    const titleChanged = !!(titleField && fieldDirty(titleField));

    if (saveTitleBtn) {
      saveTitleBtn.disabled = !(hasTitleValue && titleChanged);
    }

    if (processMainBtn) {
      processMainBtn.disabled = !(hasMainValue && mainChanged);
    }

    if (processInstructionBtn && instructionField) {
      processInstructionBtn.disabled = norm(instructionField.value) === "";
    }

    if (saveMainBtn) {
      saveMainBtn.disabled = !hasMainValue;
      saveMainBtn.dataset.mode = isSavedMainValue ? "next" : "save";
      saveMainBtn.type = isSavedMainValue ? "button" : "submit";
      setButtonText(
        saveMainBtn,
        isSavedMainValue ? (saveMainBtn.dataset.nextLabel || "") : (saveMainBtn.dataset.saveLabel || ""),
        isSavedMainValue
      );
    }
  }

  function dirtyFormNames(exceptForm) {
    return forms
      .filter(function (form) {
        return form !== exceptForm && formDirty(form);
      })
      .map(function (form) {
        return labels[form.getAttribute("data-dirty-form")] || form.getAttribute("data-dirty-form");
      });
  }

  function dirtyFormNamesForSubmit(form, submitter) {
    const action = submitter && submitter.name === "action" ? String(submitter.value || "") : "";

    if (action === "suggest_title") {
      return forms
        .filter(function (candidate) {
          const key = candidate.getAttribute("data-dirty-form");
          return key !== "title" && formDirty(candidate);
        })
        .map(function (candidate) {
          return labels[candidate.getAttribute("data-dirty-form")] || candidate.getAttribute("data-dirty-form");
        });
    }

    if (
      action === "process_product" ||
      action === "process_company" ||
      action === "process_geo" ||
      action === "save_product" ||
      action === "save_company" ||
      action === "save_geo" ||
      action === "save_title" ||
      action === "reset_product_context" ||
      action === "reset_company_context" ||
      action === "reset_geo_context"
    ) {
      return dirtyFormNames(form);
    }

    return dirtyFormNames(null);
  }

  function requiredMissingNames() {
    return ["product", "company", "geo"]
      .filter(function (key) {
        const field = document.querySelector("[data-summary-section='" + key + "']");
        return !field || norm(field.dataset.summaryValue || "") === "";
      })
      .map(function (key) {
        return labels[key] || key;
      });
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

    const saveMainBtn = form.querySelector("[data-save-main='1']");
    if (saveMainBtn) {
      saveMainBtn.addEventListener("click", function (event) {
        if (saveMainBtn.dataset.mode !== "next") return;
        event.preventDefault();
        const nextUrl = saveMainBtn.dataset.nextUrl || "";
        if (!nextUrl) return;
        const names = dirtyFormNames(null);
        if (!names.length) {
          window.location.href = nextUrl;
          return;
        }
        openDirtyModal(names, function () {
          window.location.href = nextUrl;
        });
      });
    }

    form.querySelectorAll("textarea, input").forEach(function (field) {
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
      const missing = requiredMissingNames();
      if (missing.length) {
        openDirtyModal(missing, function () {
          window.location.href = button.getAttribute("data-dirty-nav");
        }, {
          title: missingTitle,
          text: missingText,
          closeLabel: closeLabel,
        });
        return;
      }
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
})();
