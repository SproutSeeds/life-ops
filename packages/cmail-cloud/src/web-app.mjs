function application() {
  // Recovered C-mail UI with indexed, lazy conversation loading.
  function address(value) {
    if (value?.group) return value.group.map(address).filter(Boolean).join(", ");
    return value?.name ? `${value.name} <${value.address || ""}>` : value?.address || "";
  }
  function formatMessage(message) {
    return [
      `Subject: ${message.subject || "(No subject)"}`,
      `From: ${address(message.from)}`,
      `To: ${(message.to || []).map(address).join(", ")}`,
      ...message.cc?.length ? [`Cc: ${message.cc.map(address).join(", ")}`] : [],
      `Date: ${message.date || message.received_at || ""}`,
      "",
      message.text || ""
    ].join("\n");
  }
  function formatThread(messages) {
    return messages.map((message, index) => `----- Message ${index + 1} of ${messages.length} -----
${formatMessage(message)}`).join("\n\n");
  }

  // web/app.js
  var $ = (id) => document.getElementById(id);
  var cursor = null;
  var selectedId = null;
  var requestGeneration = 0;
  var readingGeneration = 0;
  var loading = false;
  var items = [];
  var pageIndex = 0;
  var pageSize = 50;
  try {
    const saved = Number(localStorage.getItem("cmail.page-size"));
    if ([20, 50, 100].includes(saved)) pageSize = saved;
  } catch {
  }
  var exhausted = false;
  var interrupted = false;
  var activeFilters = null;
  var totalStored = 0;
  var totalView = null;
  var bulkBusy = false;
  var actionStatusTimer;
  var checkedIds = /* @__PURE__ */ new Set();
  var selectedMessage = null;
  var renderedIds = "";
  var pendingReadIds = /* @__PURE__ */ new Set();
  var readStateOverrides = /* @__PURE__ */ new Map();
  var scanned = 0;
  var failed = 0;
  var loadController = null;
  var signingOut = false;
  var knownMailboxes = /* @__PURE__ */ new Set();
  var conversationGeneration = 0;
  var conversationController = null;
  var conversationMessages = [];
  var conversationBodies = new Map();
  var conversationVisible = 20;
  var exactViewTotal = false;
  function resetConversation() {
    conversationGeneration += 1;
    conversationController?.abort();
    conversationController = null;
    conversationMessages = [];
    conversationBodies.clear();
    conversationVisible = 20;
    $("threadMessages").replaceChildren();
    $("copyThread").disabled = true;
    $("threadStatus").textContent = "";
    $("retryThread").hidden = true;
    $("copyFallback").close();
    $("copyText").value = "";
  }
  function addMailboxes(addresses) {
    for (const value of addresses) {
      const address2 = String(value || "").trim().toLowerCase();
      if (!address2 || knownMailboxes.has(address2)) continue;
      knownMailboxes.add(address2);
      const option = document.createElement("option");
      option.value = address2;
      option.textContent = address2;
      $("mailbox").append(option);
    }
    updateInboxLabel();
  }
  function updateInboxLabel() {
    const mailbox = $("mailbox").value;
    const view = $("mailView").value;
    $("inboxTitle").textContent = view === "archive" ? "Archive" : view === "all" ? "All mail" : mailbox ? "Inbox" : "All inboxes";
    $("search").placeholder = view === "archive" ? "Search archive" : view === "all" ? "Search all mail" : mailbox ? "Search this inbox" : "Search all inboxes";
    $("inboxTitle").title = mailbox || "All connected addresses";
  }
  function closeMessage() {
    resetConversation();
    readingGeneration += 1;
    selectedId = null;
    selectedMessage = null;
    document.body.classList.remove("reading");
    window.history.replaceState(null, "", window.location.pathname + window.location.search);
    $("message").hidden = true;
    $("readerPlaceholder").hidden = false;
    document.querySelectorAll(".message-row").forEach((row) => row.classList.remove("selected"));
  }
  function switchInbox() {
    if (bulkBusy) return;
    const url = new URL(window.location.href);
    if ($("mailbox").value) url.searchParams.set("mailbox", $("mailbox").value);
    else url.searchParams.delete("mailbox");
    if ($("mailView").value !== "inbox") url.searchParams.set("view", $("mailView").value);
    else url.searchParams.delete("view");
    url.hash = "";
    window.history.replaceState(null, "", url);
    closeMessage();
    updateInboxLabel();
    load(true);
  }
  function notice(message) {
    $("notice").textContent = message;
    $("notice").hidden = !message;
  }
  function actionStatus(message) {
    clearTimeout(actionStatusTimer);
    $("actionStatus").textContent = message;
    $("actionStatus").hidden = !message;
    if (message) actionStatusTimer = setTimeout(() => {
      $("actionStatus").hidden = true;
    }, 5e3);
  }
  function connected(value) {
    $("connectionText").textContent = value;
    $("connection").title = value;
  }
  async function api(path, signal, options = {}) {
    const result = await fetch(`/cmail/api/${path}`, { ...options, cache: "no-store", redirect: "manual", signal });
    if (result.type === "opaqueredirect" || [401, 403].includes(result.status)) {
      throw new Error("Your session has expired. Reload this page to sign in again.");
    }
    if (!result.ok || !result.headers.get("content-type")?.includes("application/json")) {
      throw new Error("Your mail is temporarily unavailable. Please try Refresh.");
    }
    return result.json();
  }
  function dateLabel(value, full = false) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value || "";
    return new Intl.DateTimeFormat(void 0, full ? { dateStyle: "medium", timeStyle: "short" } : { month: "short", day: "numeric" }).format(date);
  }
  function senderLabel(value) {
    return String(value).replace(/<[^>]*>/g, "").trim().replace(/^"|"$/g, "").trim() || value;
  }
  function mailboxAddress(value) {
    if (!value) return "";
    if (value.group) return value.group.map(mailboxAddress).join(", ");
    return value.name ? `${value.name} <${value.address}>` : value.address || "";
  }
  function addText(parent, tag, className, value) {
    const element = document.createElement(tag);
    element.className = className;
    element.textContent = value;
    parent.append(element);
    return element;
  }
  function applyRowState(row, item) {
    row.classList.toggle("unread", !item.read);
    row.classList.toggle("selected", item.id === selectedId);
    row.dataset.read = String(Boolean(item.read));
    row.classList.toggle("checked", checkedIds.has(item.id));
    const checkbox = row.querySelector("input");
    checkbox.checked = checkedIds.has(item.id);
    checkbox.disabled = bulkBusy;
    row.querySelector(".message-open").setAttribute("aria-label", `${item.read ? "Read" : "Unread"}: ${senderLabel(item.from)} \u2014 ${item.subject}`);
  }
  function appendRow(item) {
    const row = document.createElement("div");
    row.className = "message-row";
    row.dataset.id = item.id;
    const label = addText(row, "label", "row-check", "");
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.setAttribute("aria-label", `Select: ${senderLabel(item.from)} \u2014 ${item.subject}`);
    label.append(checkbox);
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) checkedIds.add(item.id);
      else checkedIds.delete(item.id);
      renderList();
    });
    const open = addText(row, "button", "message-open", "");
    open.type = "button";
    const indicator = addText(open, "span", "read-indicator", "");
    indicator.setAttribute("aria-hidden", "true");
    addText(open, "span", "row-sender", senderLabel(item.from));
    const subject = addText(open, "span", "row-subject", item.subject);
    subject.title = item.subject;
    addText(open, "span", "row-mailbox", item.mailbox);
    addText(open, "time", "row-date", dateLabel(item.received_at));
    applyRowState(row, item);
    open.addEventListener("click", () => openMessage(item.id));
    $("messages").append(row);
  }
  function closeActions() {
    $("actionsMenu").hidden = true;
    $("actionsButton").setAttribute("aria-expanded", "false");
  }
  function updateSelection() {
    const visible = items.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);
    const count = checkedIds.size;
    $("selectPage").checked = visible.length > 0 && visible.every((item) => checkedIds.has(item.id));
    $("selectPage").indeterminate = count > 0 && !$("selectPage").checked;
    $("selectPage").disabled = !visible.length || bulkBusy;
    $("selectionCount").textContent = `${count} selected`;
    $("selectionCount").hidden = !count;
    $("clearSelection").hidden = !count;
    $("clearSelection").disabled = bulkBusy;
    document.body.classList.toggle("has-selection", count > 0);
    $("actionsButton").disabled = !count || loading || bulkBusy || [...checkedIds].some((id) => pendingReadIds.has(id));
    $("actionsButton").textContent = bulkBusy ? "Saving\u2026" : "Actions \u2304";
    $("actionsButton").title = loading ? "Wait for loading to finish, or choose Stop." : "Actions for checked messages";
    document.querySelector('[data-action="archive"]').hidden = $("mailView").value === "archive";
    document.querySelector('[data-action="unarchive"]').hidden = $("mailView").value === "inbox";
    for (const id of ["mailbox", "mailView", "pageSize", "search", "after", "before", "applyDates", "clearDates"]) $(id).disabled = bulkBusy;
    $("refresh").disabled = bulkBusy || loading;
  }
  function clearSelection() {
    checkedIds.clear();
    closeActions();
    renderList();
  }
  function isSearching() {
    return Boolean(activeFilters?.query || activeFilters?.after || activeFilters?.before);
  }
  function renderList() {
    const start = pageIndex * pageSize;
    const visible = items.slice(start, start + pageSize);
    const ids = visible.map((item) => item.id).join(",");
    if (ids !== renderedIds || !visible.length) {
      $("messages").replaceChildren();
      renderedIds = ids;
      visible.forEach(appendRow);
      if (!visible.length) {
        const text = loading ? isSearching() ? "Searching your mail\u2026 Results will appear as they are found." : "Loading messages for this inbox\u2026" : interrupted ? "Loading has not finished. Choose Continue to keep looking." : failed ? "Some messages could not be loaded. Results may be incomplete." : isSearching() ? "No messages match these filters." : $("mailView").value === "archive" ? "No archived messages in this view." : "No messages in this inbox.";
        addText($("messages"), "div", "empty-list", text);
      }
    } else {
      document.querySelectorAll(".message-row").forEach((row, index) => applyRowState(row, visible[index]));
    }
    const knownTotal = exhausted ? items.length : !isSearching() && exactViewTotal && !failed ? totalView : null;
    $("resultCount").textContent = visible.length ? `${(start + 1).toLocaleString()}\u2013${(start + visible.length).toLocaleString()}${knownTotal === null ? " shown" : ` of ${knownTotal.toLocaleString()}`}` : "0 shown";
    const pageCount = knownTotal === null ? null : Math.max(1, Math.ceil(knownTotal / pageSize));
    $("pageLabel").textContent = `Page ${pageIndex + 1}${pageCount === null ? "" : ` of ${pageCount}`}`;
    $("previousPage").disabled = pageIndex === 0 || bulkBusy;
    const cachedNext = items.length > start + pageSize;
    $("nextPage").disabled = bulkBusy || !cachedNext && (exhausted || loading || interrupted);
    $("continueLoad").hidden = !interrupted || loading;
    $("stopSearch").hidden = !loading;
    updateSelection();
  }
  async function load(reset = false) {
    if (signingOut || bulkBusy || loading && !reset) return;
    if (reset) {
      requestGeneration += 1;
      loadController?.abort();
      cursor = null;
      items = [];
      pageIndex = 0;
      scanned = 0;
      failed = 0;
      exhausted = false;
      interrupted = false;
      totalView = null;
      exactViewTotal = false;
      checkedIds.clear();
      closeActions();
      actionStatus("");
      readStateOverrides.clear();
      activeFilters = { query: $("search").value.trim(), mailbox: $("mailbox").value, view: $("mailView").value, after: $("after").value, before: $("before").value };
      renderedIds = "";
      $("messages").replaceChildren();
      $("messages").scrollTop = 0;
      notice("");
    }
    if (exhausted) {
      renderList();
      return;
    }
    const generation = requestGeneration;
    loadController = new AbortController();
    const { signal } = loadController;
    const searching = isSearching();
    const parameters = new URLSearchParams(activeFilters);
    loading = true;
    interrupted = false;
    $("refresh").disabled = true;
    connected(searching ? "Searching mail" : "Loading mail");
    $("listLabel").textContent = searching ? "Searching your mail\u2026" : "Loading messages\u2026";
    renderList();
    try {
      do {
        if (cursor) parameters.set("cursor", cursor);
        else parameters.delete("cursor");
        const target = (pageIndex + 1) * pageSize + 1;
        parameters.set("limit", String(searching ? pageSize : Math.min(100, Math.max(1, target - items.length))));
        const result = await api(`messages?${parameters}`, signal);
        if (generation !== requestGeneration || signingOut) return;
        addMailboxes(result.items.map((item) => item.mailbox));
        for (const item of result.items) if (readStateOverrides.has(item.id)) item.read = readStateOverrides.get(item.id);
        items.push(...result.items);
        cursor = result.next_cursor;
        exhausted = !cursor;
        scanned += result.scanned;
        failed += result.failed;
        totalStored = Number(result.total_stored);
        totalView = Number(result.total_view);
        exactViewTotal = result.exact_view_total === true;
        $("listLabel").textContent = searching ? `Searching ${scanned.toLocaleString()} / ${totalStored.toLocaleString()}` : "Latest messages";
        if (failed) notice("Some messages could not be loaded. Results may be incomplete.");
        renderList();
      } while (cursor && items.length < (pageIndex + 1) * pageSize + 1);
      if (exhausted) pageIndex = Math.min(pageIndex, Math.max(0, Math.ceil(items.length / pageSize) - 1));
      $("listLabel").textContent = searching ? failed ? "Search finished with errors" : exhausted ? "Search complete" : "Search results" : "Latest messages";
      connected("Cloud inbox");
    } catch (error) {
      if (generation !== requestGeneration || signingOut) return;
      interrupted = true;
      if (error.name === "AbortError") {
        $("listLabel").textContent = searching ? "Search paused" : "Loading paused";
        connected("Paused");
      } else {
        notice(error.message);
        connected("Connection interrupted");
        $("listLabel").textContent = "Loading interrupted";
      }
    } finally {
      if (generation === requestGeneration && !signingOut) {
        loading = false;
        $("refresh").disabled = false;
        renderList();
      }
    }
  }
  function changePage(delta) {
    if (bulkBusy) return;
    checkedIds.clear();
    closeActions();
    pageIndex = Math.max(0, pageIndex + delta);
    closeMessage();
    $("messages").scrollTop = 0;
    renderList();
    if (!exhausted && items.length < (pageIndex + 1) * pageSize + 1) load();
  }
  function updateReadControl() {
    if (!selectedMessage) return;
    $("toggleRead").disabled = bulkBusy || pendingReadIds.has(selectedMessage.id);
    $("toggleRead").textContent = pendingReadIds.has(selectedMessage.id) ? "Saving\u2026" : selectedMessage.read ? "Mark unread" : "Mark read";
  }
  async function setReadState(id, read) {
    if (pendingReadIds.has(id) || signingOut || bulkBusy) return;
    pendingReadIds.add(id);
    updateReadControl();
    updateSelection();
    if (selectedId === id) {
      $("readerNotice").hidden = true;
      $("readerNotice").textContent = "";
    }
    try {
      const result = await api(`messages/${id}/read-status`, void 0, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ read })
      });
      if (signingOut) return;
      readStateOverrides.set(id, result.read);
      const item = items.find((item2) => item2.id === id);
      if (item) item.read = result.read;
      if (selectedMessage?.id === id) selectedMessage.read = result.read;
      renderList();
    } catch {
      if (!signingOut && selectedId === id) {
        $("readerNotice").textContent = "Read status could not be saved. Use the read/unread button to try again.";
        $("readerNotice").hidden = false;
      }
    } finally {
      pendingReadIds.delete(id);
      if (!signingOut) {
        updateReadControl();
        updateSelection();
      }
    }
  }
  async function applyBulkAction(action) {
    if (!checkedIds.size || $("actionsButton").disabled || signingOut) return;
    const ids = [...checkedIds];
    bulkBusy = true;
    readingGeneration += 1;
    closeActions();
    renderList();
    updateReadControl();
    notice("");
    let succeeded = false;
    try {
      const result = await api("messages/actions", void 0, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ids, action })
      });
      if (signingOut) return;
      for (const state of result.items) {
        const item = items.find((item2) => item2.id === state.id);
        if (item) {
          const archiveDelta = Number(state.archived) - Number(Boolean(item.archived));
          if (totalView !== null && activeFilters.view !== "all") totalView += activeFilters.view === "archive" ? archiveDelta : -archiveDelta;
          Object.assign(item, state);
        }
        readStateOverrides.set(state.id, state.read);
        if (selectedMessage?.id === state.id) Object.assign(selectedMessage, state);
      }
      if (action === "archive" || action === "unarchive") {
        items = items.filter((item) => activeFilters.view === "all" || item.archived === (activeFilters.view === "archive"));
        if (ids.includes(selectedId)) closeMessage();
        pageIndex = Math.min(pageIndex, Math.max(0, Math.ceil(items.length / pageSize) - 1));
      }
      checkedIds.clear();
      succeeded = true;
    } catch (error) {
      const detail = error.message.startsWith("Your ") ? error.message : "Check your connection and try again.";
      if (!signingOut) notice(`Could not confirm the action. Your selection is kept so you can retry. ${detail}`);
    } finally {
      bulkBusy = false;
      if (!signingOut) {
        renderList();
        updateReadControl();
        if (succeeded) {
          const label = { read: "Marked read", unread: "Marked unread", archive: "Archived", unarchive: "Moved to inbox" }[action];
          actionStatus(`${label}: ${ids.length} ${ids.length === 1 ? "message" : "messages"}.`);
          if (!exhausted && !interrupted && items.length < (pageIndex + 1) * pageSize + 1) load();
        }
      }
    }
  }
  function renderBody(text, target = $("messageBody")) {
    target.replaceChildren();
    const matches = String(text).matchAll(/https?:\/\/[^\s<>"\u0000-\u001f]+/g);
    let offset = 0;
    for (const match of matches) {
      target.append(document.createTextNode(text.slice(offset, match.index)));
      const link = document.createElement("a");
      link.href = match[0];
      link.textContent = match[0];
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      target.append(link);
      offset = match.index + match[0].length;
    }
    target.append(document.createTextNode(text.slice(offset)));
  }
  async function copyText(text, button, label) {
    const id = selectedId;
    const original = button.textContent;
    button.disabled = true;
    button.textContent = "Copying\u2026";
    try {
      await navigator.clipboard.writeText(text);
      if (!signingOut && selectedId === id) actionStatus(`${label} copied.`);
    } catch {
      if (!signingOut && selectedId === id) {
        $("copyText").value = text;
        $("copyFallback").showModal();
        $("copyText").focus();
        $("copyText").select();
      }
    } finally {
      button.textContent = original;
      button.disabled = button.id === "copyThread" ? !conversationMessages.length : false;
    }
  }
  async function conversationBody(message, signal) {
    if (conversationBodies.has(message.id)) return conversationBodies.get(message.id);
    const loaded = await api(`messages/${message.id}`, signal);
    if (!signal?.aborted) conversationBodies.set(message.id, loaded);
    return loaded;
  }
  function renderConversation() {
    $("threadMessages").replaceChildren();
    const others = conversationMessages.filter((message) => message.id !== selectedId);
    if (!others.length) return;
    addText($("threadMessages"), "h3", "thread-heading", "Conversation · newest first");
    for (const message of others.slice(0, conversationVisible)) {
      const card = addText($("threadMessages"), "article", "thread-message", "");
      const heading = addText(card, "div", "thread-message-heading", "");
      const sender = addText(heading, "div", "", "");
      const from = typeof message.from === "string" ? message.from : mailboxAddress(message.from);
      addText(sender, "strong", "", from);
      addText(sender, "time", "", dateLabel(message.date, true));
      const copy = addText(heading, "button", "quiet copy-message", "Copy message");
      copy.type = "button";
      copy.setAttribute("aria-label", `Copy message from ${from}`);
      copy.addEventListener("click", async () => {
        copy.disabled = true;
        try { await copyText(formatMessage(await conversationBody(message, conversationController?.signal)), copy, "Message"); }
        catch { actionStatus("Could not load that message. Please try again."); }
        finally { copy.disabled = false; }
      });
      const detail = addText(card, "details", "", "");
      const summary = addText(detail, "summary", "", message.subject);
      summary.style.cursor = "pointer";
      const body = addText(detail, "div", "message-body", "");
      let loaded = false, pending = false;
      detail.addEventListener("toggle", async () => {
        if (!detail.open || loaded || pending) return;
        pending = true; body.textContent = "Loading message…";
        try {
          const result = await conversationBody(message, conversationController?.signal);
          if (detail.isConnected) { renderBody(result.text, body); loaded = true; }
        } catch { body.textContent = "Could not load this message. Close and reopen it to retry."; }
        finally { pending = false; }
      });
    }
    if (others.length > conversationVisible) {
      const more = addText($("threadMessages"), "button", "quiet", `Show older messages (${others.length - conversationVisible})`);
      more.type = "button"; more.style.cursor = "pointer";
      more.addEventListener("click", () => { conversationVisible += 20; renderConversation(); });
    }
  }
  async function loadConversation(message) {
    resetConversation();
    const generation = conversationGeneration;
    conversationController = new AbortController();
    const { signal } = conversationController;
    const current = () => !signingOut && selectedId === message.id && generation === conversationGeneration;
    $("threadStatus").textContent = "Loading conversation…";
    try {
      const result = await api(`messages/${message.id}/conversation`, signal);
      if (!current()) return;
      conversationMessages = result.items;
      conversationBodies.set(message.id, message);
      renderConversation();
      $("copyThread").disabled = false;
      $("threadStatus").textContent = `${result.total} ${result.total === 1 ? "message" : "messages"} in this conversation.${result.missing ? " Some referenced messages are not in C-mail; quoted history is kept." : ""}`;
    } catch (error) {
      if (!current() || error.name === "AbortError") return;
      $("threadStatus").textContent = "The conversation could not be loaded. You can still copy this message.";
      $("retryThread").hidden = false;
      $("copyThread").disabled = true;
    }
  }
  async function openMessage(id) {
    if (signingOut || bulkBusy) return;
    const generation = ++readingGeneration;
    notice("");
    connected("Opening message");
    try {
      const message = await api(`messages/${encodeURIComponent(id)}`);
      if (generation !== readingGeneration) return;
      selectedId = id;
      selectedMessage = message;
      $("readerNotice").hidden = true;
      $("readerNotice").textContent = "";
      window.history.replaceState(null, "", `#${id}`);
      document.querySelectorAll(".message-row").forEach((row) => row.classList.toggle("selected", row.dataset.id === id));
      $("messageSubject").textContent = message.subject;
      $("messageMailbox").textContent = message.mailbox;
      $("messageFrom").textContent = mailboxAddress(message.from);
      $("senderAvatar").textContent = (message.from?.name || message.from?.address || "M").slice(0, 1).toUpperCase();
      $("messageTo").textContent = `To: ${message.to.map(mailboxAddress).join(", ")}${message.cc.length ? ` \xB7 Cc: ${message.cc.map(mailboxAddress).join(", ")}` : ""}`;
      $("messageDate").textContent = dateLabel(message.date, true);
      $("downloadRaw").href = `/cmail/api/messages/${id}/raw`;
      $("attachments").replaceChildren();
      message.attachments.forEach((file) => {
        const link = document.createElement("a");
        link.className = "attachment";
        link.href = `/cmail/api/messages/${id}/attachments/${file.index}`;
        link.textContent = `\u2193 ${file.filename}`;
        addText(link, "span", "", `${Math.max(1, Math.round(file.size / 1024))} KB`);
        $("attachments").append(link);
      });
      renderBody(message.text);
      $("readerPlaceholder").hidden = true;
      $("message").hidden = false;
      document.body.classList.add("reading");
      document.querySelector(".reader").scrollTop = 0;
      connected("Cloud inbox");
      const item = items.find((item2) => item2.id === id);
      if (item) item.read = message.read;
      renderList();
      updateReadControl();
      loadConversation(message);
      if (!message.read) await setReadState(id, true);
    } catch (error) {
      if (generation === readingGeneration) {
        notice(error.message);
        connected("Connection interrupted");
      }
    }
  }
  $("searchForm").addEventListener("submit", (event) => {
    event.preventDefault();
    load(true);
  });
  $("refresh").addEventListener("click", () => {
    conversationBodies.clear();
    load(true);
    if (selectedMessage) loadConversation(selectedMessage);
  });
  $("mailbox").addEventListener("change", switchInbox);
  $("mailView").addEventListener("change", switchInbox);
  function applyDates() {
    if (bulkBusy) return;
    $("dateFilters").open = false;
    $("dateSummary").textContent = $("after").value || $("before").value ? "Dates \u2022" : "Dates";
    closeMessage();
    load(true);
  }
  $("applyDates").addEventListener("click", applyDates);
  $("clearDates").addEventListener("click", () => {
    $("after").value = "";
    $("before").value = "";
    applyDates();
  });
  $("selectPage").addEventListener("change", () => {
    const checked = $("selectPage").checked;
    for (const item of items.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize)) if (checked) checkedIds.add(item.id);
    else checkedIds.delete(item.id);
    renderList();
  });
  $("clearSelection").addEventListener("click", clearSelection);
  $("actionsButton").addEventListener("click", () => {
    const open = $("actionsMenu").hidden;
    $("actionsMenu").hidden = !open;
    $("actionsButton").setAttribute("aria-expanded", String(open));
    if (open) $("dateFilters").open = false;
  });
  document.querySelectorAll("[data-action]").forEach((button) => button.addEventListener("click", () => applyBulkAction(button.dataset.action)));
  document.addEventListener("click", (event) => {
    if (!event.target.closest(".bulk-menu")) closeActions();
    if (!event.target.closest(".date-filters")) $("dateFilters").open = false;
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeActions();
      $("dateFilters").open = false;
    }
  });
  $("previousPage").addEventListener("click", () => changePage(-1));
  $("nextPage").addEventListener("click", () => changePage(1));
  $("continueLoad").addEventListener("click", () => load());
  $("pageSize").value = String(pageSize);
  $("pageSize").addEventListener("change", () => {
    if (bulkBusy) return;
    checkedIds.clear();
    closeActions();
    pageSize = Number($("pageSize").value);
    pageIndex = 0;
    try {
      localStorage.setItem("cmail.page-size", String(pageSize));
    } catch {
    }
    closeMessage();
    $("messages").scrollTop = 0;
    renderList();
    if (!exhausted && items.length < pageSize + 1) load();
  });
  $("toggleRead").addEventListener("click", () => {
    if (selectedMessage) setReadState(selectedMessage.id, !selectedMessage.read);
  });
  $("copyMessage").addEventListener("click", () => {
    if (selectedMessage) copyText(formatMessage(selectedMessage), $("copyMessage"), "Message");
  });
  $("copyThread").addEventListener("click", async () => {
    if (!conversationMessages.length) return;
    const generation = conversationGeneration;
    const signal = conversationController?.signal;
    const button = $("copyThread");
    button.disabled = true; button.textContent = "Preparing thread…";
    try {
      const all = [];
      for (let offset = 0; offset < conversationMessages.length; offset += 3) {
        all.push(...await Promise.all(conversationMessages.slice(offset, offset + 3).map((item) => conversationBody(item, signal))));
        if (generation !== conversationGeneration || signingOut) return;
      }
      all.sort((a, b) => (Date.parse(a.date) || Date.parse(a.received_at)) - (Date.parse(b.date) || Date.parse(b.received_at)) || a.id.localeCompare(b.id));
      await copyText(formatThread(all), button, "Thread");
    } catch { if (generation === conversationGeneration) actionStatus("Could not prepare the whole thread. Please try again."); }
    finally { button.textContent = "Copy thread"; button.disabled = !conversationMessages.length; }
  });
  $("retryThread").addEventListener("click", () => {
    conversationBodies.clear();
    if (selectedMessage) loadConversation(selectedMessage);
  });
  $("closeCopy").addEventListener("click", () => $("copyFallback").close());
  $("selectCopyText").addEventListener("click", () => {
    $("copyText").focus();
    $("copyText").select();
  });
  $("stopSearch").addEventListener("click", () => loadController?.abort());
  $("signOut").addEventListener("click", (event) => {
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    signingOut = true;
    requestGeneration += 1;
    readingGeneration += 1;
    loadController?.abort();
    clearTimeout(actionStatusTimer);
    conversationController?.abort();
    conversationGeneration += 1;
    conversationMessages = [];
    conversationBodies.clear();
    document.body.replaceChildren();
    document.title = "Signing out \xB7 C-mail";
  });
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) window.location.reload();
  });
  $("back").addEventListener("click", closeMessage);
  api("session").then((session) => {
    if (signingOut) return;
    $("signOut").title = `Sign out ${session.email}`;
    addMailboxes(session.mailboxes || []);
  }).catch((error) => {
    if (!signingOut) notice(error.message);
  });
  function openLinkedMessage() {
    if (signingOut) return;
    const linked = window.location.hash.match(/^#(mail_\d{16})$/);
    if (linked) return openMessage(linked[1]);
  }
  window.addEventListener("hashchange", openLinkedMessage);
  var initialMailbox = new URLSearchParams(window.location.search).get("mailbox") || "";
  var initialView = new URLSearchParams(window.location.search).get("view");
  if (["inbox", "archive", "all"].includes(initialView)) $("mailView").value = initialView;
  if (initialMailbox) {
    addMailboxes([initialMailbox]);
    $("mailbox").value = initialMailbox.trim().toLowerCase();
  }
  updateInboxLabel();
  load(true);
  openLinkedMessage();
}

export const appScript = "(" + application.toString() + ")();";
