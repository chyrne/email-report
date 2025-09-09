/***** CONFIG *****/
const SPREADSHEET_ID       = "";             // Optional: central Sheet ID; "" = use bound sheet.
const DEFAULT_QUERY        = "in:anywhere";  // Broad for cleanup; narrow later if desired.
const MAX_PAGES_PER_RUN    = 8;              // Process at most N pages per execution.
const MAX_MS_PER_RUN       = 5 * 60 * 1000;  // Safety stop ~5 minutes per run (defensive).
const WRITE_EVERY_PAGES    = 3;              // Flush partial results to sheet every N pages.
const CONTINUE_EVERY_MIN   = 5;              // Continuation trigger cadence (minutes).
const RETRY_MAX            = 7;
const RETRY_BASE_MS        = 400;
const RETRY_JITTER_MS      = 300;

/***** MENU *****/
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Email Audit")
    .addItem("Start / Continue Audit (default query)", "startAudit")
    .addItem("Start / Continue Audit (prompt query)", "startAuditWithPrompt")
    .addItem("Reset State (this account)", "resetAuditState")
    .addToUi();
}

function startAudit() {
  ensureContinuationTrigger_();
  processChunk_(DEFAULT_QUERY);
}

function startAuditWithPrompt() {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt(
    "Gmail search query",
    "Enter a Gmail search query (e.g., in:anywhere newer_than:24m). Leave blank for default:",
    ui.ButtonSet.OK_CANCEL
  );
  if (resp.getSelectedButton() !== ui.Button.OK) return;
  const query = (resp.getResponseText() || "").trim() || DEFAULT_QUERY;
  ensureContinuationTrigger_();
  processChunk_(query);
}

/**
 * This runs on the continuation trigger until the run is done.
 */
function continueAudit() {
  // Resume using the most recent query for this account (if multiple, pick the one with status RUNNING)
  const ss = openTargetSpreadsheet_();
  const progressSh = getOrCreateProgressSheet_(ss);
  const accountEmail = Session.getActiveUser().getEmail() || "me";
  const runningQuery = findRunningQueryForAccount_(progressSh, accountEmail);
  if (!runningQuery) {
    // Nothing to do; remove trigger if no other account/query is running under this user
    maybeRemoveContinuationTrigger_();
    return;
  }
  processChunk_(runningQuery);
}

/***** CORE *****/
function processChunk_(query) {
  const startMs = Date.now();
  const ss = openTargetSpreadsheet_();
  const accountEmail = Session.getActiveUser().getEmail() || "me";
  const sheetName = makeSheetNameForAccount_(accountEmail);
  const header = ["domain", "sender", "messages", "threads", "total_size_mb", "query", "generated_at"];

  // Initialize progress + state
  const progressSh = getOrCreateProgressSheet_(ss);
  const errorsSh   = getOrCreateErrorsSheet_(ss);
  const stateKey   = makeStateKey_(accountEmail, query);
  const stateStore = PropertiesService.getUserProperties();
  let state = readState_(stateStore, stateKey) || {
    pageToken: null,
    pagesDone: 0,
    messagesDone: 0,
    lastFlushAt: 0
  };

  upsertProgress_(progressSh, accountEmail, {
    status: "RUNNING",
    query,
    pages_done: state.pagesDone,
    messages_done: state.messagesDone,
    last_page_size: 0,
    last_error: ""
  });

  // Aggregation (we rebuild table each flush from state? Simpler: accumulate in-memory each run then flush)
  // To avoid re-reading earlier pages, we only aggregate what we see this execution PLUS
  // a lightweight "read-modify-rewrite" approach:
  //   1) Read existing rows for this account from the sheet (if present) to seed aggregation.
  //   2) Append this run’s new messages to the aggregation.
  const agg = readExistingAggregation_(ss, sheetName, header, query);

  // Ensure destination sheet visible before first flush
  ensureSheetHasHeader_(ss, sheetName, header);

  const userId = "me";
  let pagesThisRun = 0;
  let done = false;

  try {
    do {
      if (pagesThisRun >= MAX_PAGES_PER_RUN) break;
      if (Date.now() - startMs > MAX_MS_PER_RUN) break;

      // List page
      const listParams = {
        q: query,
        maxResults: 500,
        pageToken: state.pageToken || null,
        fields: "messages/id,messages/threadId,nextPageToken"
      };
      const resp = gmailWithRetry_(() => Gmail.Users.Messages.list(userId, listParams));
      const messages = resp.messages || [];
      const pageSize = messages.length;

      if (!pageSize) {
        done = true;
        break;
      }

      // Per-message metadata
      for (const m of messages) {
        let msg;
        try {
          msg = gmailWithRetry_(() =>
            Gmail.Users.Messages.get(userId, m.id, {
              format: "metadata",
              metadataHeaders: ["From"],
              fields: "id,threadId,payload/headers,sizeEstimate"
            })
          );
        } catch (e) {
          // Handle "Empty response" or other terminal errors: log + skip
          appendError_(errorsSh, accountEmail, query, m.id, state.pagesDone + 1, String(e && e.message ? e.message : e));
          continue;
        }

        // Guard against null/empty objects
        if (!msg || !msg.payload) {
          appendError_(errorsSh, accountEmail, query, m.id, state.pagesDone + 1, "Empty or malformed message response");
          continue;
        }

        const threadId  = msg.threadId;
        const sizeBytes = Number(msg.sizeEstimate || 0);
        const fromHdr   = getHeader_(msg, "From") || "";
        const sender    = extractEmailAddress_(fromHdr) || "(unknown)";
        const domain    = sender.includes("@") ? sender.split("@").pop().toLowerCase() : "(unknown)";

        upsertAgg_(agg, domain, sender, threadId, sizeBytes);
      }

      // Advance state
      state.pagesDone += 1;
      state.messagesDone += pageSize;
      state.pageToken = resp.nextPageToken || null;
      pagesThisRun += 1;

      upsertProgress_(progressSh, accountEmail, {
        status: "RUNNING",
        query,
        pages_done: state.pagesDone,
        messages_done: state.messagesDone,
        last_page_size: pageSize,
        last_error: ""
      });
      SpreadsheetApp.getActive().toast(`Processed page ${state.pagesDone} (${pageSize} msgs) for ${accountEmail}`);

      // Periodic flush
      if (state.pagesDone % WRITE_EVERY_PAGES === 0 || !state.pageToken) {
        flushAggToSheet_(ss, sheetName, header, agg, query);
        state.lastFlushAt = Date.now();
      }

      if (!state.pageToken) {
        done = true;
        break;
      }
    } while (true);

    // Finalize
    if (done) {
      flushAggToSheet_(ss, sheetName, header, agg, query);
      upsertProgress_(progressSh, accountEmail, {
        status: "DONE",
        query,
        pages_done: state.pagesDone,
        messages_done: state.messagesDone,
        last_page_size: 0,
        last_error: ""
      });
      clearState_(stateStore, stateKey);
      maybeRemoveContinuationTrigger_();
      SpreadsheetApp.getActive().toast(`Audit DONE for ${accountEmail} — ${state.pagesDone} pages, ${state.messagesDone} messages.`);
    } else {
      // Persist state for next chunk
      writeState_(stateStore, stateKey, state);
      ensureContinuationTrigger_();
      SpreadsheetApp.getActive().toast(`Paused (time/page cap). Will continue via trigger. Pages so far: ${state.pagesDone}`);
    }

  } catch (e) {
    // Persist state and mark error
    writeState_(stateStore, stateKey, state);
    upsertProgress_(progressSh, accountEmail, {
      status: "ERROR",
      query,
      pages_done: state.pagesDone,
      messages_done: state.messagesDone,
      last_page_size: 0,
      last_error: String(e && e.message ? e.message : e)
    });
    ensureContinuationTrigger_(); // try again on next tick for transient failures
    throw e;
  }
}

/***** STATE MGMT *****/
function makeStateKey_(accountEmail, query) {
  return `audit_state::${accountEmail}::${query}`;
}
function readState_(store, key) {
  const raw = store.getProperty(key);
  return raw ? JSON.parse(raw) : null;
}
function writeState_(store, key, obj) {
  store.setProperty(key, JSON.stringify(obj));
}
function clearState_(store, key) {
  store.deleteProperty(key);
}
function resetAuditState() {
  const store = PropertiesService.getUserProperties();
  const all = store.getProperties();
  Object.keys(all).forEach(k => {
    if (k.startsWith("audit_state::")) store.deleteProperty(k);
  });
  SpreadsheetApp.getActive().toast("Audit state cleared for this account.");
}

/***** TRIGGERS *****/
function ensureContinuationTrigger_() {
  const triggers = ScriptApp.getProjectTriggers().filter(t => t.getHandlerFunction() === "continueAudit");
  if (triggers.length === 0) {
    ScriptApp.newTrigger("continueAudit").timeBased().everyMinutes(CONTINUE_EVERY_MIN).create();
  }
}
function maybeRemoveContinuationTrigger_() {
  // Remove the trigger if nothing is RUNNING for this account
  const ss = openTargetSpreadsheet_();
  const progressSh = getOrCreateProgressSheet_(ss);
  const accountEmail = Session.getActiveUser().getEmail() || "me";
  const anyRunning = !!findRunningQueryForAccount_(progressSh, accountEmail);
  if (!anyRunning) {
    ScriptApp.getProjectTriggers()
      .filter(t => t.getHandlerFunction() === "continueAudit")
      .forEach(t => ScriptApp.deleteTrigger(t));
  }
}
function findRunningQueryForAccount_(sh, accountEmail) {
  const vals = sh.getDataRange().getValues();
  const header = vals[0] || [];
  const idxEmail = header.indexOf("account_email");
  const idxStatus = header.indexOf("status");
  const idxQuery = header.indexOf("query");
  for (let i = 1; i < vals.length; i++) {
    const row = vals[i];
    if (row[idxEmail] === accountEmail && row[idxStatus] === "RUNNING") {
      return row[idxQuery];
    }
  }
  return null;
}

/***** AGGREGATION *****/
function upsertAgg_(agg, domain, sender, threadId, sizeBytes) {
  if (!agg.has(domain)) agg.set(domain, new Map());
  const inner = agg.get(domain);
  if (!inner.has(sender)) inner.set(sender, { msgCount: 0, sizeBytes: 0, threadIds: new Set() });
  const a = inner.get(sender);
  a.msgCount += 1;
  a.sizeBytes += sizeBytes;
  a.threadIds.add(threadId);
}

function readExistingAggregation_(ss, sheetName, header, query) {
  const sh = ss.getSheetByName(sheetName);
  const agg = new Map();
  if (!sh) return agg;
  const lastRow = sh.getLastRow();
  if (lastRow <= 1) return agg;

  const vals = sh.getRange(2, 1, lastRow - 1, header.length).getValues();
  const idxDomain = 0, idxSender = 1, idxMessages = 2, idxThreads = 3, idxMB = 4, idxQuery = 5;
  for (const r of vals) {
    if ((r[idxQuery] || "") !== query) continue; // keep separate by query
    const domain = r[idxDomain];
    const sender = r[idxSender];
    // We can’t reconstruct threadIds from the sheet; seed counts and size only.
    if (!agg.has(domain)) agg.set(domain, new Map());
    const inner = agg.get(domain);
    if (!inner.has(sender)) inner.set(sender, { msgCount: 0, sizeBytes: 0, threadIds: new Set() });
    const a = inner.get(sender);
    a.msgCount = Number(r[idxMessages]) || a.msgCount;
    a.sizeBytes = (Number(r[idxMB]) || 0) * 1024 * 1024; // approximate back to bytes
    // threadIds Set will grow only with new pages; that’s okay—thread count will be an underestimate until run completes.
  }
  return agg;
}

/***** SHEETS I/O *****/
function openTargetSpreadsheet_() {
  return SPREADSHEET_ID ? SpreadsheetApp.openById(SPREADSHEET_ID) : SpreadsheetApp.getActiveSpreadsheet();
}

function ensureSheetHasHeader_(ss, name, header) {
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
  }
  const existing = sh.getRange(1, 1, 1, header.length).getValues()[0] || [];
  const same = existing.join("|") === header.join("|");
  if (!same) {
    sh.clear();
    sh.getRange(1, 1, 1, header.length).setValues([header]);
    sh.setFrozenRows(1);
  }
  return sh;
}

function flushAggToSheet_(ss, sheetName, header, agg, query) {
  const sh = ensureSheetHasHeader_(ss, sheetName, header);
  // Build rows
  const rows = [];
  for (const [domain, inner] of agg.entries()) {
    const senders = Array.from(inner.entries()).sort((a, b) => (b[1].sizeBytes - a[1].sizeBytes));
    for (const [sender, a] of senders) {
      rows.push([
        domain,
        sender,
        a.msgCount,
        a.threadIds.size, // accurate once full run completes
        roundMB_(a.sizeBytes),
        query,
        new Date()
      ]);
    }
  }
  // Sort domain asc, size desc
  rows.sort((a, b) => (a[0] === b[0]) ? (b[4] - a[4]) : (a[0] < b[0] ? -1 : 1));

  // Overwrite body
  const lastCol = header.length;
  const lastRow = sh.getLastRow();
  if (lastRow > 1) sh.getRange(2, 1, lastRow - 1, lastCol).clearContent();
  if (rows.length) sh.getRange(2, 1, rows.length, lastCol).setValues(rows);
  sh.setFrozenRows(1);
  sh.autoResizeColumns(1, sh.getLastColumn());
}

function getOrCreateProgressSheet_(ss) {
  let sh = ss.getSheetByName("audit_progress");
  if (!sh) {
    sh = ss.insertSheet("audit_progress");
    sh.getRange(1, 1, 1, 8).setValues([[
      "account_email", "status", "query", "pages_done", "messages_done", "last_page_size", "last_update", "last_error"
    ]]);
    sh.setFrozenRows(1);
  }
  return sh;
}

function upsertProgress_(sh, accountEmail, {status, query, pages_done, messages_done, last_page_size, last_error}) {
  const now = new Date();
  const vals = sh.getDataRange().getValues();
  const header = vals[0];
  const idx = {
    email: header.indexOf("account_email"),
    status: header.indexOf("status"),
    query: header.indexOf("query"),
    pages: header.indexOf("pages_done"),
    msgs: header.indexOf("messages_done"),
    last: header.indexOf("last_page_size"),
    upd: header.indexOf("last_update"),
    err: header.indexOf("last_error")
  };
  let rowIdx = -1;
  for (let i = 1; i < vals.length; i++) {
    if (vals[i][idx.email] === accountEmail && vals[i][idx.query] === query) {
      rowIdx = i + 1;
      break;
    }
  }
  const row = [
    accountEmail,
    status,
    query,
    Number(pages_done) || 0,
    Number(messages_done) || 0,
    Number(last_page_size) || 0,
    now,
    last_error || ""
  ];
  if (rowIdx === -1) {
    sh.appendRow(row);
  } else {
    sh.getRange(rowIdx, 1, 1, row.length).setValues([row]);
  }
}

function getOrCreateErrorsSheet_(ss) {
  let sh = ss.getSheetByName("audit_errors");
  if (!sh) {
    sh = ss.insertSheet("audit_errors");
    sh.getRange(1,1,1,6).setValues([["account_email","query","message_id","page_number","error","logged_at"]]);
    sh.setFrozenRows(1);
  }
  return sh;
}
function appendError_(sh, accountEmail, query, messageId, pageNumber, err) {
  sh.appendRow([accountEmail, query, messageId, pageNumber, String(err || ""), new Date()]);
}

/***** GMAIL HELPERS *****/
function gmailWithRetry_(fn) {
  let attempt = 0;
  while (true) {
    try {
      return fn();
    } catch (e) {
      attempt++;
      if (attempt > RETRY_MAX) throw e;
      const backoff = RETRY_BASE_MS * Math.pow(2, attempt - 1) + Math.floor(Math.random() * RETRY_JITTER_MS);
      Utilities.sleep(backoff);
    }
  }
}

function getHeader_(msg, name) {
  const hdrs = (msg && msg.payload && msg.payload.headers) || [];
  const target = name.toLowerCase();
  for (const h of hdrs) if ((h.name || "").toLowerCase() === target) return h.value || "";
  return "";
}

function extractEmailAddress_(fromValue) {
  if (!fromValue) return "";
  const angle = fromValue.match(/<([^>]+)>/);
  if (angle && angle[1]) return angle[1].trim().toLowerCase();
  const bare = fromValue.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  if (bare && bare[0]) return bare[0].trim().toLowerCase();
  return fromValue.trim().toLowerCase();
}

function roundMB_(bytes) {
  const mb = bytes / (1024 * 1024);
  return Math.round(mb);
}

function makeSheetNameForAccount_(email) {
  const safe = String(email).toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 80);
  return `audit_${safe}`;
}
