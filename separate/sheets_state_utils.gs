EmailAudit.Sheets = (function () {
  const C = EmailAudit.Config;

  function openTargetSpreadsheet() {
    return C.SPREADSHEET_ID ? SpreadsheetApp.openById(C.SPREADSHEET_ID) : SpreadsheetApp.getActiveSpreadsheet();
  }

  function ensureSheetHeader(ss, name, header) {
    let sh = ss.getSheetByName(name);
    if (!sh) sh = ss.insertSheet(name);
    const existing = sh.getRange(1, 1, 1, header.length).getValues()[0] || [];
    if (existing.join("|") !== header.join("|")) {
      sh.clear();
      sh.getRange(1, 1, 1, header.length).setValues([header]);
      sh.setFrozenRows(1);
    }
    return sh;
  }

  function getOrCreateProgress(ss) {
    let sh = ss.getSheetByName("audit_progress");
    if (!sh) {
      sh = ss.insertSheet("audit_progress");
      sh.getRange(1, 1, 1, 8).setValues([[
        "account_email","status","query","pages_done","messages_done","last_page_size","last_update","last_error"
      ]]);
      sh.setFrozenRows(1);
    }
    return sh;
  }

  function upsertProgress(sh, email, obj) {
    const now = new Date();
    const vals = sh.getDataRange().getValues();
    const hdr = vals[0];
    const idx = {
      email: hdr.indexOf("account_email"),
      status: hdr.indexOf("status"),
      query:  hdr.indexOf("query"),
      pages:  hdr.indexOf("pages_done"),
      msgs:   hdr.indexOf("messages_done"),
      last:   hdr.indexOf("last_page_size"),
      upd:    hdr.indexOf("last_update"),
      err:    hdr.indexOf("last_error")
    };
    let rowIdx = -1;
    for (let i=1;i<vals.length;i++) if (vals[i][idx.email]===email && vals[i][idx.query]===obj.query) { rowIdx=i+1; break; }
    const row = [email,obj.status,obj.query,Number(obj.pages_done)||0,Number(obj.messages_done)||0,Number(obj.last_page_size)||0,now,obj.last_error||""];
    if (rowIdx === -1) sh.appendRow(row); else sh.getRange(rowIdx,1,1,row.length).setValues([row]);
  }

  function getOrCreateErrors(ss) {
    let sh = ss.getSheetByName("audit_errors");
    if (!sh) {
      sh = ss.insertSheet("audit_errors");
      sh.getRange(1,1,1,6).setValues([[ "account_email","query","message_id","page_number","error","logged_at" ]]);
      sh.setFrozenRows(1);
    }
    return sh;
  }
  function appendError(sh, accountEmail, query, messageId, pageNumber, err) {
    sh.appendRow([accountEmail, query, messageId, pageNumber, String(err||""), new Date()]);
  }

  function readExistingAggregation(ss, sheetName, header, query) {
    const sh = ss.getSheetByName(sheetName);
    const agg = new Map();
    if (!sh) return agg;
    const lastRow = sh.getLastRow();
    if (lastRow <= 1) return agg;
    const vals = sh.getRange(2,1,lastRow-1, header.length).getValues();
    for (const r of vals) {
      if ((r[5]||"") !== query) continue; // query column
      const domain = r[0], sender = r[1];
      if (!agg.has(domain)) agg.set(domain, new Map());
      const inner = agg.get(domain);
      if (!inner.has(sender)) inner.set(sender, { msgCount:0, sizeBytes:0, threadIds:new Set() });
      const a = inner.get(sender);
      a.msgCount = Number(r[2]) || a.msgCount;
      a.sizeBytes = (Number(r[4])||0) * 1024 * 1024;
    }
    return agg;
  }

  function upsertAgg(agg, domain, sender, threadId, sizeBytes) {
    if (!agg.has(domain)) agg.set(domain, new Map());
    const inner = agg.get(domain);
    if (!inner.has(sender)) inner.set(sender, { msgCount:0, sizeBytes:0, threadIds:new Set() });
    const a = inner.get(sender);
    a.msgCount += 1;
    a.sizeBytes += sizeBytes;
    a.threadIds.add(threadId);
  }

  function flushAgg(ss, sheetName, header, agg, query) {
    const sh = ensureSheetHeader(ss, sheetName, header);
    const rows = [];
    for (const [domain, inner] of agg.entries()) {
      const arr = Array.from(inner.entries()).sort((a,b)=> b[1].sizeBytes - a[1].sizeBytes);
      for (const [sender, a] of arr) {
        rows.push([domain, sender, a.msgCount, a.threadIds.size, EmailAudit.Util.roundMB(a.sizeBytes), query, new Date()]);
      }
    }
    rows.sort((a,b)=> a[0]===b[0] ? (b[4]-a[4]) : (a[0]<b[0] ? -1 : 1));

    const lastCol = header.length;
    const lastRow = sh.getLastRow();
    if (lastRow > 1) sh.getRange(2,1,lastRow-1,lastCol).clearContent();
    if (rows.length) sh.getRange(2,1,rows.length,lastCol).setValues(rows);
    sh.setFrozenRows(1);
    sh.autoResizeColumns(1, sh.getLastColumn());
  }

  function findRunningQuery(ss, accountEmail) {
    const sh = getOrCreateProgress(ss);
    const vals = sh.getDataRange().getValues();
    const hdr = vals[0] || [];
    const idxEmail = hdr.indexOf("account_email");
    const idxStatus= hdr.indexOf("status");
    const idxQuery = hdr.indexOf("query");
    for (let i=1;i<vals.length;i++) {
      if (vals[i][idxEmail]===accountEmail && vals[i][idxStatus]==="RUNNING") return vals[i][idxQuery];
    }
    return null;
  }

  return {
    openTargetSpreadsheet, ensureSheetHeader,
    getOrCreateProgress, upsertProgress,
    getOrCreateErrors, appendError,
    readExistingAggregation, upsertAgg, flushAgg,
    findRunningQuery
  };
})();

EmailAudit.State = (function () {
  function key(email, query) { return `audit_state::${email}::${query}`; }
  function read(key)  { const raw = PropertiesService.getUserProperties().getProperty(key); return raw ? JSON.parse(raw) : null; }
  function write(key, obj) { PropertiesService.getUserProperties().setProperty(key, JSON.stringify(obj)); }
  function clear(key) { PropertiesService.getUserProperties().deleteProperty(key); }
  function reset() {
    const store = PropertiesService.getUserProperties();
    const all = store.getProperties();
    Object.keys(all).forEach(k => { if (k.startsWith("audit_state::")) store.deleteProperty(k); });
    SpreadsheetApp.getActive().toast("Audit state cleared for this account.");
  }
  return { key, read, write, clear, reset };
})();

EmailAudit.Triggers = (function () {
  const C = EmailAudit.Config;
  function ensure() {
    const ts = ScriptApp.getProjectTriggers().filter(t => t.getHandlerFunction()==="continueAudit");
    if (ts.length===0) ScriptApp.newTrigger("continueAudit").timeBased().everyMinutes(C.CONTINUE_EVERY_MIN).create();
  }
  function maybeRemove() {
    // remove if nothing RUNNING for this account
    const ss = EmailAudit.Sheets.openTargetSpreadsheet();
    const acc = Session.getActiveUser().getEmail() || "me";
    const running = EmailAudit.Sheets.findRunningQuery(ss, acc);
    if (!running) {
      ScriptApp.getProjectTriggers()
        .filter(t => t.getHandlerFunction()==="continueAudit")
        .forEach(t => ScriptApp.deleteTrigger(t));
    }
  }
  return { ensure, maybeRemove };
})();

EmailAudit.Util = (function () {
  function extractEmail(fromValue) {
    if (!fromValue) return "";
    const angle = fromValue.match(/<([^>]+)>/);
    if (angle && angle[1]) return angle[1].trim().toLowerCase();
    const bare = fromValue.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    if (bare && bare[0]) return bare[0].trim().toLowerCase();
    return fromValue.trim().toLowerCase();
  }
  function roundMB(bytes) { return Math.round(bytes / (1024*1024)); }
  function safeSheetName(email) {
    const safe = String(email).toLowerCase().replace(/[^a-z0-9]+/g,"_").replace(/^_+|_+$/g,"").slice(0,80);
    return `audit_${safe}`;
  }
  return { extractEmail, roundMB, safeSheetName };
})();
