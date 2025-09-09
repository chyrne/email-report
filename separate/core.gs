EmailAudit.Core = (function () {
  const C = EmailAudit.Config;
  const G = EmailAudit.Gmail;
  const S = EmailAudit.Sheets;
  const ST = EmailAudit.State;
  const U = EmailAudit.Util;

  function startOrContinue(query) {
    EmailAudit.Triggers.ensure();
    processChunk(query);
  }

  function promptAndStart() {
    const ui = SpreadsheetApp.getUi();
    const res = ui.prompt("Gmail search query", "Leave blank for default:", ui.ButtonSet.OK_CANCEL);
    if (res.getSelectedButton() !== ui.Button.OK) return;
    const q = (res.getResponseText() || "").trim() || C.DEFAULT_QUERY;
    startOrContinue(q);
  }

  function continueTick() {
    const ss = S.openTargetSpreadsheet();
    const acc = Session.getActiveUser().getEmail() || "me";
    const q = S.findRunningQuery(ss, acc);
    if (!q) { EmailAudit.Triggers.maybeRemove(); return; }
    processChunk(q);
  }

  function processChunk(query) {
    const ss = S.openTargetSpreadsheet();
    const acc = Session.getActiveUser().getEmail() || "me";
    const sheetName = U.safeSheetName(acc);
    const header = ["domain","sender","messages","threads","total_size_mb","query","generated_at"];

    S.ensureSheetHeader(ss, sheetName, header);
    const progress = S.getOrCreateProgress(ss);
    const errors   = S.getOrCreateErrors(ss);

    const key   = ST.key(acc, query);
    const state = ST.read(key) || { pageToken:null, pagesDone:0, messagesDone:0, lastFlushAt:0 };
    S.upsertProgress(progress, acc, { status:"RUNNING", query, pages_done:state.pagesDone, messages_done:state.messagesDone, last_page_size:0, last_error:"" });

    const agg = S.readExistingAggregation(ss, sheetName, header, query);

    const startMs = Date.now();
    let pagesThisRun = 0, done = false;

    while (true) {
      if (pagesThisRun >= C.MAX_PAGES_PER_RUN) break;
      if (Date.now() - startMs > C.MAX_MS_PER_RUN) break;

      const listResp = G.listMessages(query, state.pageToken);
      const msgs = listResp.messages || [];
      if (!msgs.length) { done = true; break; }

      for (const m of msgs) {
        const msg = G.getMessageSafe(m.id, errors, acc, query, state.pagesDone + 1);
        if (!msg) continue;
        const from = G.getHeader(msg, "From");
        const sender = U.extractEmail(from) || "(unknown)";
        const domain = sender.includes("@") ? sender.split("@").pop().toLowerCase() : "(unknown)";
        S.upsertAgg(agg, domain, sender, msg.threadId, Number(msg.sizeEstimate || 0));
      }

      state.pagesDone += 1;
      state.messagesDone += msgs.length;
      state.pageToken = listResp.nextPageToken || null;
      pagesThisRun += 1;

      S.upsertProgress(progress, acc, { status:"RUNNING", query, pages_done:state.pagesDone, messages_done:state.messagesDone, last_page_size:msgs.length, last_error:"" });
      SpreadsheetApp.getActive().toast(`Page ${state.pagesDone} (${msgs.length}) — ${acc}`);

      if (state.pagesDone % C.WRITE_EVERY_PAGES === 0 || !state.pageToken) {
        S.flushAgg(ss, sheetName, header, agg, query);
        state.lastFlushAt = Date.now();
      }

      if (!state.pageToken) { done = true; break; }
    }

    if (done) {
      S.flushAgg(ss, sheetName, header, agg, query);
      S.upsertProgress(progress, acc, { status:"DONE", query, pages_done:state.pagesDone, messages_done:state.messagesDone, last_page_size:0, last_error:"" });
      ST.clear(key);
      EmailAudit.Triggers.maybeRemove();
      SpreadsheetApp.getActive().toast(`DONE — ${acc}: ${state.pagesDone} pages, ${state.messagesDone} msgs`);
    } else {
      ST.write(key, state);
      EmailAudit.Triggers.ensure();
      SpreadsheetApp.getActive().toast(`Paused — will continue. Pages: ${state.pagesDone}`);
    }
  }

  return { startOrContinue, promptAndStart, continueTick };
})();
