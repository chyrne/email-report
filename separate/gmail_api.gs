EmailAudit.Gmail = (function () {
  const C = EmailAudit.Config;
  const U = EmailAudit.Util;

  function withRetry(fn) {
    let attempt = 0;
    while (true) {
      try { return fn(); }
      catch (e) {
        attempt++;
        if (attempt > C.RETRY_MAX) throw e;
        const backoff = C.RETRY_BASE_MS * Math.pow(2, attempt - 1) + Math.floor(Math.random() * C.RETRY_JITTER_MS);
        Utilities.sleep(backoff);
      }
    }
  }

  function listMessages(query, pageToken) {
    return withRetry(() =>
      Gmail.Users.Messages.list("me", {
        q: query,
        maxResults: 500,
        pageToken: pageToken || null,
        fields: "messages/id,messages/threadId,nextPageToken"
      })
    );
  }

  function getMessageSafe(id, errorsSh, accountEmail, query, pageNumber) {
    try {
      const msg = withRetry(() =>
        Gmail.Users.Messages.get("me", id, {
          format: "metadata",
          metadataHeaders: ["From"],
          fields: "id,threadId,payload/headers,sizeEstimate"
        })
      );
      if (!msg || !msg.payload) throw new Error("Empty or malformed message response");
      return msg;
    } catch (e) {
      EmailAudit.Sheets.appendError(errorsSh, accountEmail, query, id, pageNumber, String(e && e.message ? e.message : e));
      return null;
    }
  }

  function getHeader(msg, name) {
    const headers = (msg && msg.payload && msg.payload.headers) || [];
    const target = name.toLowerCase();
    for (const h of headers) if ((h.name || "").toLowerCase() === target) return h.value || "";
    return "";
  }

  return { listMessages, getMessageSafe, getHeader };
})();
