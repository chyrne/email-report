var EmailAudit = this.EmailAudit || {}; // global namespace

EmailAudit.Config = {
  SPREADSHEET_ID: "",                 // central sheet ("" = bound)
  DEFAULT_QUERY: "in:anywhere",
  WRITE_EVERY_PAGES: 3,
  MAX_PAGES_PER_RUN: 8,
  MAX_MS_PER_RUN: 5 * 60 * 1000,
  CONTINUE_EVERY_MIN: 5,
  RETRY_MAX: 7,
  RETRY_BASE_MS: 400,
  RETRY_JITTER_MS: 300
};
