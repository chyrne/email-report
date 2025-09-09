function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Email Audit")
    .addItem("Start / Continue (default)", "startAudit")
    .addItem("Start / Continue (prompt)", "startAuditWithPrompt")
    .addItem("Reset State (this account)", "resetAuditState")
    .addToUi();
}

function startAudit()         { EmailAudit.Core.startOrContinue(EmailAudit.Config.DEFAULT_QUERY); }
function startAuditWithPrompt() { EmailAudit.Core.promptAndStart(); }
function continueAudit()      { EmailAudit.Core.continueTick(); }
function resetAuditState()    { EmailAudit.State.reset(); }
