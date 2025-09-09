# email-report
Script for generating a report about your Gmail account, max # of messages, and storage amount being consumed

### How to use
1. Open your target Google Sheet; paste this script
2. In the AppsScript menu > Services (left side) > add Advanced Gmail service (`Gmail`) 
3. Share the sheet with as many Google accounts as you want the report to run on.
4. In the Sheet: `Email Audit` → Start / Continue Audit (default query) for Account A. Do the same while logged in as Account B.
6. Watch `audit_progress` update; partial results appear in each account’s tab every few pages.
7. The script auto-continues via a time-driven trigger until status = DONE. You can Reset State from the menu if needed.

### Error handling
- Time limit: each run processes a bounded number of pages/time and then resumes from the saved pageToken via trigger.
- Empty response: handled by retries; if still bad, the message is skipped and logged so one bad item never stalls the audit.
