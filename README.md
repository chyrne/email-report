# email-report
Script for generating a report about your Gmail account, max # of messages, and storage amount being consumed

Caveat: much heavy lifting from AI-assisted coding.

### Background
* Do you keep running out of space cause of email attachments from a previous life years ago?
* Is Inbox Zero anathema to you?
* Have you unsubscribed to many marketing campaigns over the years and lost track of all the emails you need to delete?

Well, have I got the Google Apps script for you.

Follow the directions and you'll get a spreadsheet that summarizes your inbox by domain and sender, counting:
* the number of emails
* the number of threads
* the size of all those emails (including attachments)

<img width="1019" height="269" alt="spreadsheet_sample" src="https://github.com/user-attachments/assets/b98e1ca3-eb0b-494b-851d-81cf3af0df4b" />

### Instructions

1. Open your target Google Sheet; paste this script
2. OPTIONAL: add the Sheet ID (in the URL) to the script
3. In the AppsScript menu > Services (left side) > add Advanced Gmail service (`Gmail`) 
4. Share the sheet with as many Google accounts as you want the report to run on.
5. In the Sheet: `Email Audit` → Start / Continue Audit (default query) for Account A. Do the same while logged in as Account B.
6. Watch `audit_progress` update; partial results appear in each account’s tab every few pages.
7. The script auto-continues via a time-driven trigger until status = DONE. You can Reset State from the menu if needed.

### Error handling
- Time limit: each run processes a bounded number of pages/time and then resumes from the saved pageToken via trigger.
- Empty response: handled by retries; if still bad, the message is skipped and logged so one bad item never stalls the audit.
