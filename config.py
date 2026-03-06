"""
config.py — Centralized configuration for the SharePoint/Excel/Splunk/Teams automation.

INSTRUCTIONS FOR REMOTE MACHINE:
  Set the TEAMS_* and SPLUNK_* values as environment variables, or fill them in below.
  Search for "# --- REPLACE ON REMOTE ---" comments to find every placeholder.
"""

import os

# ============================================================
# SharePoint settings
# ============================================================
SHAREPOINT_USERNAME  = os.environ.get("SP_USERNAME",  "USERNAME")          # --- REPLACE ON REMOTE ---
SHAREPOINT_PASSWORD  = os.environ.get("SP_PASSWORD",  "PASSWORD")          # --- REPLACE ON REMOTE ---
SHAREPOINT_URL       = os.environ.get("SP_URL",       "https://your-tenant.sharepoint.com")  # --- REPLACE ON REMOTE ---
ROOT_FOLDER          = os.environ.get("SP_ROOT",      "/sites/YourSite/Shared Documents")    # --- REPLACE ON REMOTE ---
EXCEL_SP_PATH        = "path/to/"                   # --- REPLACE ON REMOTE --- subfolder inside ROOT_FOLDER
FP_GUIDE_SP_PATH     = "path/to/fp_guide/"          # --- REPLACE ON REMOTE --- subfolder inside ROOT_FOLDER
EXCEL_FILENAME       = "Myojo FDI DEF request status survey.xlsx"

# Local simulation paths (used when USE_LOCAL=True)
LOCAL_EXCEL_PATH     = r"D:\Work\Windows_ENV\Myojo FDI DEF request status survey.xlsx"
LOCAL_SHAREPOINT_ROOT = r"D:\Work\Sharepoint_Excel_Team_automation\local_sharepoint"

# Toggle: True = use local file system (current machine); False = use remote SharePoint
USE_LOCAL_SHAREPOINT = True

# ============================================================
# Excel settings
# ============================================================
EXCEL_SHEET_NAME     = "20260226 FDI 1st DEF request"
EXCEL_HEADER_ROW     = 3    # Row containing column names (subsys, FE, BC, BE …)
EXCEL_DATA_START_ROW = 4    # First data row
# Columns (1-indexed)
COL_PUSHDOWN         = 1    # A
COL_SUBSYS           = 2    # B
COL_FE               = 3    # C
COL_BC               = 4    # D
COL_BE               = 5    # E
COL_PPT              = 6    # F
COL_NETLIST          = 7    # G
COL_SDC              = 8    # H
COL_CCF              = 9    # I
COL_UPF              = 10   # J
COL_RELEASE_DATE     = 11   # K
COL_REMARKS          = 12   # L

# Colors
COLOR_PREFILLED_BG   = "FF00B0F0"   # Blue  — B:E, read-only
COLOR_USERFILL_BG    = "FFFFC000"   # Yellow — F:J, user-filled
COLOR_BLACK          = "FF000000"   # Standard black font
COLOR_DEFAULT        = "00000000"   # Excel default (also treated as black)

# ============================================================
# Splunk settings
# ============================================================
SPLUNK_BASE_URL      = os.environ.get("SPLUNK_URL",      "https://splunk.xxx.inc:8089")   # --- REPLACE ON REMOTE ---
SPLUNK_USERNAME      = os.environ.get("SPLUNK_USER",     "splunk_user")                    # --- REPLACE ON REMOTE ---
SPLUNK_PASSWORD      = os.environ.get("SPLUNK_PASSWORD", "splunk_pw")                      # --- REPLACE ON REMOTE ---
SPLUNK_SEARCH_QUERY  = 'index="test_idx" source="/proj/A123456/*.csv" TOOL_NAME=REQ_DEF | table *'

# Toggle: True = return mock data; False = hit real Splunk
USE_MOCK_SPLUNK      = True

# ============================================================
# Microsoft Teams / Graph API settings
# All values are read from env vars — set them on your remote machine.
# ============================================================
TEAMS_AUTHORITY      = os.environ.get("TEAMS_AUTHORITY",    "https://login.microsoftonline.com/YOUR_TENANT_ID")  # --- REPLACE ON REMOTE ---
TEAMS_CLIENT_ID      = os.environ.get("TEAMS_CLIENT_ID",    "YOUR_CLIENT_ID")               # --- REPLACE ON REMOTE ---
TEAMS_CLIENT_SECRET  = os.environ.get("TEAMS_CLIENT_SECRET","YOUR_CLIENT_SECRET")            # --- REPLACE ON REMOTE ---
TEAMS_CLIENT_VALUE   = os.environ.get("TEAMS_CLIENT_VALUE", "YOUR_CLIENT_VALUE")             # --- REPLACE ON REMOTE ---
TEAMS_OBJECT_ID      = os.environ.get("TEAMS_OBJECT_ID",    "YOUR_OBJECT_ID")               # --- REPLACE ON REMOTE ---
TEAMS_USERNAME       = os.environ.get("TEAMS_USERNAME",     "your@email.com")               # --- REPLACE ON REMOTE ---
TEAMS_PASSWORD       = os.environ.get("TEAMS_PASSWORD",     "YOUR_TEAMS_PASSWORD")           # --- REPLACE ON REMOTE ---
TEAMS_ENDPOINT       = os.environ.get("TEAMS_ENDPOINT",     "https://graph.microsoft.com/v1.0")
TEAMS_CHAT_ID        = os.environ.get("TEAMS_CHAT_ID",      "YOUR_CHAT_OR_GROUP_ID")        # --- REPLACE ON REMOTE ---
# Scopes pre-consented in Azure app registration:
TEAMS_SCOPES         = ["https://graph.microsoft.com/.default"]

# Toggle: True = print messages to console instead of calling Graph API
USE_MOCK_TEAMS       = True

# ============================================================
# Scheduler settings
# ============================================================
# Time for the daily jobs (24-hour HH:MM, local time zone)
DAILY_SUMMARY_TIME   = os.environ.get("DAILY_SUMMARY_TIME",  "09:00")   # e.g. "09:00", "14:30"
REMINDER_LEAD_DAYS   = int(os.environ.get("REMINDER_LEAD_DAYS", "1"))    # notify N days before ETA deadline
TIMEZONE             = os.environ.get("TZ", "Asia/Taipei")               # APScheduler timezone

# ============================================================
# Project deadline & deliverable ownership
# ============================================================
# Single source-of-truth for the project deliverable deadline.
# Format: (YYYY, MM, DD) — change here to affect all modules.
from datetime import date as _date
PROJECT_DEADLINE: _date = _date(2026, 2, 26)

# Backward-compat alias used inside the overdue tracker
DEFAULT_ETA: _date = PROJECT_DEADLINE

# Which fields are owned by the Frontend Engineer (FE).
# All other tracked fields (netlist, sdc, ccf, upf) default to BE.
FE_OWNED_FIELDS: list = ["ppt", "ppt_status"]

# ============================================================
# SharePoint Excel web link (shown in Teams ETA-request messages)
# ============================================================
# Set EXCEL_WEB_LINK env-var on the remote machine to the full
# browser-accessible URL of the tracking Excel file.
# No path segments are concatenated here — all components are
# independently configurable via environment variables.
EXCEL_WEB_LINK = os.environ.get("EXCEL_WEB_LINK", "")  # --- REPLACE ON REMOTE ---
