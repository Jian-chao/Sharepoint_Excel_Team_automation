"""
modules/teams_notifier.py
==========================
Posts messages to a Microsoft Teams group chat using Microsoft Graph API.
Authentication via MSAL ConfidentialClientApplication (ROPC flow).

Two implementations:
  MockTeamsNotifier   — prints messages to stdout (current machine, no Azure)
  RemoteTeamsNotifier — real Graph API calls (activate on remote machine)

All Teams credentials are read from environment variables.  See config.py for
the full list.  Search "--- REMOTE ONLY ---" to find every placeholder.

Usage:
    from modules.teams_notifier import get_notifier
    notifier = get_notifier()
    notifier.post_daily_summary(records, splunk_df)
    notifier.send_eta_reminder(record)
    notifier.send_overdue_alert(record)
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import config
from modules.excel_reader import SubsysRecord

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────

def get_notifier():
    if config.USE_MOCK_TEAMS:
        return MockTeamsNotifier()
    return RemoteTeamsNotifier()                               # --- REMOTE ONLY ---


# ─────────────────────────────────────────────────────────────
# Message formatting helpers (shared)
# ─────────────────────────────────────────────────────────────

def _status_icon(val: str, splunk_val: Optional[bool] = None) -> str:
    """
    Produce a human-readable status string.
    Excel value: 'v' uploaded, 'x' not needed, '' not uploaded
    Splunk value: True uploaded, False not, None no data
    """
    if val and val.lower() == "x":
        return "➖ N/A"
    if val and val.lower() == "v":
        return "✅ Done"
    if splunk_val is True:
        return "✅ Perforce"
    if splunk_val is False:
        return "❌ Missing"
    if splunk_val is None:
        return "❓ No Data"
    return f"⏳ {val}" if val else "❌ Missing"


def _ppt_icon(val: str, deadline: date) -> str:
    if not val:
        if date.today() > deadline:
            return "🔴 OVERDUE"
        return "❌ Missing"
    if val.lower() == "done":
        return "✅ Done"
    if val.lower() == "n/a":
        return "➖ N/A"
    # Could be an ETA string
    try:
        eta = datetime.strptime(val.replace("eta:", "").strip(), "%Y-%m-%d").date()
        days_left = (eta - date.today()).days
        if days_left < 0:
            return f"🔴 OVERDUE (eta:{eta})"
        if days_left <= config.REMINDER_LEAD_DAYS:
            return f"⚠️ DUE SOON ({eta})"
        return f"⏳ ETA {eta}"
    except ValueError:
        return f"⏳ {val}"


def _build_summary_html(records: list[SubsysRecord], splunk_data: dict) -> str:
    """
    Build an HTML table for posting to Teams via Graph API.
    splunk_data: {subsys_name: {"netlist": bool|None, "sdc": bool|None, ...}}
    """
    # Parse deadline from G1 cell value (stored as a string like "BE upload ... before 2/26 11:59 HQ")
    deadline = date(2026, 2, 26)   # fallback — update from config or dynamic parse

    rows_html = ""
    for rec in records:
        sp = splunk_data.get(rec.subsys, {})
        rows_html += (
            f"<tr>"
            f"<td><b>{rec.subsys}</b></td>"
            f"<td>{rec.fe}</td>"
            f"<td>{rec.bc}</td>"
            f"<td>{rec.be}</td>"
            f"<td>{_ppt_icon(rec.ppt_status, deadline)}</td>"
            f"<td>{_status_icon(rec.netlist, sp.get('netlist'))}</td>"
            f"<td>{_status_icon(rec.sdc,     sp.get('sdc'))}</td>"
            f"<td>{_status_icon(rec.ccf,     sp.get('ccf'))}</td>"
            f"<td>{_status_icon(rec.upf,     sp.get('upf'))}</td>"
            f"</tr>"
        )

    today_str = date.today().strftime("%Y-%m-%d")
    html = f"""
<h3>📋 FDI DEF Request — Daily Status Report ({today_str})</h3>
<table border="1" cellpadding="4" cellspacing="0">
  <thead>
    <tr>
      <th>Subsys</th><th>FE</th><th>BC</th><th>BE</th>
      <th>PPT</th><th>Netlist</th><th>SDC</th><th>CCF</th><th>UPF</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>
<p><i>Legend: ✅ Done | ❌ Missing | ⏳ ETA | 🔴 Overdue | ➖ N/A | ❓ No Splunk data</i></p>
"""
    return html.strip()


def _build_reminder_html(rec: SubsysRecord, field: str, eta: str) -> str:
    return (
        f"<p>⚠️ <b>Reminder:</b> The <b>{field}</b> deliverable for subsys "
        f"<b>{rec.subsys}</b> is due <b>tomorrow ({eta})</b>.<br>"
        f"Owner: {rec.be} / FE: {rec.fe} — please ensure upload is complete.</p>"
    )


def _build_overdue_html(rec: SubsysRecord, field: str) -> str:
    return (
        f"<p>🔴 <b>OVERDUE:</b> <b>{field}</b> for subsys <b>{rec.subsys}</b> "
        f"has passed its deadline!<br>"
        f"Owner: {rec.be} / FE: {rec.fe} / BC: {rec.bc}<br>"
        f"Please provide an updated ETA immediately.</p>"
    )


# ─────────────────────────────────────────────────────────────
# MOCK notifier
# ─────────────────────────────────────────────────────────────

class MockTeamsNotifier:
    """Prints messages to stdout — no actual Graph API calls."""

    def post_to_chat(self, html_body: str, chat_id: str = "MOCK"):
        print(f"\n{'='*60}")
        print(f"[MockTeams] → Chat: {chat_id}")
        print(html_body)
        print(f"{'='*60}\n")

    def post_daily_summary(self, records: list[SubsysRecord], splunk_data: dict):
        html = _build_summary_html(records, splunk_data)
        self.post_to_chat(html)

    def send_eta_reminder(self, rec: SubsysRecord, field: str, eta: str):
        html = _build_reminder_html(rec, field, eta)
        self.post_to_chat(html)

    def send_overdue_alert(self, rec: SubsysRecord, field: str):
        html = _build_overdue_html(rec, field)
        self.post_to_chat(html)

    def poll_chat_messages(self, chat_id: str, since: Optional[datetime] = None) -> list[dict]:
        logger.info("[MockTeams] poll_chat_messages — returning empty list (mock)")
        return []


# ─────────────────────────────────────────────────────────────
# REMOTE notifier  (Graph API via MSAL)  --- REMOTE ONLY ---
# ─────────────────────────────────────────────────────────────

class RemoteTeamsNotifier:                                     # --- REMOTE ONLY ---
    """
    Authenticates with Microsoft Graph API using MSAL ConfidentialClientApplication
    (ROPC flow: username + password with client credentials).

    Required env vars (set on remote machine):
        TEAMS_AUTHORITY      https://login.microsoftonline.com/<tenant_id>
        TEAMS_CLIENT_ID      Azure app client ID
        TEAMS_CLIENT_SECRET  Azure app client secret
        TEAMS_CLIENT_VALUE   (same as secret, or a separate value field)
        TEAMS_OBJECT_ID      Azure app object ID
        TEAMS_USERNAME       UPN of the user account (e.g. you@company.com)
        TEAMS_PASSWORD       Password for that user account
        TEAMS_ENDPOINT       https://graph.microsoft.com/v1.0
        TEAMS_CHAT_ID        ID of the target group chat
    """

    def __init__(self):
        try:
            import msal                                        # --- REMOTE ONLY ---
        except ImportError:
            raise ImportError("Install msal: pip install msal")

        import msal
        self._msal_app = msal.ConfidentialClientApplication(  # --- REMOTE ONLY ---
            client_id         = config.TEAMS_CLIENT_ID,
            client_credential = config.TEAMS_CLIENT_SECRET,
            authority         = config.TEAMS_AUTHORITY,
        )
        self._token: Optional[str] = None
        self._endpoint = config.TEAMS_ENDPOINT.rstrip("/")
        self._default_chat = config.TEAMS_CHAT_ID

    def _get_token(self) -> str:                              # --- REMOTE ONLY ---
        """Acquire (or refresh) the Graph API access token using ROPC flow."""
        # Try cache first
        accounts = self._msal_app.get_accounts(username=config.TEAMS_USERNAME)
        if accounts:
            result = self._msal_app.acquire_token_silent(
                scopes=config.TEAMS_SCOPES, account=accounts[0]
            )
            if result and "access_token" in result:
                return result["access_token"]

        # ROPC flow (username + password)
        result = self._msal_app.acquire_token_by_username_password(
            username = config.TEAMS_USERNAME,
            password = config.TEAMS_PASSWORD,
            scopes   = config.TEAMS_SCOPES,
        )
        if "access_token" not in result:
            err = result.get("error_description", result.get("error", "Unknown"))
            raise RuntimeError(f"[Teams] MSAL token acquisition failed: {err}")

        logger.info("[Teams] Acquired new access token via ROPC.")
        return result["access_token"]

    def _headers(self) -> dict:                               # --- REMOTE ONLY ---
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type":  "application/json",
        }

    def post_to_chat(                                         # --- REMOTE ONLY ---
        self,
        html_body: str,
        chat_id: Optional[str] = None,
    ) -> dict:
        """
        POST an HTML message to a Teams group chat.
        Graph API: POST /v1.0/chats/{chat-id}/messages
        Docs: https://learn.microsoft.com/en-us/graph/api/chat-post-messages
        """
        import requests
        chat_id = chat_id or self._default_chat
        url     = f"{self._endpoint}/chats/{chat_id}/messages"
        payload = {
            "body": {
                "contentType": "html",
                "content":     html_body,
            }
        }
        resp = requests.post(url, headers=self._headers(), json=payload)
        resp.raise_for_status()
        logger.info(f"[Teams] Message posted to chat {chat_id}.")
        return resp.json()

    def post_daily_summary(                                   # --- REMOTE ONLY ---
        self,
        records: list[SubsysRecord],
        splunk_data: dict,
        chat_id: Optional[str] = None,
    ):
        html = _build_summary_html(records, splunk_data)
        return self.post_to_chat(html, chat_id)

    def send_eta_reminder(                                    # --- REMOTE ONLY ---
        self,
        rec: SubsysRecord,
        field: str,
        eta: str,
        chat_id: Optional[str] = None,
    ):
        html = _build_reminder_html(rec, field, eta)
        return self.post_to_chat(html, chat_id)

    def send_overdue_alert(                                   # --- REMOTE ONLY ---
        self,
        rec: SubsysRecord,
        field: str,
        chat_id: Optional[str] = None,
    ):
        html = _build_overdue_html(rec, field)
        return self.post_to_chat(html, chat_id)

    def poll_chat_messages(                                   # --- REMOTE ONLY ---
        self,
        chat_id: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> list[dict]:
        """
        Poll recent messages from the group chat.
        Useful for reading ETA replies from owners.
        Graph API: GET /v1.0/chats/{chat-id}/messages
        """
        import requests
        chat_id = chat_id or self._default_chat
        url     = f"{self._endpoint}/chats/{chat_id}/messages"
        params  = {}
        if since:
            params["$filter"] = f"createdDateTime ge {since.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        resp = requests.get(url, headers=self._headers(), params=params)
        resp.raise_for_status()
        messages = resp.json().get("value", [])
        logger.info(f"[Teams] Polled {len(messages)} message(s) from chat {chat_id}.")
        return messages

    def get_user_display_name(self, upn: str) -> str:        # --- REMOTE ONLY ---
        """Resolve a user's display name from their UPN via Graph API."""
        import requests
        url  = f"{self._endpoint}/users/{upn}"
        resp = requests.get(url, headers=self._headers())
        if resp.ok:
            return resp.json().get("displayName", upn)
        return upn
