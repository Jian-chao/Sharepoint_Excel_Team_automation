"""
modules/teams_notifier.py
==========================
Posts messages to a Microsoft Teams group chat using Microsoft Graph API.
Authentication via a custom MSALCredential that wraps
msal.ConfidentialClientApplication and exposes the azure.core
TokenCredential protocol so it can be passed directly to GraphServiceClient.

Two implementations:
  MockTeamsNotifier   — prints messages to stdout (no Azure required)
  RemoteTeamsNotifier — real Graph API via msgraph-sdk  (--- REMOTE ONLY ---)

All public methods on both implementations are **async** so callers can
``await`` them uniformly regardless of which notifier is active:

    await notifier.post_daily_summary(records, splunk_data)
    await notifier.send_eta_reminder(record, "netlist", "2026-03-05")
    await notifier.send_overdue_alert(record, "sdc")

The Remote notifier builds and sends chat messages using the fluent
msgraph request-builder API:

    result = await client.chats.by_chat_id(chat_id).messages.post(request_body)

and reads messages with an OData $filter via MessagesRequestBuilder /
RequestConfiguration from kiota_abstractions.

Required env vars for RemoteTeamsNotifier (set on remote machine):
    TEAMS_AUTHORITY      https://login.microsoftonline.com/<tenant_id>
    TEAMS_CLIENT_ID      Azure app registration client ID
    TEAMS_CLIENT_SECRET  Client secret value
    TEAMS_USERNAME       UPN of the delegated user (ROPC flow); leave blank
                         for app-only client-credentials flow
    TEAMS_PASSWORD       Password (ROPC flow only)
    TEAMS_SCOPES         Space-separated scopes, e.g. "https://graph.microsoft.com/.default"
    TEAMS_CHAT_ID        Target group-chat ID

Search "--- REMOTE ONLY ---" to find every placeholder.
"""

import asyncio
import logging
import time
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Optional
import certifi

# TYPE_CHECKING is False at runtime — this import is skipped on machines that
# only use MockTeamsNotifier (no azure-core installed).  Pylance / mypy read it
# during static analysis so they can resolve the AccessToken return type on
# MSALCredential.get_token() without a top-level ImportError.
if TYPE_CHECKING:
    from azure.core.credentials import AccessToken      # --- REMOTE ONLY ---

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
# Message formatting helpers (shared, synchronous)
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
<p><i>Legend: ✅ Done | ❌ Missing | ⏳ ETA | 🔴 Overdue | ➖ N/A | ❓ No data uploaded</i></p>
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


def _build_blank_eta_nudge_html(rec: SubsysRecord, field: str) -> str:
    """
    Message sent to the owner when their ETA / upload field is blank.
    Politely asks them to fill in an expected upload date.
    """
    owner      = rec.be or rec.fe or "team"
    field_name = field.replace("_status", "").upper()
    return (
        f"<p>📅 <b>ETA Request:</b> The <b>{field_name}</b> upload field for "
        f"subsys <b>{rec.subsys}</b> is currently blank.<br>"
        f"Hi <b>{owner}</b>, please reply with your expected upload date "
        f"so we can keep the tracker up to date. 🙏<br>"
        f"(FE: {rec.fe} | BC: {rec.bc})</p>"
    )


# Field display order in all 2-D tables
_FIELD_ORDER = ["ppt", "netlist", "sdc", "ccf", "upf"]

# CSS column widths per deliverable in the 2-D tables
_TH_STYLE  = "padding:5px 8px;border:1px solid #888;white-space:nowrap;"
_TD_STYLE  = "padding:4px 6px;border:1px solid #888;"
_HDR_STYLE = _TH_STYLE + "background:#333;color:#FFF;font-weight:bold;"


def _overdue_cell_color(days: int) -> tuple[str, str]:
    """
    Map days-overdue → (background_color, text_color) for the overdue table.
    Gradient runs light-pink → dark-maroon; text flips to white above day 3.
    """
    if days <= 1:  return "#FFCCCC", "#000000"
    if days <= 3:  return "#FF9999", "#000000"
    if days <= 7:  return "#FF4444", "#FFFFFF"
    if days <= 14: return "#CC0000", "#FFFFFF"
    return             "#800000", "#FFFFFF"


def _build_overdue_batch_html(owner: str, subsys_map: dict) -> str:
    """
    2-D overdue table:  rows=subsys, columns=deliverable fields.

    subsys_map : {subsys: {field: {"eta":str, "days_overdue":int, "delivered":bool}}}

    - Delivered field  → green cell (✅)
    - Overdue  field   → red gradient cell with ETA date (deeper red = more overdue)
    """
    seen = {f for fd in subsys_map.values() for f in fd}
    fields = [f for f in _FIELD_ORDER if f in seen]
    today_str = date.today().strftime("%Y-%m-%d")

    col_hdrs = "".join(
        f'<th style="{_HDR_STYLE}">{f.upper()}</th>' for f in fields
    )
    header = (
        f'<tr><td colspan="{len(fields)+1}" '
        f'style="{_TH_STYLE}background:#111;color:#FFF;font-size:1.05em;">'
        f'🔴 Overdue Deliverables — {today_str} &nbsp;|  Owner: <b>{owner}</b>'
        f'</td></tr>'
        f'<tr><th style="{_HDR_STYLE}">Subsys</th>{col_hdrs}</tr>'
    )

    rows = ""
    for subsys, fd in subsys_map.items():
        cells = "".join(
            (
                f'<td style="{_TD_STYLE}background:#22BB22;color:#FFF;'
                f'text-align:center;">✅</td>'
                if fd.get(f, {}).get("delivered")
                else (
                    "".join([
                        f'<td style="{_TD_STYLE}'
                        f'background:{_overdue_cell_color(fd[f]["days_overdue"])[0]};'
                        f'color:{_overdue_cell_color(fd[f]["days_overdue"])[1]};'
                        f'font-weight:bold;text-align:center;">'
                        f'{fd[f]["eta"]}</td>'
                    ]) if f in fd
                    else f'<td style="{_TD_STYLE}"></td>'
                )
            )
            for f in fields
        )
        rows += f'<tr><td style="{_TD_STYLE}font-weight:bold;">{subsys}</td>{cells}</tr>'

    guide = (
        "<p><i>Colour: "
        "<span style='background:#FFCCCC;padding:1px 4px;'>1d</span> "
        "<span style='background:#FF9999;padding:1px 4px;'>3d</span> "
        "<span style='background:#FF4444;color:#FFF;padding:1px 4px;'>7d</span> "
        "<span style='background:#CC0000;color:#FFF;padding:1px 4px;'>14d</span> "
        "<span style='background:#800000;color:#FFF;padding:1px 4px;'>14d+</span> "
        "overdue</i></p>"
    )
    return (
        f'<table cellpadding="0" cellspacing="0" '
        f'style="border-collapse:collapse;font-family:Arial,sans-serif;">'
        f'<thead>{header}</thead>'
        f'<tbody>{rows}</tbody>'
        f'</table>{guide}'
    )


def _build_blank_eta_batch_html(owner: str, subsys_map: dict) -> str:
    """
    2-D ETA-Required table:  rows=subsys, columns=deliverable fields.

    subsys_map : {subsys: {field: str}}
      field value = ""           → blank   → yellow cell (ETA missing)
      field value = "2026/3/2"   → has ETA → plain white cell  showing the date

    Fields missing from a subsys dict are skipped (field not applicable).
    """
    seen = {f for fd in subsys_map.values() for f in fd}
    fields = [f for f in _FIELD_ORDER if f in seen]
    today_str = date.today().strftime("%Y-%m-%d")

    col_hdrs = "".join(
        f'<th style="{_TH_STYLE}">{f.upper()}</th>' for f in fields
    )
    header = (
        f'<tr><td colspan="{len(fields)+1}" '
        f'style="{_TH_STYLE}font-weight:bold;">'
        f'Owner: <b>{owner}</b>'
        f'</td></tr>'
        f'<tr><th style="{_TH_STYLE}">ETA required</th>{col_hdrs}</tr>'
    )

    rows = ""
    for subsys, fd in subsys_map.items():
        cells = "".join(
            (
                f'<td style="{_TD_STYLE}background:#FFD700;"></td>'
                if fd.get(f, None) == ""
                else (
                    f'<td style="{_TD_STYLE}">{fd[f]}</td>'
                    if f in fd
                    else f'<td style="{_TD_STYLE}"></td>'
                )
            )
            for f in fields
        )
        rows += f'<tr><td style="{_TD_STYLE}font-weight:bold;">{subsys}</td>{cells}</tr>'

    return (
        f'<table cellpadding="0" cellspacing="0" '
        f'style="border-collapse:collapse;font-family:Arial,sans-serif;">'
        f'<thead>{header}</thead>'
        f'<tbody>{rows}</tbody>'
        f'</table>'
        f'<p><i>🟡 Yellow = ETA missing &nbsp; White = ETA already provided</i></p>'
    )


# ─────────────────────────────────────────────────────────────
# MOCK notifier  (all methods are async for interface parity)
# ─────────────────────────────────────────────────────────────

class MockTeamsNotifier:
    """
    Prints messages to stdout — no actual Graph API calls.

    All public methods are ``async def`` so callers can uniformly
    ``await`` them whether Mock or Remote is in use.
    """

    async def post_to_chat(self, html_body: str, chat_id: str = "MOCK"):
        print(f"\n{'='*60}")
        print(f"[MockTeams] → Chat: {chat_id}")
        print(html_body)
        print(f"{'='*60}\n")

    async def post_daily_summary(
        self, records: list[SubsysRecord], splunk_data: dict
    ):
        html = _build_summary_html(records, splunk_data)
        await self.post_to_chat(html)

    async def send_eta_reminder(self, rec: SubsysRecord, field: str, eta: str):
        html = _build_reminder_html(rec, field, eta)
        await self.post_to_chat(html)

    async def send_overdue_alert(self, rec: SubsysRecord, field: str):
        """Send a single overdue alert (kept for backward-compat; prefer send_overdue_batch)."""
        html = _build_overdue_html(rec, field)
        await self.post_to_chat(html)

    async def send_blank_eta_nudge(self, rec: SubsysRecord, field: str):
        """Nudge the owner to fill in their ETA (kept for backward-compat; prefer send_blank_eta_batch)."""
        html = _build_blank_eta_nudge_html(rec, field)
        await self.post_to_chat(html)

    async def send_overdue_batch(
        self,
        grouped: dict,
        chat_id: str = "MOCK",
    ):
        """
        Send one mock message per owner covering all their overdue items.
        grouped : {owner: [{"subsys", "field", "fe", "bc"}, ...]}
        """
        for owner, items in grouped.items():
            html = _build_overdue_batch_html(owner, items)
            await self.post_to_chat(html, chat_id)

    async def send_blank_eta_batch(
        self,
        grouped: dict,
        chat_id: str = "MOCK",
    ):
        """
        Send one mock message per owner listing all their blank ETA fields.
        grouped : {owner: [{"subsys", "field", "fe", "bc"}, ...]}
        """
        for owner, items in grouped.items():
            html = _build_blank_eta_batch_html(owner, items)
            await self.post_to_chat(html, chat_id)

    async def poll_chat_messages(
        self,
        chat_id: str = "MOCK",
        since: Optional[datetime] = None,
    ) -> list:
        logger.info("[MockTeams] poll_chat_messages — returning empty list (mock)")
        return []


# ─────────────────────────────────────────────────────────────
# MSAL credential adapter          --- REMOTE ONLY ---
# ─────────────────────────────────────────────────────────────

class MSALCredential:                                          # --- REMOTE ONLY ---
    """
    Implements the ``azure.core.credentials.TokenCredential`` protocol
    backed by an MSAL ``ConfidentialClientApplication``.

    Compatible with ``GraphServiceClient`` through
    ``kiota_authentication_azure.AzureIdentityAuthenticationProvider``.

    Token acquisition strategy
    --------------------------
    1. Try the MSAL in-memory token cache (silent refresh).
    2. If a ``TEAMS_USERNAME`` / ``TEAMS_PASSWORD`` are configured:
       use the **ROPC** (resource-owner password credentials) flow.
    3. Otherwise fall back to the **client-credentials** (app-only) flow.

    The returned ``AccessToken`` is the
    ``azure.core.credentials.AccessToken(token, expires_on)`` namedtuple
    that kiota_authentication_azure expects.
    """

    def __init__(self):
        try:
            import msal                                        # --- REMOTE ONLY ---
        except ImportError:
            raise ImportError("Install msal: pip install msal")

        session = requests.Session()
        session.verify = certifi.where()
        # import msal  # noqa: F811  (re-import after the try/except guard)
        self.app = msal.ConfidentialClientApplication(         # --- REMOTE ONLY ---
            client_id         = config.TEAMS_CLIENT_ID,
            client_credential = config.TEAMS_CLIENT_SECRET,
            authority         = config.TEAMS_AUTHORITY,
            http_session      = session,
        )

    def get_token(self, *scopes: str, **kwargs) -> "AccessToken":  # --- REMOTE ONLY ---
        """
        Acquire (or refresh) an access token.

        Parameters
        ----------
        *scopes : str
            One or more OAuth2 scope strings, e.g.
            ``"https://graph.microsoft.com/.default"``.

        Returns
        -------
        azure.core.credentials.AccessToken
            A ``(token, expires_on)`` namedtuple.

        Raises
        ------
        RuntimeError
            If token acquisition fails after all attempts.
        """
        import time

        scope_list = list(scopes) or config.TEAMS_SCOPES

        # 1 — Try the MSAL in-memory cache first (avoids unnecessary round-trips)
        result = None
        accounts = self.app.get_accounts()
        if accounts:
            result = self.app.acquire_token_silent(
                scopes  = scope_list,
                account = accounts[0],
            )

        # 2 — Delegated (ROPC) flow when username/password are configured
        if not result or "access_token" not in result:
            username = getattr(config, "TEAMS_USERNAME", "")
            password = getattr(config, "TEAMS_PASSWORD", "")
            if username and password:
                result = self.app.acquire_token_by_username_password(
                    username = username,
                    password = password,
                    scopes   = scope_list,
                )
            else:
                # 3 — App-only client-credentials flow
                result = self.app.acquire_token_for_client(scopes=scope_list)

        if not result or "access_token" not in result:
            err = (result or {}).get(
                "error_description",
                (result or {}).get("error", "Unknown MSAL error"),
            )
            raise RuntimeError(f"[MSALCredential] Token acquisition failed: {err}")

        expires_on = int(time.time()) + result.get("expires_in", 3600)
        logger.debug("[MSALCredential] Token acquired/refreshed.")
        return AccessToken(result["access_token"], expires_on)


# ─────────────────────────────────────────────────────────────
# REMOTE notifier  (msgraph SDK + kiota)   --- REMOTE ONLY ---
# ─────────────────────────────────────────────────────────────

class RemoteTeamsNotifier:                                     # --- REMOTE ONLY ---
    """
    Posts and reads Teams chat messages via the Microsoft Graph SDK
    (``msgraph-sdk``).

    Authentication
    --------------
    Uses :class:`MSALCredential` (above) which is passed to
    ``kiota_authentication_azure.AzureIdentityAuthenticationProvider``
    and then to ``GraphServiceClient``.

    Message construction
    --------------------
    Messages are built with the msgraph model classes::

        from msgraph.generated.models.chat_message import ChatMessage
        from msgraph.generated.models.item_body    import ItemBody
        from msgraph.generated.models.body_type    import BodyType

    and sent through the fluent request-builder API::

        result = await client.chats.by_chat_id(chat_id).messages.post(request_body)

    Message listing uses ``MessagesRequestBuilder`` query parameters wrapped
    in a ``kiota_abstractions.base_request_configuration.RequestConfiguration``::

        from msgraph.generated.chats.item.messages.messages_request_builder import (
            MessagesRequestBuilder,
        )
        from kiota_abstractions.base_request_configuration import RequestConfiguration

    Dependencies (install on remote machine)
    -----------------------------------------
        pip install msgraph-sdk kiota-authentication-azure azure-core msal
    """

    def __init__(self):                                        # --- REMOTE ONLY ---
        try:
            from msgraph import GraphServiceClient             # --- REMOTE ONLY ---
            # from kiota_authentication_azure.azure_identity_authentication_provider import (
            #     AzureIdentityAuthenticationProvider,
            # )
        except ImportError:
            raise ImportError(
                "Install msgraph dependencies:\n"
                "  pip install msgraph-sdk kiota-authentication-azure azure-core"
            )

        credential   = MSALCredential()                        # --- REMOTE ONLY ---
        # auth_provider = AzureIdentityAuthenticationProvider(
        #     credentials = credential,
        #     scopes      = config.TEAMS_SCOPES,
        # )
        self._client       = GraphServiceClient(               # --- REMOTE ONLY ---
            credentials = credential,
            scopes      = config.TEAMS_SCOPES,
        )
        self._default_chat   = config.TEAMS_CHAT_ID

        # Rate-limiter state — Graph API enforces 10 messages / 10 seconds.
        # Sleeping at least _min_interval seconds between posts keeps us safely
        # under that threshold without any retry logic.
        self._last_post_time: float = 0.0
        self._min_interval:   float = 1.1   # seconds  → max ≈ 9 posts / 10s

    # ----------------------------------------------------------
    async def post_to_chat(                                    # --- REMOTE ONLY ---
        self,
        html_body: str,
        chat_id:   Optional[str] = None,
    ):
        """
        POST an HTML-formatted message to a Teams group chat.

        Uses the fluent msgraph request-builder pattern::

            result = await client.chats.by_chat_id(chat_id).messages.post(body)

        Graph API reference:
        https://learn.microsoft.com/en-us/graph/api/chat-post-messages

        Parameters
        ----------
        html_body : str
            HTML content to send.
        chat_id : str, optional
            Target chat ID; defaults to ``config.TEAMS_CHAT_ID``.

        Returns
        -------
        msgraph.generated.models.chat_message.ChatMessage
            The created message object returned by the Graph API.
        """
        from msgraph.generated.models.chat_message import ChatMessage
        from msgraph.generated.models.item_body    import ItemBody
        from msgraph.generated.models.body_type    import BodyType

        chat_id = chat_id or self._default_chat

        # --- Rate limiter ---------------------------------------------------
        # Enforce >= _min_interval seconds between consecutive Graph API posts
        # to stay below the 10-per-10-seconds quota.
        elapsed = time.monotonic() - self._last_post_time
        if elapsed < self._min_interval:
            wait = self._min_interval - elapsed
            logger.debug(f"[Teams] Rate-limiting: sleeping {wait:.2f}s before post.")
            await asyncio.sleep(wait)
        self._last_post_time = time.monotonic()
        # --------------------------------------------------------------------

        # Build request body using msgraph model classes
        request_body                   = ChatMessage()
        request_body.body              = ItemBody()
        request_body.body.content_type = BodyType.Html
        request_body.body.content      = html_body

        result = await self._client.chats.by_chat_id(chat_id).messages.post(
            request_body
        )
        logger.info(f"[Teams] Message posted to chat {chat_id}.")
        return result

    # ----------------------------------------------------------
    async def post_daily_summary(                              # --- REMOTE ONLY ---
        self,
        records:     list[SubsysRecord],
        splunk_data: dict,
        chat_id:     Optional[str] = None,
    ):
        html = _build_summary_html(records, splunk_data)
        return await self.post_to_chat(html, chat_id)

    async def send_eta_reminder(                               # --- REMOTE ONLY ---
        self,
        rec:     SubsysRecord,
        field:   str,
        eta:     str,
        chat_id: Optional[str] = None,
    ):
        html = _build_reminder_html(rec, field, eta)
        return await self.post_to_chat(html, chat_id)

    async def send_overdue_alert(                              # --- REMOTE ONLY ---
        self,
        rec:     SubsysRecord,
        field:   str,
        chat_id: Optional[str] = None,
    ):
        """Single overdue alert (kept for backward-compat; prefer send_overdue_batch)."""
        html = _build_overdue_html(rec, field)
        return await self.post_to_chat(html, chat_id)

    async def send_blank_eta_nudge(                            # --- REMOTE ONLY ---
        self,
        rec:     SubsysRecord,
        field:   str,
        chat_id: Optional[str] = None,
    ):
        """Single blank-ETA nudge (kept for backward-compat; prefer send_blank_eta_batch)."""
        html = _build_blank_eta_nudge_html(rec, field)
        return await self.post_to_chat(html, chat_id)

    async def send_overdue_batch(                              # --- REMOTE ONLY ---
        self,
        grouped:  dict,
        chat_id:  Optional[str] = None,
    ):
        """
        Send **one** Teams message per owner with a table of all their overdue items.
        The built-in rate-limiter in :meth:`post_to_chat` ensures we stay
        below the 10/10s Graph API quota automatically.

        grouped : {owner_name: [{"subsys", "field", "fe", "bc"}, ...]}
        """
        for owner, items in grouped.items():
            html = _build_overdue_batch_html(owner, items)
            await self.post_to_chat(html, chat_id)
            logger.info(
                f"[Teams] Overdue batch → {owner!r} ({len(items)} item(s))."
            )

    async def send_blank_eta_batch(                            # --- REMOTE ONLY ---
        self,
        grouped:  dict,
        chat_id:  Optional[str] = None,
    ):
        """
        Send **one** Teams message per owner listing all their blank ETA fields.
        The built-in rate-limiter in :meth:`post_to_chat` ensures we stay
        below the 10/10s Graph API quota automatically.

        grouped : {owner_name: [{"subsys", "field", "fe", "bc"}, ...]}
        """
        for owner, items in grouped.items():
            html = _build_blank_eta_batch_html(owner, items)
            await self.post_to_chat(html, chat_id)
            logger.info(
                f"[Teams] Blank-ETA batch → {owner!r} ({len(items)} field(s))."
            )

    # ----------------------------------------------------------
    async def poll_chat_messages(                              # --- REMOTE ONLY ---
        self,
        chat_id: Optional[str]       = None,
        since:   Optional[datetime]  = None,
    ) -> list:
        """
        Read recent messages from the group chat.

        Uses ``MessagesRequestBuilder`` query parameters and
        ``kiota_abstractions.base_request_configuration.RequestConfiguration``
        to apply an OData ``$filter`` when *since* is given::

            from msgraph.generated.chats.item.messages.messages_request_builder import (
                MessagesRequestBuilder,
            )
            from kiota_abstractions.base_request_configuration import RequestConfiguration

            query_params = MessagesRequestBuilder.MessagesRequestBuilderGetQueryParameters(
                filter="createdDateTime ge 2026-03-01T00:00:00Z",
                top=50,
            )
            config = RequestConfiguration(query_parameters=query_params)
            result = await client.chats.by_chat_id(chat_id).messages.get(
                request_configuration=config
            )

        Parameters
        ----------
        chat_id : str, optional
            Source chat ID; defaults to ``config.TEAMS_CHAT_ID``.
        since : datetime, optional
            If given, only messages created at or after this UTC time are
            returned (OData ``$filter`` on ``createdDateTime``).

        Returns
        -------
        list[msgraph.generated.models.chat_message.ChatMessage]
        """
        from msgraph.generated.chats.item.messages.messages_request_builder import (
            MessagesRequestBuilder,
        )
        from kiota_abstractions.base_request_configuration import RequestConfiguration

        chat_id = chat_id or self._default_chat

        if since:
            since_str    = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            query_params = MessagesRequestBuilder.MessagesRequestBuilderGetQueryParameters(
                filter = f"createdDateTime ge {since_str}",
                top    = 50,
            )
            request_config = RequestConfiguration(query_parameters=query_params)
            result = await self._client.chats.by_chat_id(chat_id).messages.get(
                request_configuration=request_config
            )
        else:
            result = await self._client.chats.by_chat_id(chat_id).messages.get()

        messages = result.value if result and result.value else []
        logger.info(f"[Teams] Polled {len(messages)} message(s) from chat {chat_id}.")
        return messages

    # ----------------------------------------------------------
    async def get_user_display_name(self, upn: str) -> str:   # --- REMOTE ONLY ---
        """Resolve a user's display name from their UPN via Graph API."""
        try:
            result = await self._client.users.by_user_id(upn).get()
            return result.display_name or upn
        except Exception as exc:
            logger.warning(
                f"[Teams] Could not resolve display name for {upn!r}: {exc}"
            )
            return upn
