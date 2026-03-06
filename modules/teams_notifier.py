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

# ─────────────────────────────────────────────────────────────────────────────
# Low-level cell helpers  (Teams-compatible, no Emoji)
# ─────────────────────────────────────────────────────────────────────────────

def _format_month_day(d: date) -> str:
    """Format a date as 'Mar/4' (cross-platform, no leading zero)."""
    return f"{d.strftime('%b')}/{d.day}"


def _cell(bg: str, fg: str, content: str) -> str:
    """Build a <td> with optional Teams-supported background/foreground colour."""
    style = ""
    if bg: style += f"background-color:{bg};"
    if fg: style += f"color:{fg};"
    return f'<td style="{style}">{content}</td>'


def _upload_cell_attrs(val: str, splunk_val: Optional[bool] = None) -> tuple:
    """
    Return (bg, fg, text) for a Perforce upload field (netlist/sdc/ccf/upf).

    Teams-confirmed colour palette:
      Done    → green  #22BB22 / #FFFFFF
      Missing → yellow #FDD472 / #B6424C
      ETA     → amber text on inherit bg
      N/A     → plain
    """
    if val and val.lower() == "x":
        return "", "", "N/A"
    if (val and val.lower() == "v") or splunk_val is True:
        return "#22BB22", "#FFFFFF", "&#10003; Done"
    if splunk_val is False:
        return "#FDD472", "#B6424C", "&#10007; Missing"
    if splunk_val is None and not val:
        return "", "", "?"
    if val:
        return "", "#FDC030", val   # ETA text written by DE
    return "#FDD472", "#B6424C", "&#10007; Missing"


def _ppt_cell_attrs(
    val: str,
    deadline: date,
    eta_date: Optional[date] = None,
) -> tuple:
    """
    Return (bg, fg, text) for the PPT / ETA column.

    Parameters
    ----------
    val      : raw Excel cell value
    deadline : project deadline (from config.PROJECT_DEADLINE)
    eta_date : LLM-parsed date from ETAResult.parsed (may be None)
    """
    if not val:
        if date.today() > deadline:
            return "#DF9299", "#B6424C", "<b>[OVERDUE]</b>"
        return "#FDD472", "#B6424C", "&#10007; Missing"

    lo = val.lower().strip()
    if lo in ("done", "v"):
        return "#22BB22", "#FFFFFF", "&#10003; Done"
    if lo in ("n/a", "x"):
        return "", "", "N/A"

    # If LLM gave us a parsed date, use it; otherwise try a quick ISO parse.
    d = eta_date
    if d is None:
        try:
            d = datetime.strptime(
                val.replace("eta:", "").strip(), "%Y-%m-%d"
            ).date()
        except ValueError:
            pass

    if d is not None:
        days_left = (d - date.today()).days
        label = _format_month_day(d)
        if days_left < 0:
            return "#DF9299", "#B6424C", f"<b>[OD] {label}</b>"
        if days_left <= config.REMINDER_LEAD_DAYS:
            return "#FDD472", "#B6424C", label
        return "", "#FDC030", label

    # Unparsed raw value — amber text, inherit bg
    return "", "#FDC030", val


# Legacy wrappers kept for backward-compat (tests, etc.)
def _status_icon(val: str, splunk_val: Optional[bool] = None) -> str:
    """Return text status for an upload field. Use _upload_cell_attrs for styled tables."""
    _, _, text = _upload_cell_attrs(val, splunk_val)
    return text


def _ppt_icon(val: str, deadline: date, eta_date: Optional[date] = None) -> str:
    """Return text status for the PPT field. Use _ppt_cell_attrs for styled tables."""
    _, _, text = _ppt_cell_attrs(val, deadline, eta_date)
    return text


# ─────────────────────────────────────────────────────────────────────────────
# HTML message builders
# ─────────────────────────────────────────────────────────────────────────────

def _build_summary_html(
    records: list,
    splunk_data: dict,
    eta_results_map: Optional[dict] = None,
) -> str:
    """
    Build a Teams-compatible HTML summary table.

    Parameters
    ----------
    records         : list[SubsysRecord]
    splunk_data     : {subsys: {"netlist": bool|None, ...}}
    eta_results_map : {subsys: ETAResult} pre-computed by caller, or None.
                      Used to display the LLM-parsed PPT date as 'Mar/4'.

    Deadline is read from ``config.PROJECT_DEADLINE`` (configurable).
    No Emoji — uses Teams-confirmed colour palette & HTML entities.
    """
    deadline  = getattr(config, "PROJECT_DEADLINE", date(2026, 2, 26))
    eta_map   = eta_results_map or {}
    today_str = date.today().strftime("%Y-%m-%d")

    rows_html = ""
    for rec in records:
        sp       = splunk_data.get(rec.subsys, {})
        eta_res  = eta_map.get(rec.subsys)
        eta_date = (
            eta_res.parsed
            if eta_res and not isinstance(eta_res, BaseException)
            else None
        )

        ppt_bg, ppt_fg, ppt_txt = _ppt_cell_attrs(rec.ppt_status, deadline, eta_date)
        nl_bg,  nl_fg,  nl_txt  = _upload_cell_attrs(rec.netlist, sp.get("netlist"))
        sd_bg,  sd_fg,  sd_txt  = _upload_cell_attrs(rec.sdc,     sp.get("sdc"))
        cf_bg,  cf_fg,  cf_txt  = _upload_cell_attrs(rec.ccf,     sp.get("ccf"))
        uf_bg,  uf_fg,  uf_txt  = _upload_cell_attrs(rec.upf,     sp.get("upf"))

        rows_html += (
            "<tr>"
            + _cell("", "", f"<b>{rec.subsys}</b>")
            + _cell("", "", rec.fe)
            + _cell("", "", rec.bc)
            + _cell("", "", rec.be)
            + _cell(ppt_bg, ppt_fg, ppt_txt)
            + _cell(nl_bg,  nl_fg,  nl_txt)
            + _cell(sd_bg,  sd_fg,  sd_txt)
            + _cell(cf_bg,  cf_fg,  cf_txt)
            + _cell(uf_bg,  uf_fg,  uf_txt)
            + "</tr>"
        )

    legend = (
        '<span style="background-color:#22BB22;color:#FFFFFF">&#10003;</span> Done &nbsp; '
        '<span style="color:#B6424C">&#10007;</span> Missing &nbsp; '
        '<span style="color:#FDC030">Mar/D</span> = ETA &nbsp; '
        '<span style="background-color:#DF9299;color:#B6424C">[OD]</span> Overdue &nbsp; '
        "N/A = Not applicable"
    )
    return (
        f"<p><b>FDI DEF Request &#8212; Daily Status ({today_str})</b></p>"
        f'<table border="1">'
        f"<thead><tr>"
        f"<th>Subsys</th><th>FE</th><th>BC</th><th>BE</th>"
        f"<th>PPT</th><th>Netlist</th><th>SDC</th><th>CCF</th><th>UPF</th>"
        f"</tr></thead>"
        f"<tbody>{rows_html}</tbody>"
        f"</table>"
        f"<p><i>{legend}</i></p>"
    )


def _build_reminder_html(rec: SubsysRecord, field: str, eta: str) -> str:
    return (
        f"<p><b>Reminder:</b> The <b>{field}</b> deliverable for subsystem "
        f"<b>{rec.subsys}</b> is due <b>tomorrow ({eta})</b>.<br>"
        f"Owner: {rec.be} / FE: {rec.fe} &#8212; please ensure upload is complete.</p>"
    )


def _build_overdue_html(rec: SubsysRecord, field: str) -> str:
    return (
        f"<p><b>OVERDUE:</b> <b>{field}</b> for subsys <b>{rec.subsys}</b> "
        f"has passed its deadline!<br>"
        f"Owner: {rec.be} / FE: {rec.fe} / BC: {rec.bc}<br>"
        f"Please provide an updated ETA immediately.</p>"
    )


def _build_blank_eta_nudge_html(rec: SubsysRecord, field: str) -> str:
    """Single-field nudge kept for backward-compat. Prefer _gen_blank_eta_message."""
    owner      = rec.be or rec.fe or "team"
    field_name = field.replace("_status", "").upper()
    excel_link = getattr(config, "EXCEL_WEB_LINK", "")
    link_html  = (
        f' &#8212; <a href="{excel_link}">update here</a>' if excel_link else ""
    )
    return (
        f"<p><b>ETA Request:</b> The <b>{field_name}</b> field for "
        f"<b>{rec.subsys}</b> is currently blank.<br>"
        f"Hi <b>{owner}</b>, please provide your expected upload date{link_html}.<br>"
        f"(FE: {rec.fe} | BC: {rec.bc})<br>"
        f"<i>(Automated &#8212; do not reply here, update the Excel file directly.)</i></p>"
    )



# Field display order in all 2-D tables
_FIELD_ORDER = ["ppt", "netlist", "sdc", "ccf", "upf"]

# ── Teams-safe inline styles ─────────────────────────────────────────────────
# Teams strips: padding, border (CSS), font-weight (CSS), font-size, margin.
# Teams keeps:  background-color, color, text-align on <td>/<th>.
# Use border="1" on <table> and <b>/<strong> tags for bold.
_TH_STYLE  = "background-color:#333;color:#FFFFFF;"   # dark header cell
_TD_STYLE  = ""                                        # plain cell (no stripped props)
_HDR_STYLE = _TH_STYLE                                # alias kept for readability


def _overdue_cell_color(days: int) -> tuple[str, str, bool]:
    """
    Map days-overdue → (background_color, text_color, bold) using the
    Microsoft Teams-confirmed colour palette observed in actual rendered messages.

    Severity scale:
      ≤ 1d  : no highlight         (inherit bg / black text)
      ≤ 3d  : amber text warning   (inherit bg / #FDC030 amber text)
      ≤ 7d  : teal bg / rust text  (#90D9DB / #CD5937)
      ≤ 14d : yellow bg / crimson  (#FDD472 / #B6424C)
      14d+  : rose bg  / crimson   (#DF9299 / #B6424C) + bold
    """
    if days <= 1:  return "",        "",        False
    if days <= 3:  return "",        "#FDC030", False
    if days <= 7:  return "#90D9DB", "#CD5937", False
    if days <= 14: return "#FDD472", "#B6424C", False
    return                "#DF9299", "#B6424C", True


def _build_overdue_batch_html(owner: str, subsys_map: dict) -> str:
    """
    2-D overdue table:  rows=subsys, columns=deliverable fields.

    subsys_map : {subsys: {field: {"eta":str, "days_overdue":int, "delivered":bool}}}

    - Delivered field -> green cell  (&#10003;)
    - Overdue  field  -> colour-coded cell per Teams-confirmed palette
    - Missing  field  -> empty cell

    Returns empty string "" when every field in every subsystem is delivered,
    so callers can skip posting entirely.
    """
    # Skip post entirely if nothing is overdue
    if all(
        info.get("delivered")
        for fd in subsys_map.values()
        for info in fd.values()
    ):
        return ""

    seen   = {f for fd in subsys_map.values() for f in fd}
    fields = [f for f in _FIELD_ORDER if f in seen]
    today_str = date.today().strftime("%Y-%m-%d")

    # ── header row ───────────────────────────────────────────────
    col_hdrs = "".join(
        f'<th style="{_TH_STYLE}"><b>{f.upper()}</b></th>' for f in fields
    )
    header = (
        f'<tr>'
        f'<td colspan="{len(fields)+1}" style="background-color:#222222;color:#FFFFFF;">'
        f'&#128308; Overdue Deliverables &#8212; {today_str} &nbsp;| '
        f'Owner: <b>{owner}</b>'
        f'</td></tr>'
        f'<tr><th style="{_TH_STYLE}"><b>Subsys</b></th>{col_hdrs}</tr>'
    )

    # ── data rows ────────────────────────────────────────────────
    rows = ""
    for subsys, fd in subsys_map.items():
        cells = ""
        for f in fields:
            if f not in fd:
                cells += "<td></td>"
            elif fd[f].get("delivered"):
                cells += '<td style="background-color:#22BB22;color:#FFFFFF;text-align:center;">&#10003;</td>'
            else:
                bg, fg, bold = _overdue_cell_color(fd[f]["days_overdue"])
                style = ""
                if bg: style += f"background-color:{bg};"
                if fg: style += f"color:{fg};"
                style += "text-align:center;"
                eta_text = fd[f]["eta"]
                inner    = f"<b>{eta_text}</b>" if bold else eta_text
                cells   += f'<td style="{style}">{inner}</td>'

        rows += f'<tr><td><b>{subsys}</b></td>{cells}</tr>'

    # ── legend ───────────────────────────────────────────────────
    guide = (
        "<p><i>Colour: "
        "<span>1d</span> "
        '<span style="color:#FDC030"><i>3d</i></span> '
        '<span style="background-color:#90D9DB;color:#CD5937"> 7d </span> '
        '<span style="background-color:#FDD472;color:#B6424C"> 14d </span> '
        '<span style="background-color:#DF9299;color:#B6424C"><b>14d+</b></span>'
        " overdue</i></p>"
    )

    return (
        f'<table border="1">'
        f"<thead>{header}</thead>"
        f"<tbody>{rows}</tbody>"
        f"</table>{guide}"
    )


def _build_blank_eta_batch_html(owner: str, subsys_map: dict) -> str:
    """
    2-D ETA-Required table:  rows=subsys, columns=deliverable fields.

    subsys_map : {subsys: {field: str}}
      field value = ""           → blank   → yellow cell (#FDD472) — ETA missing
      field value = "2026/3/2"   → has ETA → plain cell showing the date
    """
    seen   = {f for fd in subsys_map.values() for f in fd}
    fields = [f for f in _FIELD_ORDER if f in seen]

    # ── header row ───────────────────────────────────────────────
    col_hdrs = "".join(
        f'<th style="{_TH_STYLE}"><b>{f.upper()}</b></th>' for f in fields
    )
    header = (
        f'<tr>'
        f'<td colspan="{len(fields)+1}"><b>ETA Required &#8212; Owner: {owner}</b></td>'
        f'</tr>'
        f'<tr><th style="{_TH_STYLE}"><b>Subsys</b></th>{col_hdrs}</tr>'
    )

    # ── data rows ────────────────────────────────────────────────
    rows = ""
    for subsys, fd in subsys_map.items():
        cells = ""
        for f in fields:
            if f not in fd:
                cells += "<td></td>"
            elif fd[f] == "":
                # Blank → yellow warning cell (same Teams palette yellow as 14d)
                cells += '<td style="background-color:#FDD472;color:#B6424C;text-align:center;">&#8212;</td>'
            else:
                cells += f"<td>{fd[f]}</td>"
        rows += f'<tr><td><b>{subsys}</b></td>{cells}</tr>'

    return (
        f'<table border="1">'
        f"<thead>{header}</thead>"
        f"<tbody>{rows}</tbody>"
        f"</table>"
        f'<p><i>'
        f'<span style="background-color:#FDD472;color:#B6424C"> &#8212; </span>'
        f" = ETA missing &nbsp; plain = ETA already provided"
        f"</i></p>"
    )




_FALLBACK_TEMPLATE = (
    "Hi [OWNER], the following deliverables are missing ETAs in the tracker. "
    "Could you please fill in your expected upload dates?\n\n"
    "[ITEMS]\n\n"
    "Please update the Excel file directly[LINK_NOTE] — "
    "do not reply to this automated message. Thank you!"
)


async def _get_eta_message_template(llm) -> str:
    """
    Query the LLM **once** to produce a reusable message template.

    The template must contain these literal placeholders:
      [OWNER]     — recipient's name
      [ITEMS]     — list of missing deliverables ("subsys: f1, f2" per line)
      [LINK_NOTE] — replaced with " at <link>" or "" if no link configured

    Returns the raw template string (plain text, not HTML).
    Falls back to ``_FALLBACK_TEMPLATE`` if LLM is None or fails.
    """
    if llm is None:
        return _FALLBACK_TEMPLATE

    prompt = (
        "Write a short, professional ETA-request message template for a "
        "hardware project tracker. The message will be sent to different engineers "
        "each time, so it uses these exact placeholders (keep them verbatim):\n\n"
        "  [OWNER]     — replaced with the recipient's name\n"
        "  [ITEMS]     — replaced with the list of missing deliverables\n"
        "  [LINK_NOTE] — replaced with ' at <Excel URL>' or empty string\n\n"
        "Requirements:\n"
        "- just use Hi [OWNER] and directly ask them to fill in the missing ETAs\n"
        "- Mention the items from [ITEMS] clearly\n"
        "- Instruct them to update the Excel file[LINK_NOTE] and NOT reply to this "
        "automated message\n"
        "- Plain text only, no emoji, no markdown, no HTML tags\n"
        "Output only the template text — nothing else."
    )
    try:
        tmpl = await llm.simple_query(prompt)
        # Validate that all required placeholders are present; fall back otherwise.
        if all(p in tmpl for p in ("[OWNER]", "[ITEMS]", "[LINK_NOTE]")):
            return tmpl
        logger.warning("[Teams] LLM template missing placeholders, using fallback.")
        return _FALLBACK_TEMPLATE
    except Exception as exc:
        logger.warning(f"[Teams] LLM template gen failed ({exc}), using fallback.")
        return _FALLBACK_TEMPLATE


def _render_eta_message(
    template: str,
    owner: str,
    subsys_map: dict,
    excel_link: str,
) -> str:
    """
    Fill *template* with owner-specific data and return an HTML paragraph.

    Sync (no LLM call).  Returns "" when nothing in *subsys_map* is blank.
    """
    lines = []
    for subsys, fd in subsys_map.items():
        missing = [f.replace("_status", "") for f, v in fd.items() if v == ""]
        if missing:
            lines.append(f"{subsys}: {', '.join(missing)}")

    if not lines:
        return ""

    link_note = f" at {excel_link}" if excel_link else ""
    items_str = "\n".join(lines)

    text = (
        template
        .replace("[OWNER]",     owner)
        .replace("[ITEMS]",     items_str)
        .replace("[LINK_NOTE]", link_note)
    )

    # Escape HTML, convert newlines, append clickable link
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = text.replace("\n\n", "</p><p>").replace("\n", "<br>")
    if excel_link:
        text += (
            f'<br>&#8594; <a href="{excel_link}">Open Excel tracker</a>'
        )
    return f"<p>{text}</p>"



# ─────────────────────────────────────────────────────────────
# MOCK notifier  (all methods are async for interface parity)
# ─────────────────────────────────────────────────────────────

class MockTeamsNotifier:
    """
    Prints messages to stdout — no actual Graph API calls.

    All public methods are ``async def`` so callers can uniformly
    ``await`` them whether Mock or Remote is in use.
    """

    def __init__(self, llm=None):
        self._llm = llm

    async def post_to_chat(self, html_body: str, chat_id: str = "MOCK"):
        print(f"\n{'='*60}")
        print(f"[MockTeams] → Chat: {chat_id}")
        print(html_body)
        print(f"{'='*60}\n")

    async def post_daily_summary(
        self,
        records: list,
        splunk_data: dict,
        eta_results_map: Optional[dict] = None,
    ):
        html = _build_summary_html(records, splunk_data, eta_results_map)
        await self.post_to_chat(html)

    async def send_eta_reminder(self, rec: SubsysRecord, field: str, eta: str):
        html = _build_reminder_html(rec, field, eta)
        await self.post_to_chat(html)

    async def send_overdue_alert(self, rec: SubsysRecord, field: str):
        """Single overdue alert (kept for backward-compat; prefer send_overdue_batch)."""
        html = _build_overdue_html(rec, field)
        await self.post_to_chat(html)

    async def send_blank_eta_nudge(self, rec: SubsysRecord, field: str):
        """Single ETA nudge (kept for backward-compat; prefer send_blank_eta_batch)."""
        html = _build_blank_eta_nudge_html(rec, field)
        await self.post_to_chat(html)

    async def send_overdue_batch(
        self,
        grouped: dict,
        chat_id: str = "MOCK",
    ):
        """One message per owner. Skips if all fields are already delivered."""
        for owner, subsys_map in grouped.items():
            html = _build_overdue_batch_html(owner, subsys_map)
            if html:   # empty string means all delivered — skip
                await self.post_to_chat(html, chat_id)

    async def send_blank_eta_batch(
        self,
        grouped: dict,
        chat_id: str = "MOCK",
    ):
        """One LLM template (1 call) then sync render per owner."""
        excel_link = getattr(config, "EXCEL_WEB_LINK", "")
        template   = await _get_eta_message_template(self._llm)
        for owner, subsys_map in grouped.items():
            msg = _render_eta_message(template, owner, subsys_map, excel_link)
            if msg:
                await self.post_to_chat(msg, chat_id)

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

    def __init__(self, llm=None):                                # --- REMOTE ONLY ---
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
        self._llm            = llm   # for _gen_blank_eta_message

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
        records:         list,
        splunk_data:     dict,
        eta_results_map: Optional[dict] = None,
        chat_id:         Optional[str]  = None,
    ):
        html = _build_summary_html(records, splunk_data, eta_results_map)
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
        """One Teams message per owner. Skips if all fields are delivered."""
        for owner, subsys_map in grouped.items():
            html = _build_overdue_batch_html(owner, subsys_map)
            if html:   # empty string = all delivered — skip post
                await self.post_to_chat(html, chat_id)
                logger.info(f"[Teams] Overdue batch → {owner!r}.")

    async def send_blank_eta_batch(                            # --- REMOTE ONLY ---
        self,
        grouped:  dict,
        chat_id:  Optional[str] = None,
    ):
        """One LLM template (1 call) then sync render per owner."""
        excel_link = getattr(config, "EXCEL_WEB_LINK", "")
        template   = await _get_eta_message_template(self._llm)
        for owner, subsys_map in grouped.items():
            msg = _render_eta_message(template, owner, subsys_map, excel_link)
            if msg:
                await self.post_to_chat(msg, chat_id)
                logger.info(f"[Teams] ETA-request → {owner!r}.")

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
