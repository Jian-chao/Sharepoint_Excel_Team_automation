"""
modules/eta_checker.py
======================
LLM-assisted ETA field parser and blank-ETA Teams notifier.

Responsibilities
----------------
1. **Parse fuzzy ETA strings** typed by designers into strict ``date`` objects.
   Designers may write any of:
       "2026.03.02"  "3/2"  "March 3rd"  "Mar.3"  "eta: 3/5"  "done"  "v"
   The LLM normalises them to ISO 8601 (YYYY-MM-DD).

2. **Year-flip correction** — if the parsed year is the *previous* calendar year
   and the delta to today is > 300 days backwards, assume the user forgot to
   update the year at the turn of the year (e.g. typed "2025.3.2" in March 2026)
   and silently bump the year by 1.

3. **Blank ETA nudge** — for records whose ETA field is blank (and the file has
   not yet been confirmed uploaded), post a Teams message to the owner asking
   for their expected upload date.

Usage (standalone — must be called from an async context)
----------------------------------------------------------
    import asyncio
    from modules.eta_checker import ETAChecker
    checker = ETAChecker(llm=my_llm_instance, notifier=teams_notifier)

    # Parse a single fuzzy string
    result = await checker.parse_eta("Mar.3")
    # → ETAResult(raw="Mar.3", parsed=date(2026, 3, 3), corrected=False, ...)

    # Process all records concurrently — parse ETAs and nudge blank ones
    report = await checker.check_records(records, splunk_data)
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Result dataclass
# ─────────────────────────────────────────────────────────────

@dataclass
class ETAResult:
    """Outcome of parsing a single ETA cell value."""

    raw:           str            # original string from Excel cell
    parsed:        Optional[date] # normalised date, None if un-parseable
    corrected:     bool = False   # True if year-flip correction was applied
    is_done:       bool = False   # True if value means "already uploaded"
    is_na:         bool = False   # True if value means "not required"
    is_blank:      bool = False   # True if cell was empty
    parse_error:   str  = ""      # non-empty if LLM/fallback failed to parse
    llm_used:      bool = False   # True if the LLM was invoked

    @property
    def iso(self) -> Optional[str]:
        """Return the date as an ISO string, or None."""
        return self.parsed.isoformat() if self.parsed else None

    @property
    def days_until(self) -> Optional[int]:
        """Days from today until the parsed ETA (negative = past)."""
        if self.parsed is None:
            return None
        return (self.parsed - date.today()).days


# ─────────────────────────────────────────────────────────────
# Regex fast-path patterns (no LLM needed for these)
# ─────────────────────────────────────────────────────────────

_DONE_RE  = re.compile(r"^\s*(done|v|✅)\s*$", re.IGNORECASE)
_NA_RE    = re.compile(r"^\s*(n/?a|x|not\s+required|不需要)\s*$", re.IGNORECASE)
_ISO_RE   = re.compile(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})")       # 2026.03.02
_MDY_RE   = re.compile(r"(\d{1,2})[/.-](\d{1,2})(?:[/.-](\d{2,4}))?") # 3/2 or 3/2/26
_ETA_PFX  = re.compile(r"^\s*eta\s*:\s*", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────
# Main class
# ─────────────────────────────────────────────────────────────

class ETAChecker:
    """
    LLM-assisted ETA field normaliser and blank-ETA Teams notifier.

    Parameters
    ----------
    llm :
        Any object that has a ``simple_query(prompt: str) -> str`` method
        (your ``LLM_cls`` instance).  Pass ``None`` to disable LLM and use
        only the regex fast-path.
    notifier :
        A ``MockTeamsNotifier`` / ``RemoteTeamsNotifier`` instance.  Used to
        send Teams nudges for blank ETA fields.  Pass ``None`` to disable.
    reference_date : date, optional
        The date to use as "today" for year-flip logic (defaults to
        ``date.today()``).  Useful for unit testing.
    year_flip_threshold_days : int
        If the parsed date is more than this many days *before* today, assume
        a year-flip typo and add 1 year.  Default: 300 days.
    """

    def __init__(
        self,
        llm=None,
        notifier=None,
        reference_date: Optional[date] = None,
        year_flip_threshold_days: int = 300,
    ):
        self._llm       = llm
        self._notifier  = notifier
        self._today     = reference_date or date.today()
        self._yft       = year_flip_threshold_days

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    async def parse_eta(self, raw: str) -> ETAResult:
        """
        Parse a single ETA cell value.  **Must be awaited.**

        Resolution order
        ----------------
        1. Blank  → ETAResult(is_blank=True)           — no I/O
        2. "done" / "v" → ETAResult(is_done=True)      — no I/O
        3. "n/a" / "x"  → ETAResult(is_na=True)        — no I/O
        4. Regex fast-path for common date formats      — no I/O
        5. LLM fallback (async, non-blocking)           — awaits Azure
        6. Year-flip correction on the resulting date   — no I/O
        """
        if not raw or not raw.strip():
            return ETAResult(raw=raw, parsed=None, is_blank=True)

        # Strip leading "eta:" prefix before everything else
        cleaned = _ETA_PFX.sub("", raw).strip()

        if _DONE_RE.match(cleaned):
            return ETAResult(raw=raw, parsed=None, is_done=True)
        if _NA_RE.match(cleaned):
            return ETAResult(raw=raw, parsed=None, is_na=True)

        # --- Regex fast-path (synchronous, no await) ---
        parsed, err = self._regex_parse(cleaned)

        # --- LLM fallback (async, non-blocking) ---
        llm_used = False
        if parsed is None and self._llm is not None:
            parsed, err, llm_used = await self._llm_parse(raw, cleaned)

        if parsed is None:
            return ETAResult(
                raw=raw, parsed=None,
                parse_error=err or "Could not parse date",
                llm_used=llm_used,
            )

        # --- Year-flip correction (synchronous) ---
        parsed, corrected = self._correct_year_flip(parsed, raw)

        return ETAResult(
            raw=raw, parsed=parsed,
            corrected=corrected,
            llm_used=llm_used,
        )

    async def check_records(
        self,
        records,            # list[SubsysRecord]
        splunk_data: dict,  # {subsys: {"netlist": bool|None, ...}}
        fields: tuple[str, ...] = ("ppt_status", "netlist", "sdc", "ccf", "upf"),
        nudge_blank: bool = True,
    ) -> list[dict]:
        """
        Iterate over all records, parse ETA fields, and optionally send Teams
        nudges for blank ETA fields.  **Must be awaited.**

        All ``ppt_status`` LLM parse calls are launched **concurrently** via
        ``asyncio.gather`` — the event loop is free to handle other work while
        waiting for Azure responses, and N module queries finish in roughly the
        time of the slowest single query rather than N × that time.

        Parameters
        ----------
        records :
            list[SubsysRecord] from ExcelReader.
        splunk_data :
            Splunk upload-status map built by the scheduler.
        fields :
            Which record attributes to check.  The ETA logic only applies to
            ``ppt_status``; the others are treated as upload-flag fields.
        nudge_blank : bool
            If True and a notifier was supplied, post a Teams prompt asking the
            owner for their ETA when a field is blank and not yet uploaded.

        Returns
        -------
        list[dict]
            One dict per (record, field) combination that is interesting
            (blank, needs correction, or couldn't be parsed).
        """
        report = []

        # ── Step 1: handle non-ppt upload-flag fields synchronously ──────────
        #   These never need LLM calls — process them up-front.
        for rec in records:
            sp = splunk_data.get(rec.subsys, {})
            for fld in fields:
                if fld == "ppt_status":
                    continue  # handled below
                raw_val = getattr(rec, fld, "") or ""
                uploaded = (
                    raw_val.lower() in ("v", "done", "x")
                    or sp.get(fld) is True
                )
                if uploaded:
                    continue
                if not raw_val or raw_val.strip() == "":
                    if nudge_blank and self._notifier:
                        await self._send_blank_nudge(rec, fld)
                    report.append({
                        "subsys": rec.subsys,
                        "field":  fld,
                        "status": "blank",
                        "owner":  rec.be,
                    })

        # ── Step 2: fan out ALL ppt_status parse_eta calls concurrently ──────
        #   Build the task list first, keeping track of which record each
        #   task belongs to, then gather them all in one shot.
        if "ppt_status" not in fields:
            return report

        ppt_records = [
            (rec, getattr(rec, "ppt_status", "") or "")
            for rec in records
        ]

        # asyncio.gather fires all coroutines; return_exceptions prevents one
        # failed LLM call from aborting the rest.
        eta_results: list[ETAResult | BaseException] = await asyncio.gather(
            *[self.parse_eta(raw) for _, raw in ppt_records],
            return_exceptions=True,
        )

        # ── Step 3: process gathered results ─────────────────────────────────
        for (rec, raw_val), result in zip(ppt_records, eta_results):
            # Wrap unexpected exceptions in a failed ETAResult
            if isinstance(result, BaseException):
                result = ETAResult(
                    raw=raw_val, parsed=None,
                    parse_error=f"Unhandled exception: {result}",
                )

            entry = {
                "subsys":     rec.subsys,
                "field":      "ppt_status",
                "raw":        raw_val,
                "owner":      rec.be,
                "eta_result": result,
            }

            if result.is_done or result.is_na:
                continue

            if result.is_blank:
                if nudge_blank and self._notifier:
                    await self._send_blank_nudge(rec, "ppt_status")
                entry["status"] = "blank"
                report.append(entry)
                continue

            if result.parse_error:
                logger.warning(
                    f"[ETAChecker] Could not parse ETA for "
                    f"{rec.subsys}/ppt_status: {raw_val!r} → {result.parse_error}"
                )
                entry["status"] = "parse_error"
                report.append(entry)
                continue

            if result.corrected:
                logger.warning(
                    f"[ETAChecker] Year-flip correction applied for "
                    f"{rec.subsys}/ppt_status: {raw_val!r} → {result.iso}"
                )
                entry["status"] = "year_corrected"
                report.append(entry)

            if result.days_until is not None and result.days_until < 0:
                logger.warning(
                    f"[ETAChecker] OVERDUE ETA: {rec.subsys}/ppt_status "
                    f"was {result.iso} ({abs(result.days_until)}d ago)"
                )

        return report

    # ----------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------

    def _regex_parse(self, text: str) -> tuple[Optional[date], str]:
        """
        Try common date patterns without calling the LLM.

        Returns (date, "") on success or (None, error_msg) on failure.
        """
        today = self._today

        # YYYY.MM.DD / YYYY-MM-DD / YYYY/MM/DD
        m = _ISO_RE.search(text)
        if m:
            try:
                yr, mo, dy = int(m.group(1)), int(m.group(2)), int(m.group(3))
                return date(yr, mo, dy), ""
            except ValueError as e:
                return None, str(e)

        # MM/DD  or  MM/DD/YY  or  MM/DD/YYYY  (local convention: M/D)
        m = _MDY_RE.search(text)
        if m:
            try:
                mo = int(m.group(1))
                dy = int(m.group(2))
                if m.group(3):
                    yr_raw = int(m.group(3))
                    yr = yr_raw if yr_raw > 99 else 2000 + yr_raw
                else:
                    # No year supplied — assume current year, unless that date
                    # is already far in the past (→ next year)
                    yr = today.year
                    candidate = date(yr, mo, dy)
                    if (today - candidate).days > 60:
                        yr += 1
                return date(yr, mo, dy), ""
            except ValueError as e:
                return None, str(e)

        return None, "No regex pattern matched"

    async def _llm_parse(
        self, raw: str, cleaned: str
    ) -> tuple[Optional[date], str, bool]:
        """
        Ask the LLM to normalise the date string.  **Must be awaited.**

        Calls ``self._llm.simple_query`` which is itself an async coroutine,
        so the event loop is free while waiting for Azure's response.

        Returns (date | None, error_msg, llm_used=True).
        """
        today_str = self._today.isoformat()
        prompt = f"""You are a date normaliser. Today is {today_str}.

A designer filled in an ETA field in an Excel sheet with this text:
  "{raw}"

Your task:
1. Identify the intended calendar date (ignore any "eta:" prefix).
2. If the year is missing, assume {self._today.year} (bump to {self._today.year + 1} if the date has already passed by more than 60 days).
3. Output ONLY a JSON object with a single key "date" containing the ISO 8601 date string (YYYY-MM-DD), for example:
   {{"date": "{today_str}"}}
4. If you cannot determine a valid date at all, output:
   {{"date": null, "error": "reason"}}

Do NOT include any explanation outside the JSON object."""

        try:
            raw_reply = await self._llm.simple_query(prompt)   # ← non-blocking await
            # Extract the JSON object from the reply (LLM may wrap in markdown fences)
            json_match = re.search(r"\{.*?\}", raw_reply, re.DOTALL)
            if not json_match:
                return None, f"LLM returned no JSON: {raw_reply[:120]}", True
            payload = json.loads(json_match.group())
            if payload.get("date") is None:
                return None, payload.get("error", "LLM returned null date"), True
            parsed = date.fromisoformat(payload["date"])
            return parsed, "", True
        except json.JSONDecodeError as e:
            return None, f"LLM JSON parse error: {e}", True
        except Exception as e:
            logger.error(f"[ETAChecker] LLM call failed: {e}", exc_info=True)
            return None, f"LLM exception: {e}", True

    def _correct_year_flip(self, d: date, raw: str) -> tuple[date, bool]:
        """
        If the parsed date is more than ``year_flip_threshold_days`` in the
        past, assume the user forgot to update the year (common just after
        New Year) and add 1 year.
        """
        delta = (self._today - d).days
        if delta > self._yft:
            corrected = date(d.year + 1, d.month, d.day)
            logger.info(
                f"[ETAChecker] Year-flip: {raw!r} → {d.isoformat()} "
                f"({delta}d ago) corrected to {corrected.isoformat()}"
            )
            return corrected, True
        return d, False

    async def _send_blank_nudge(self, rec, field: str):
        """
        Post a Teams message asking the owner for their ETA.

        Awaits ``notifier.send_blank_eta_nudge()`` if available, otherwise
        falls back to ``notifier.post_to_chat()`` with a plain HTML message.
        Both notifier implementations are now async, so this method
        must also be ``async def``.
        """
        owner      = rec.be or rec.fe or "owner"
        field_name = field.replace("_status", "").upper()
        html = (
            f"<p>📅 <b>ETA Required:</b> The <b>{field_name}</b> field for "
            f"subsys <b>{rec.subsys}</b> is blank.<br>"
            f"Hi <b>{owner}</b>, could you please provide your expected upload "
            f"date so we can track the delivery status? Thank you! 🙏</p>"
        )

        # Use dedicated method if the notifier exposes it
        if hasattr(self._notifier, "send_blank_eta_nudge"):
            try:
                await self._notifier.send_blank_eta_nudge(rec, field)
                return
            except Exception as e:
                logger.warning(f"[ETAChecker] send_blank_eta_nudge failed: {e}")

        # Fallback: generic post_to_chat
        if hasattr(self._notifier, "post_to_chat"):
            try:
                await self._notifier.post_to_chat(html)
            except Exception as e:
                logger.warning(f"[ETAChecker] post_to_chat fallback failed: {e}")
        else:
            logger.info(
                f"[ETAChecker] (no notifier) Blank ETA nudge for "
                f"{rec.subsys}/{field} \u2192 {owner}"
            )
