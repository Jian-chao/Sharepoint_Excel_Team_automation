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

        # _llm_parse returns err="done" when the model recognises a completion
        # marker that the regex didn't catch (e.g. unusual "done" phrasing).
        if parsed is None and err == "done":
            return ETAResult(raw=raw, parsed=None, is_done=True, llm_used=llm_used)

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
        # 2-D nudge: {owner: {subsys: {field: eta_str}}}
        blank_nudge_grouped: dict[str, dict[str, dict]] = {}

        # Build the FE-owned field set from config (normalised, no "_status" suffix)
        _fe_fields = {
            f.lower().replace("_status", "")
            for f in getattr(config, "FE_OWNED_FIELDS", ["ppt", "ppt_status"])
        }

        def _owner(rec, field: str) -> str:
            """Return the responsible owner name based on config.FE_OWNED_FIELDS."""
            norm = field.lower().replace("_status", "")
            return (rec.fe if norm in _fe_fields else rec.be) or rec.fe or rec.be or "Unknown"

        def _add_nudge(owner: str, subsys: str, field: str, eta_str: str):
            blank_nudge_grouped.setdefault(owner, {}).setdefault(subsys, {})[field] = eta_str

        # ── Pass 1: classify every (rec, field) ─────────────────────────────────
        #   delivered → skip   |   blank → yellow cell, no LLM
        #   non-blank → queue for concurrent LLM / regex parse (Pass 2)
        to_parse: list = []   # [(rec, field, cleaned_raw)]

        for rec in records:
            sp = splunk_data.get(rec.subsys, {})
            for fld in fields:
                raw    = (getattr(rec, fld, "") or "").strip()
                sp_key = fld.replace("_status", "")
                done   = (
                    raw.lower() in ("v", "done", "x")
                    or sp.get(sp_key) is True
                )
                if done:
                    continue

                if not raw:
                    # Blank field — yellow cell, no LLM needed
                    if nudge_blank and self._notifier:
                        _add_nudge(_owner(rec, fld), rec.subsys, fld, "")
                    if fld != "ppt_status":
                        report.append({
                            "subsys": rec.subsys, "field": fld,
                            "status": "blank",    "owner": _owner(rec, fld),
                        })
                else:
                    # Non-blank, non-delivered → send all formats to LLM/regex.
                    # Handles: "3/2->3/4", "Mar/4th", "3/4(3/2)", "03.04", etc.
                    cleaned = raw.replace("eta:", "").strip()
                    to_parse.append((rec, fld, cleaned))

        # ── Pass 2: fan-out ALL parse_eta coroutines concurrently ───────────────
        #   Covers ppt_status, netlist, sdc, ccf, upf uniformly — the LLM
        #   resolves complex DE date shorthand for every deliverable type.
        eta_results: list = await asyncio.gather(
            *[self.parse_eta(cleaned) for _, _, cleaned in to_parse],
            return_exceptions=True,
        )

        # ── Pass 3: process gathered results ────────────────────────────────────
        for (rec, fld, raw_val), result in zip(to_parse, eta_results):
            if isinstance(result, BaseException):
                result = ETAResult(
                    raw=raw_val, parsed=None,
                    parse_error=f"Unhandled exception: {result}",
                )

            entry = {
                "subsys":     rec.subsys,
                "field":      fld,
                "raw":        raw_val,
                "owner":      _owner(rec, fld),
                "eta_result": result,
            }

            if result.is_done or result.is_na:
                continue

            if result.is_blank:
                if nudge_blank and self._notifier:
                    _add_nudge(_owner(rec, fld), rec.subsys, fld, "")
                entry["status"] = "blank"
                report.append(entry)
                continue

            if result.parse_error:
                logger.warning(
                    f"[ETAChecker] Cannot parse ETA for "
                    f"{rec.subsys}/{fld}: {raw_val!r} → {result.parse_error}"
                )
                entry["status"] = "parse_error"
                report.append(entry)
                continue

            # Valid parsed ETA — white cell (date shown) in ETA-Required table
            if nudge_blank and self._notifier and result.iso:
                _add_nudge(_owner(rec, fld), rec.subsys, fld, result.iso)

            if result.corrected:
                logger.warning(
                    f"[ETAChecker] Year-flip: {rec.subsys}/{fld} "
                    f"{raw_val!r} → {result.iso}"
                )
                entry["status"] = "year_corrected"
                report.append(entry)

            if result.days_until is not None and result.days_until < 0:
                logger.warning(
                    f"[ETAChecker] OVERDUE ETA: {rec.subsys}/{fld} "
                    f"was {result.iso} ({abs(result.days_until)}d ago)"
                )

        # ── Pass 4: batch-send ETA nudges (one message per owner) ───────────────
        await self._dispatch_blank_eta_batch(blank_nudge_grouped)
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

        Passes only the raw ETA value to ``simple_query``; all role definition,
        output-format rules, and few-shot examples now live in the system
        prompt inside ``LLM_cls.simple_query`` (via ``ChatPromptTemplate``).

        Returns ``(date | None, error_msg, llm_used=True)``.

        Special return behaviours
        ------------------------
        * ``{"date": "done"}``  — model recognised a completion marker (v, x …)
          → returned as ``(None, "", True)`` with ``is_done`` flag propagated
          via ``ETAResult`` by the caller.
        * ``{"date": null}``    — model could not parse → ``(None, error, True)``
        """
        try:
            raw_reply = await self._llm.simple_query(raw)   # ← raw value only

            # The system prompt guarantees JSON-only output, but defensively
            # extract the first {...} block in case any stray text slips through.
            json_match = re.search(r"\{.*?\}", raw_reply, re.DOTALL)
            if not json_match:
                return None, f"LLM returned no JSON: {raw_reply[:120]!r}", True

            payload = json.loads(json_match.group())
            date_val = payload.get("date")

            # Completion sentinel — model says the field means "done / N/A"
            if isinstance(date_val, str) and date_val.lower() == "done":
                return None, "done", True

            # Null — model could not determine a valid date
            if date_val is None:
                return None, payload.get("error", "LLM returned null date"), True

            parsed = date.fromisoformat(date_val)
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

    async def _dispatch_blank_eta_batch(
        self,
        grouped: dict,   # {owner: [{"subsys", "field", "fe", "bc"}, ...]}
    ) -> None:
        """
        Send ONE Teams message per owner covering all their blank ETA fields.

        Delegates to ``notifier.send_blank_eta_batch`` (preferred: single
        message per person, built-in rate-limiting) with a log-only fallback
        if the notifier does not expose that method.
        """
        if not grouped or not self._notifier:
            return
        if hasattr(self._notifier, "send_blank_eta_batch"):
            try:
                total = sum(len(v) for v in grouped.values())
                logger.info(
                    f"[ETAChecker] Sending blank-ETA batch: {total} item(s) "
                    f"across {len(grouped)} owner(s)."
                )
                await self._notifier.send_blank_eta_batch(grouped)
                return
            except Exception as exc:
                logger.warning(
                    f"[ETAChecker] send_blank_eta_batch failed: {exc}"
                )
        # Fallback: log only (avoids per-item API calls as safety net)
        for owner, items in grouped.items():
            for it in items:
                logger.info(
                    f"[ETAChecker] (fallback) Blank ETA: "
                    f"{it['subsys']}/{it['field']} → {owner}"
                )


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
