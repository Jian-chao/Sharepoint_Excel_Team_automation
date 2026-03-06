"""
modules/scheduler.py
=====================
APScheduler-based daily job orchestrator.

Jobs scheduled (all in local timezone defined by config.TIMEZONE):
  1. daily_summary   → config.DAILY_SUMMARY_TIME  (e.g. "09:00")
     - Reads Excel, queries Splunk, posts summary to Teams

  2. eta_reminder    → 1 hour before daily_summary
     - Checks each user-filled ETA field; if tomorrow = deadline, posts reminder

  3. overdue_tracker → config.DAILY_SUMMARY_TIME
     - Checks missing deliverables past deadline; posts overdue alerts

Usage:
    from modules.scheduler import AutomationScheduler
    sched = AutomationScheduler(sp_connector, excel_reader, splunk_connector, notifier)
    sched.start()
    # Blocks — press Ctrl+C to stop
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
from modules.eta_checker      import ETAChecker
from modules.excel_reader     import ExcelReader, SubsysRecord
from modules.splunk_connector import _BaseConnector
from modules.teams_notifier   import MockTeamsNotifier

logger = logging.getLogger(__name__)

# Deadline: read from config so it's configurable without touching module code.
_DEADLINE = getattr(config, "PROJECT_DEADLINE", date(2026, 2, 26))


def _parse_time(time_str: str):
    """Parse 'HH:MM' into (hour, minute) integers."""
    h, m = time_str.strip().split(":")
    return int(h), int(m)


class AutomationScheduler:
    """
    Orchestrates daily automation jobs using APScheduler's AsyncIOScheduler.

    Sync jobs  (daily_summary, overdue_tracker) run in APScheduler's default
    thread-pool executor so they never block the event loop.

    Async jobs (eta_reminder, eta_checker) are scheduled as coroutines and
    run directly on the event loop, allowing ``await`` inside them without
    blocking other tasks.

    Parameters
    ----------
    sp_connector   : sharepoint_connector.*
    splunk         : splunk_connector.*
    notifier       : teams_notifier.*
    llm            : optional LLM_cls instance (has ``async simple_query(prompt)->str``).
                     When supplied, ETA fields are parsed/corrected by the LLM.
    """

    def __init__(self, sp_connector, splunk, notifier, llm=None):
        self.sp       = sp_connector
        self.splunk   = splunk
        self.notifier = notifier
        self.llm      = llm
        self.tz       = pytz.timezone(config.TIMEZONE)

        # ETAChecker is created once; it holds the LLM reference
        self.eta_checker = ETAChecker(llm=llm, notifier=notifier)

        # Propagate LLM reference to the notifier so it can generate
        # LLM-varied ETA-request messages in send_blank_eta_batch.
        if llm is not None and not getattr(notifier, "_llm", None):
            notifier._llm = llm

        # Persistent event loop used exclusively by run_now().
        # asyncio.run() creates+destroys a new loop each call, which
        # invalidates async resources (httpx.AsyncClient inside GraphServiceClient)
        # that are reused across multiple run_now() calls.
        # Using a single long-lived loop avoids RuntimeError('Event loop is closed').
        self._run_now_loop: Optional[asyncio.AbstractEventLoop] = None

        hour, minute = _parse_time(config.DAILY_SUMMARY_TIME)
        # Reminder / ETA-checker runs 1 hour before summary
        remind_hour   = (hour - 1) % 24
        remind_minute = minute

        self._scheduler = AsyncIOScheduler(timezone=self.tz)
        self._scheduler.add_job(
            self._run_daily_summary,
            trigger="cron",
            hour=hour,
            minute=minute,
            id="daily_summary",
            name="Daily Status Summary",
        )
        self._scheduler.add_job(
            self._run_eta_reminder,
            trigger="cron",
            hour=remind_hour,
            minute=remind_minute,
            id="eta_reminder",
            name="ETA Reminder",
        )
        self._scheduler.add_job(
            self._run_overdue_tracker,
            trigger="cron",
            hour=hour,
            minute=minute,
            id="overdue_tracker",
            name="Overdue Tracker",
        )
        self._scheduler.add_job(
            self._run_eta_checker,
            trigger="cron",
            hour=remind_hour,
            minute=remind_minute,
            id="eta_checker",
            name="LLM ETA Checker",
        )
        logger.info(
            f"[Scheduler] Jobs configured. "
            f"Summary={config.DAILY_SUMMARY_TIME}, "
            f"Reminder={remind_hour:02d}:{remind_minute:02d}, "
            f"TZ={config.TIMEZONE}"
        )

    # ----------------------------------------------------------
    def _fetch_data(self):
        """Shared data fetch used by all jobs.

        ``splunk.query()`` is called **exactly once** per job run.

        The raw DataFrame is pivoted into a nested dict:

        .. code-block:: python

            {
              "venc_top_par_wrap": {"netlist": 0.0, "sdc": 1.0, "ccf": 0.0, "upf": 1.0, "OWNER": "..."},
              ...
            }

        Keys are the original MODULE strings from Splunk (case-preserved).
        Values are sub-dicts where each deliverable column holds the
        first ``Viol`` value found (float 0/1) or ``None`` if absent.
        Downstream consumers use ``sp.get(field)`` which treats 0 → False
        and 1 → True via the ``int(viol_val) == 1`` guard in ``_BaseConnector``.
        """
        excel_bytes = self.sp.download_excel()
        reader      = ExcelReader(excel_bytes)
        records     = reader.get_all_records(include_skipped=False)

        # Single Splunk query for all modules
        splunk_df = self.splunk.query()

        # Pivot: rows = MODULE, columns = SUB_GROUP (netlist/sdc/ccf/upf), values = Viol
        # aggfunc='first' keeps the latest-timestamp row (Splunk returns newest first).
        # where() converts NaN (missing SUB_GROUP for that module) to None so that
        # downstream sp.get(field) returns None rather than NaN.
        pivot_df = splunk_df.pivot_table(
            index   = ['MODULE', 'OWNER'],
            columns = 'SUB_GROUP',
            values  = 'Viol',
            aggfunc = 'first',
        )
        pivot_df.reset_index(inplace=True)

        # Convert to {MODULE: {col: value, ...}} — use MODULE as the dict key
        # (case-preserved so callers that do splunk_data.get(rec.subsys) work as
        # long as subsys names in Excel match Splunk MODULE names exactly).
        splunk_data = pivot_df.set_index('MODULE').where(
            pivot_df.set_index('MODULE').notna(), other=None
        ).T.to_dict()

        return records, splunk_data

    # ----------------------------------------------------------
    async def _run_daily_summary(self):
        logger.info("[Scheduler] ▶ Running: daily_summary")
        try:
            records, splunk_data = self._fetch_data()

            # Pre-compute PPT ETA for all records concurrently so the
            # summary table can display 'Mar/4' instead of raw strings.
            raw_eta = await asyncio.gather(
                *[self.eta_checker.parse_eta(rec.ppt_status) for rec in records],
                return_exceptions=True,
            )
            eta_results_map = {
                rec.subsys: (res if not isinstance(res, BaseException) else None)
                for rec, res in zip(records, raw_eta)
            }

            await self.notifier.post_daily_summary(records, splunk_data, eta_results_map)
            logger.info("[Scheduler] ✅ daily_summary done.")
        except Exception as exc:
            logger.error(f"[Scheduler] ❌ daily_summary failed: {exc}", exc_info=True)

    # ----------------------------------------------------------
    async def _run_eta_reminder(self):
        """
        ETA reminder (async): for each record whose PPT ETA is *tomorrow*,
        send a reminder.  Uses ETAChecker.parse_eta (async) so the event
        loop is not blocked while waiting for Azure responses.

        All per-record parse_eta calls are awaited sequentially here (the
        heavy concurrent fan-out happens inside check_records instead).
        """
        logger.info("[Scheduler] ▶ Running: eta_reminder")
        try:
            records, _ = self._fetch_data()
            tomorrow   = date.today() + timedelta(days=1)
            for rec in records:
                result = await self.eta_checker.parse_eta(rec.ppt_status)
                if result.parsed and result.parsed == tomorrow:
                    logger.info(
                        f"[Scheduler] Reminder: {rec.subsys}/ppt ETA is "
                        f"tomorrow ({result.iso})"
                    )
                    await self.notifier.send_eta_reminder(rec, "ppt", result.iso)
        except Exception as exc:
            logger.error(f"[Scheduler] ❌ eta_reminder failed: {exc}", exc_info=True)

    # ----------------------------------------------------------
    async def _run_eta_checker(self):
        """
        LLM ETA Checker (async):
          - Calls ``check_records`` which fans out all ppt_status LLM queries
            concurrently via ``asyncio.gather``, so N modules are checked in
            roughly the time of one LLM round-trip.
          - For blank ETA/upload fields, sends a Teams nudge to the owner.
          - Logs a report of all anomalies found.
        """
        logger.info("[Scheduler] ▶ Running: eta_checker")
        try:
            records, splunk_data = self._fetch_data()
            report = await self.eta_checker.check_records(
                records,
                splunk_data,
                nudge_blank=True,
            )
            if report:
                logger.info(
                    f"[Scheduler] ℹ️ eta_checker found {len(report)} item(s) to flag:"
                )
                for item in report:
                    eta_r     = item.get("eta_result")
                    iso_str   = eta_r.iso if eta_r else "-"
                    corrected = " [⚠️ year corrected]" if eta_r and eta_r.corrected else ""
                    logger.info(
                        f"  {item['subsys']}/{item['field']} "
                        f"status={item['status']} "
                        f"raw={item.get('raw', '')!r} "
                        f"parsed={iso_str}{corrected} "
                        f"owner={item['owner']}"
                    )
            else:
                logger.info("[Scheduler] ✅ eta_checker: no anomalies found.")
        except Exception as exc:
            logger.error(f"[Scheduler] ❌ eta_checker failed: {exc}", exc_info=True)

    # ----------------------------------------------------------
    async def _run_overdue_tracker(self):
        """
        Overdue tracker — 2-D batch notifications:
          • PPT deliverable → owner is FE (rec.fe)
          • NETLIST/SDC/CCF/UPF → owner is BE (rec.be)
          • ONE Teams message per owner, with a 2-D table:
              rows = subsys, columns = deliverable fields
          • Delivered field → green cell
          • Overdue field   → red gradient cell (light→dark based on days overdue)
          • ETA per field: custom date from Excel value, or _DEADLINE if blank
        """
        logger.info("[Scheduler] ▶ Running: overdue_tracker")
        try:
            records, splunk_data = self._fetch_data()
            today            = date.today()
            default_deadline = getattr(config, "DEFAULT_ETA", _DEADLINE)

            # FE-owned fields (normalised, no "_status" suffix)
            _fe_fields = {
                x.lower().replace("_status", "")
                for x in getattr(config, "FE_OWNED_FIELDS", ["ppt", "ppt_status"])
            }

            def _owner(rec, field: str) -> str:
                norm = field.lower().replace("_status", "")
                return (rec.fe if norm in _fe_fields else rec.be) or rec.fe or rec.be or "Unknown"

            # {owner: {subsys: {field: {"eta":str,"days_overdue":int,"delivered":bool}}}}
            grouped: dict[str, dict[str, dict]] = {}

            for rec in records:
                sp             = splunk_data.get(rec.subsys, {})
                fields_and_raw = {
                    "ppt":     rec.ppt_status,
                    "netlist": rec.netlist,
                    "sdc":     rec.sdc,
                    "ccf":     rec.ccf,
                    "upf":     rec.upf,
                }

                # ── Pass A: split fields into delivered / blank / parse-needed ─
                delivered_fields: dict[str, dict] = {}
                to_parse: list  = []   # (field, cleaned_raw)
                blank_fields: list = []  # field names with no ETA

                for field, excel_val in fields_and_raw.items():
                    splunk_ok = sp.get(field)
                    done = (
                        bool(excel_val and excel_val.lower() in ("v", "done", "x"))
                        or splunk_ok is True
                    )
                    if done:
                        delivered_fields[field] = {"eta": "✅", "days_overdue": 0, "delivered": True}
                        continue

                    cleaned = (excel_val or "").replace("eta:", "").strip()
                    if cleaned:
                        to_parse.append((field, cleaned))
                    else:
                        blank_fields.append(field)

                # ── Pass B: LLM / regex fan-out for non-blank non-delivered ────
                field_info: dict[str, dict] = dict(delivered_fields)

                if to_parse:
                    parse_results = await asyncio.gather(
                        *[self.eta_checker.parse_eta(c) for _, c in to_parse],
                        return_exceptions=True,
                    )
                    for (field, _), res in zip(to_parse, parse_results):
                        if isinstance(res, BaseException) or res is None or not getattr(res, "parsed", None):
                            eta_date = default_deadline
                        else:
                            eta_date = res.parsed
                        days_overdue = (today - eta_date).days
                        if days_overdue > 0:
                            field_info[field] = {
                                "eta":          eta_date.strftime("%Y/%m/%d"),
                                "days_overdue": days_overdue,
                                "delivered":    False,
                            }

                # ── Pass C: blank ETA fields → use default deadline ───────────
                for field in blank_fields:
                    days_overdue = (today - default_deadline).days
                    if days_overdue > 0:
                        field_info[field] = {
                            "eta":          default_deadline.strftime("%Y/%m/%d"),
                            "days_overdue": days_overdue,
                            "delivered":    False,
                        }

                has_overdue = any(
                    not info["delivered"] for info in field_info.values()
                )
                if not has_overdue:
                    continue

                # ── Group by owner (config-driven) ────────────────────────────
                for field, info in field_info.items():
                    owner = _owner(rec, field)
                    grouped.setdefault(owner, {}).setdefault(rec.subsys, {})[field] = info
                    if not info["delivered"]:
                        logger.warning(f"[Scheduler] OVERDUE: {rec.subsys}/{field} → {owner}")

            if grouped:
                total_items  = sum(len(fd) for sm in grouped.values() for fd in sm.values())
                logger.info(
                    f"[Scheduler] overdue_tracker: {total_items} item(s) across "
                    f"{len(grouped)} owner(s) — sending batch notification(s)."
                )
                await self.notifier.send_overdue_batch(grouped)
            else:
                logger.info("[Scheduler] ✅ overdue_tracker: no overdue items.")
        except Exception as exc:
            logger.error(f"[Scheduler] ❌ overdue_tracker failed: {exc}", exc_info=True)



    # ----------------------------------------------------------
    async def _async_run(self):
        """
        Async entry-point that keeps the event loop alive until cancelled.

        Called exclusively by :meth:`start` via ``asyncio.run()``.
        Separated from ``start()`` as a proper class method so that
        Pylance / static analysers do not flag spurious indentation warnings
        caused by a nested ``async def`` inside a plain ``def``.
        """
        self._scheduler.start()
        logger.info("[Scheduler] Started. Press Ctrl+C to stop.")
        try:
            # Yield to the event loop each second — keeps it alive without
            # a busy-spin while still allowing Ctrl+C to propagate.
            while True:
                await asyncio.sleep(1)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass
        finally:
            self._scheduler.shutdown(wait=False)
            logger.info("[Scheduler] Stopped.")

    # ----------------------------------------------------------
    def start(self):
        """
        Start the scheduler and block until Ctrl+C.

        Calls :meth:`_async_run` via ``asyncio.run()`` so the caller does
        not need an existing event loop.  APScheduler's ``AsyncIOScheduler``
        dispatches async jobs (eta_reminder, eta_checker) directly on the
        loop and sync jobs (daily_summary, overdue_tracker) to the default
        thread-pool executor.
        """
        try:
            asyncio.run(self._async_run())
        except (KeyboardInterrupt, SystemExit):
            pass   # already logged inside _async_run()

    # ----------------------------------------------------------
    def _get_run_loop(self) -> asyncio.AbstractEventLoop:
        """
        Return the persistent event loop used by :meth:`run_now`.

        Creates a new loop on first call (or if the previous loop was closed).
        Storing it on the instance means all consecutive ``run_now()`` calls
        share the SAME loop, so async resources such as the
        ``httpx.AsyncClient`` inside ``GraphServiceClient`` remain valid
        between calls instead of being invalidated when each
        ``asyncio.run()``-created loop is destroyed.
        """
        if self._run_now_loop is None or self._run_now_loop.is_closed():
            self._run_now_loop = asyncio.new_event_loop()
            logger.debug("[Scheduler] Created persistent run_now event loop.")
        return self._run_now_loop

    # ----------------------------------------------------------
    def run_now(self, job_id: str = "daily_summary"):
        """
        Manually trigger a single job by id.

        All async jobs share a single persistent event loop (see
        :meth:`_get_run_loop`) so that ``httpx.AsyncClient`` and other async
        resources inside the notifier / LLM clients are NOT re-created and
        re-bound on every call.  Calling ``asyncio.run()`` for each job would
        create and destroy a new loop each time, leaving those clients bound
        to a closed loop and causing ``RuntimeError: Event loop is closed`` on
        the second and subsequent async jobs.

        Call :meth:`close_run_loop` when all ``run_now`` calls are finished
        to release the loop and its resources cleanly.
        """
        job_map = {
            "daily_summary":   self._run_daily_summary,
            "eta_reminder":    self._run_eta_reminder,
            "overdue_tracker": self._run_overdue_tracker,
            "eta_checker":     self._run_eta_checker,
        }
        fn = job_map.get(job_id)
        if fn is None:
            raise ValueError(f"Unknown job id: {job_id!r}. Choose from {list(job_map)}")
        if asyncio.iscoroutinefunction(fn):
            self._get_run_loop().run_until_complete(fn())   # reuse the same loop
        else:
            fn()

    # ----------------------------------------------------------
    def close_run_loop(self):
        """
        Shut down the persistent event loop used by :meth:`run_now`.

        Call this once after all ``run_now()`` calls are complete
        (e.g. at the end of a ``--run-now`` or ``--check-eta`` CLI run)
        to close open connections and release resources.
        """
        if self._run_now_loop and not self._run_now_loop.is_closed():
            self._run_now_loop.run_until_complete(
                self._run_now_loop.shutdown_asyncgens()
            )
            self._run_now_loop.close()
            logger.debug("[Scheduler] Persistent run_now event loop closed.")
        self._run_now_loop = None
