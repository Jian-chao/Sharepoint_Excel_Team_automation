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

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
from modules.eta_checker      import ETAChecker
from modules.excel_reader     import ExcelReader, SubsysRecord
from modules.splunk_connector import _BaseConnector
from modules.teams_notifier   import MockTeamsNotifier

logger = logging.getLogger(__name__)

# Deadline date (parsed lazily from config / Excel cell)
_DEADLINE = date(2026, 2, 26)   # update dynamically if needed


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
        The returned DataFrame is processed in bulk via
        ``get_all_statuses()`` so no extra Splunk round-trips occur.
        """
        excel_bytes = self.sp.download_excel()
        reader      = ExcelReader(excel_bytes)
        records     = reader.get_all_records(include_skipped=False)

        # Single Splunk query for all modules
        splunk_df   = self.splunk.query()
        all_statuses = self.splunk.get_all_statuses(splunk_df)

        # Build per-subsys map; fall back to empty dict if module not in Splunk
        splunk_data = {
            rec.subsys: all_statuses.get(rec.subsys.lower(), {col: None for col in ["netlist", "sdc", "ccf", "upf"]})
            for rec in records
        }
        return records, splunk_data

    # ----------------------------------------------------------
    def _run_daily_summary(self):
        logger.info("[Scheduler] ▶ Running: daily_summary")
        try:
            records, splunk_data = self._fetch_data()
            self.notifier.post_daily_summary(records, splunk_data)
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
                    self.notifier.send_eta_reminder(rec, "ppt", result.iso)
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
    def _run_overdue_tracker(self):
        logger.info("[Scheduler] ▶ Running: overdue_tracker")
        try:
            records, splunk_data = self._fetch_data()
            today    = date.today()
            deadline = _DEADLINE

            for rec in records:
                sp = splunk_data.get(rec.subsys, {})
                # Check each user-fill deliverable
                fields_and_excel = {
                    "ppt":     rec.ppt_status,
                    "netlist": rec.netlist,
                    "sdc":     rec.sdc,
                    "ccf":     rec.ccf,
                    "upf":     rec.upf,
                }
                for field, excel_val in fields_and_excel.items():
                    splunk_ok = sp.get(field)
                    uploaded  = (
                        (excel_val.lower() in ("v", "done", "x") if excel_val else False)
                        or splunk_ok is True
                    )
                    if not uploaded and today > deadline:
                        logger.warning(f"[Scheduler] OVERDUE: {rec.subsys}/{field}")
                        self.notifier.send_overdue_alert(rec, field)
        except Exception as exc:
            logger.error(f"[Scheduler] ❌ overdue_tracker failed: {exc}", exc_info=True)

    # ----------------------------------------------------------
    def start(self):
        """
        Start the scheduler and block until Ctrl+C.

        Runs ``asyncio.run`` internally so the caller doesn't need an
        existing event loop.  APScheduler's ``AsyncIOScheduler`` schedules
        all async jobs (eta_reminder, eta_checker) directly on the loop,
        while sync jobs (daily_summary, overdue_tracker) are dispatched to
        the default thread-pool executor.
        """
        async def _run():
            self._scheduler.start()
            logger.info("[Scheduler] Started. Press Ctrl+C to stop.")
            try:
                # Yield control back to the event loop every second
                # (keeps the loop alive without a busy-spin)
                while True:
                    await asyncio.sleep(1)
            except (asyncio.CancelledError, KeyboardInterrupt):
                pass
            finally:
                self._scheduler.shutdown(wait=False)
                logger.info("[Scheduler] Stopped.")

        try:
            asyncio.run(_run())
        except (KeyboardInterrupt, SystemExit):
            pass   # already logged inside _run()

    def run_now(self, job_id: str = "daily_summary"):
        """
        Manually trigger a job by id (for testing / CLI flags).

        Sync jobs are called directly; async jobs are run via
        ``asyncio.run()`` so this method is always safe to call from a
        plain synchronous context (e.g. the ``main.py`` entrypoint).
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
            asyncio.run(fn())
        else:
            fn()
