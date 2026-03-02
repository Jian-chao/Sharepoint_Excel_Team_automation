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

import logging
from datetime import date, datetime, timedelta

import pytz
from apscheduler.schedulers.blocking import BlockingScheduler

import config
from modules.excel_reader import ExcelReader, SubsysRecord
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
    Orchestrates daily automation jobs.

    Parameters
    ----------
    sp_connector   : sharepoint_connector.*
    splunk         : splunk_connector.*
    notifier       : teams_notifier.*
    """

    def __init__(self, sp_connector, splunk, notifier):
        self.sp       = sp_connector
        self.splunk   = splunk
        self.notifier = notifier
        self.tz       = pytz.timezone(config.TIMEZONE)

        hour, minute = _parse_time(config.DAILY_SUMMARY_TIME)
        # Reminder runs 1 hour before summary
        remind_hour   = (hour - 1) % 24
        remind_minute = minute

        self._scheduler = BlockingScheduler(timezone=self.tz)
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
    def _run_eta_reminder(self):
        logger.info("[Scheduler] ▶ Running: eta_reminder")
        try:
            records, _ = self._fetch_data()
            tomorrow   = date.today() + timedelta(days=1)
            for rec in records:
                # Check PPT ETA
                self._check_eta_field(rec, "ppt", rec.ppt_status, tomorrow)
        except Exception as exc:
            logger.error(f"[Scheduler] ❌ eta_reminder failed: {exc}", exc_info=True)

    def _check_eta_field(self, rec: SubsysRecord, field: str, val: str, tomorrow: date):
        """Send reminder if a user-supplied ETA is tomorrow."""
        if not val:
            return
        val_lower = val.lower().strip()
        if val_lower in ("done", "v", "x", "n/a"):
            return
        eta_str = val_lower.replace("eta:", "").strip()
        try:
            eta = datetime.strptime(eta_str, "%Y-%m-%d").date()
            if eta == tomorrow:
                logger.info(f"[Scheduler] Reminder: {rec.subsys} {field} ETA is tomorrow ({eta})")
                self.notifier.send_eta_reminder(rec, field, str(eta))
        except ValueError:
            pass   # not a parseable date

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
        """Start the scheduler (blocking — runs until Ctrl+C)."""
        logger.info("[Scheduler] Starting. Press Ctrl+C to stop.")
        try:
            self._scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("[Scheduler] Stopped.")

    def run_now(self, job_id: str = "daily_summary"):
        """Manually trigger a job by id (for testing)."""
        job_map = {
            "daily_summary":   self._run_daily_summary,
            "eta_reminder":    self._run_eta_reminder,
            "overdue_tracker": self._run_overdue_tracker,
        }
        fn = job_map.get(job_id)
        if fn:
            fn()
        else:
            raise ValueError(f"Unknown job id: {job_id}. Choose from {list(job_map)}")
