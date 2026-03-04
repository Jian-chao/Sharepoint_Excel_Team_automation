"""
main.py — Entry point for the FDI DEF automation system.

Run modes:
  python main.py               → Start the daily scheduler (blocking)
  python main.py --run-now     → Trigger all jobs once immediately, then exit
  python main.py --check-eta   → Run only the LLM ETA-checker job, then exit
  python main.py --llm-prompt  → Generate & print the LLM Excel analysis prompt
"""

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")


def build_stack():
    """Instantiate all connectors and the scheduler."""
    from modules.sharepoint_connector import get_connector as sp_conn
    from modules.splunk_connector      import get_connector as splunk_conn
    from modules.teams_notifier        import get_notifier
    from modules.scheduler             import AutomationScheduler

    sp       = sp_conn()
    splunk   = splunk_conn()
    notifier = get_notifier()

    # Optional LLM — import gracefully so the system works without it
    llm = None
    try:
        from modules.llm_connector import LLM_cls   # your LangChain wrapper
        llm = LLM_cls()
        import logging
        logging.getLogger("main").info("LLM connector loaded successfully.")
    except ImportError:
        import logging
        logging.getLogger("main").warning(
            "modules/llm_connector.py not found — ETAChecker will use regex only."
        )

    scheduler = AutomationScheduler(sp, splunk, notifier, llm=llm)
    return scheduler


def main():
    parser = argparse.ArgumentParser(
        description="FDI DEF request status automation"
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Run all jobs immediately and exit (for testing)",
    )
    parser.add_argument(
        "--check-eta",
        action="store_true",
        help="Run only the LLM ETA-checker job and exit",
    )
    parser.add_argument(
        "--llm-prompt",
        action="store_true",
        help="Print the LLM Excel-analysis prompt and exit",
    )
    parser.add_argument(
        "--owner-notes",
        type=str,
        default="",
        help="Owner annotations to embed in the LLM prompt (used with --llm-prompt)",
    )
    args = parser.parse_args()

    if args.llm_prompt:
        from modules.excel_reader import ExcelReader
        reader = ExcelReader()
        prompt = reader.generate_llm_prompt(args.owner_notes)
        print(prompt)
        # Also save to file
        reader.save_llm_prompt("excel_llm_prompt.txt", args.owner_notes)
        logger.info("Prompt saved to excel_llm_prompt.txt")
        sys.exit(0)

    scheduler = build_stack()

    if args.check_eta:
        logger.info("=== Running ETA checker (--check-eta) ===")
        try:
            scheduler.run_now("eta_checker")
        finally:
            scheduler.close_run_loop()
        logger.info("=== Done ===")
    elif args.run_now:
        logger.info("=== Running all jobs immediately (--run-now) ===")
        try:
            scheduler.run_now("eta_checker")
            scheduler.run_now("eta_reminder")
            scheduler.run_now("daily_summary")
            scheduler.run_now("overdue_tracker")
        finally:
            scheduler.close_run_loop()   # release the shared loop and its connections
        logger.info("=== Done ===")
    else:
        scheduler.start()   # blocking


if __name__ == "__main__":
    main()
