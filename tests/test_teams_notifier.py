"""
tests/test_teams_notifier.py
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from modules.teams_notifier import (
    MockTeamsNotifier,
    _build_summary_html,
    _status_icon,
    _ppt_icon,
    _build_overdue_batch_html,
    _gen_blank_eta_message,
)
from modules.excel_reader import SubsysRecord
from datetime import date


def _dummy_record(subsys="test_sub", fe="FE_user", bc="BC_user", be="BE_user",
                  ppt="", netlist="", sdc="", ccf="", upf=""):
    return SubsysRecord(
        row=4, subsys=subsys, fe=fe, bc=bc, be=be,
        ppt_status=ppt, netlist=netlist, sdc=sdc, ccf=ccf, upf=upf,
        release_date="", remarks="", skip=False, font_color_b="FF000000"
    )


# ── _status_icon / _ppt_icon ──────────────────────────────────────────────────

def test_status_icon():
    assert "Done" in _status_icon("v")
    assert "N/A"  in _status_icon("x")
    assert "Done" in _status_icon("",  splunk_val=True)
    assert "Missing" in _status_icon("", splunk_val=False)
    assert "?"    in _status_icon("",  splunk_val=None)
    # No raw Emoji should appear
    for s in ["✅", "❌", "➖", "❓", "⏳"]:
        assert s not in _status_icon("v"), f"Emoji {s!r} found in status_icon output"


def test_ppt_icon():
    d_past   = date(2020, 1, 1)
    d_future = date(2099, 1, 1)
    assert "Done"    in _ppt_icon("done", d_future)
    assert "N/A"     in _ppt_icon("n/a",  d_future)
    assert "OVERDUE" in _ppt_icon("",     d_past)    # overdue
    assert "Missing" in _ppt_icon("",     d_future)  # missing but not yet overdue


# ── _build_summary_html ───────────────────────────────────────────────────────

def test_build_summary_html():
    records = [_dummy_record("sub_a", netlist="v", sdc="", ccf="x", upf="")]
    splunk_data = {"sub_a": {"netlist": True, "sdc": False, "ccf": None, "upf": True}}
    html = _build_summary_html(records, splunk_data)
    assert "<table" in html
    assert "sub_a"  in html
    assert "FDI DEF" in html
    # No raw Emoji
    for emoji in ["✅", "❌", "🔴", "➖", "❓", "⏳", "📋"]:
        assert emoji not in html, f"Emoji {emoji!r} found in summary HTML"


def test_build_summary_html_with_eta_map():
    """eta_results_map is forwarded to _ppt_cell_attrs for Mar/Day display."""
    from modules.eta_checker import ETAResult
    records = [_dummy_record("sub_b", ppt="2026-03-04")]
    splunk_data = {}

    class _FakeRes:
        parsed = date(2026, 3, 4)
        corrected = False

    html = _build_summary_html(records, splunk_data, eta_results_map={"sub_b": _FakeRes()})
    # The date should appear as Mar/4 (no leading zero)
    assert "Mar/4" in html


# ── _build_overdue_batch_html skip ───────────────────────────────────────────

def test_overdue_batch_skip_if_all_delivered():
    """Returns empty string when every field is marked delivered."""
    subsys_map = {
        "sub_x": {
            "ppt":     {"eta": "✅", "days_overdue": 0, "delivered": True},
            "netlist": {"eta": "✅", "days_overdue": 0, "delivered": True},
        }
    }
    html = _build_overdue_batch_html("Alice", subsys_map)
    assert html == "", f"Expected '', got: {html[:80]!r}"


def test_overdue_batch_sent_when_some_overdue():
    """Returns non-empty HTML when at least one field is overdue."""
    subsys_map = {
        "sub_y": {
            "ppt":     {"eta": "✅",       "days_overdue": 0,  "delivered": True},
            "netlist": {"eta": "2026/02/01","days_overdue": 10, "delivered": False},
        }
    }
    html = _build_overdue_batch_html("Bob", subsys_map)
    assert html != ""
    assert "<table" in html


# ── _gen_blank_eta_message ────────────────────────────────────────────────────

def test_blank_eta_no_table_no_llm():
    """Falls back to plain-text paragraph (no <table>) when llm=None."""
    subsys_map = {
        "top_par_wrap": {"netlist": "", "sdc": ""},
        "venc_top":     {"ccf": ""},
    }
    msg = asyncio.get_event_loop().run_until_complete(
        _gen_blank_eta_message("Alice", subsys_map, llm=None, excel_link="")
    )
    assert "<table" not in msg
    assert "top_par_wrap" in msg
    assert "netlist" in msg or "sdc" in msg
    assert "venc_top" in msg


def test_blank_eta_returns_empty_when_nothing_blank():
    """Returns '' when no fields are blank."""
    subsys_map = {"sub_z": {"netlist": "2026-03-10"}}
    msg = asyncio.get_event_loop().run_until_complete(
        _gen_blank_eta_message("Bob", subsys_map, llm=None, excel_link="")
    )
    assert msg == ""


# ── MockTeamsNotifier ─────────────────────────────────────────────────────────

def test_mock_notifier_has_llm_init():
    notifier = MockTeamsNotifier(llm=None)
    assert hasattr(notifier, "_llm")


def test_mock_notifier_post():
    notifier    = MockTeamsNotifier()
    rec         = _dummy_record("venc_top", ppt="done", netlist="v")
    splunk_data = {"venc_top": {"netlist": True, "sdc": False, "ccf": False, "upf": False}}

    # These are coroutines; call without await just checks they don't TypeError
    import inspect
    assert inspect.iscoroutinefunction(notifier.post_daily_summary)
    assert inspect.iscoroutinefunction(notifier.send_overdue_batch)
    assert inspect.iscoroutinefunction(notifier.send_blank_eta_batch)
    print("[test] MockTeamsNotifier interface check passed &#10003;")


if __name__ == "__main__":
    test_status_icon()
    test_ppt_icon()
    test_build_summary_html()
    test_build_summary_html_with_eta_map()
    test_overdue_batch_skip_if_all_delivered()
    test_overdue_batch_sent_when_some_overdue()
    test_blank_eta_no_table_no_llm()
    test_blank_eta_returns_empty_when_nothing_blank()
    test_mock_notifier_has_llm_init()
    test_mock_notifier_post()
    print("\nAll teams_notifier tests passed.")
