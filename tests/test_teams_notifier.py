"""
tests/test_teams_notifier.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from modules.teams_notifier import MockTeamsNotifier, _build_summary_html, _status_icon, _ppt_icon
from modules.excel_reader   import ExcelReader, SubsysRecord
from datetime import date


def _dummy_record(subsys="test_sub", fe="FE_user", bc="BC_user", be="BE_user",
                  ppt="", netlist="", sdc="", ccf="", upf=""):
    return SubsysRecord(
        row=4, subsys=subsys, fe=fe, bc=bc, be=be,
        ppt_status=ppt, netlist=netlist, sdc=sdc, ccf=ccf, upf=upf,
        release_date="", remarks="", skip=False, font_color_b="FF000000"
    )


def test_status_icon():
    assert "✅" in _status_icon("v")
    assert "➖" in _status_icon("x")
    assert "✅" in _status_icon("",  splunk_val=True)
    assert "❌" in _status_icon("",  splunk_val=False)
    assert "❓" in _status_icon("",  splunk_val=None)


def test_ppt_icon():
    d_past   = date(2020, 1, 1)
    d_future = date(2099, 1, 1)
    assert "✅" in _ppt_icon("done", d_future)
    assert "➖" in _ppt_icon("n/a",  d_future)
    assert "🔴" in _ppt_icon("",     d_past)    # overdue
    assert "❌" in _ppt_icon("",     d_future)   # missing but not yet overdue


def test_build_summary_html():
    records = [_dummy_record("sub_a", netlist="v", sdc="", ccf="x", upf="")]
    splunk_data = {"sub_a": {"netlist": True, "sdc": False, "ccf": None, "upf": True}}
    html = _build_summary_html(records, splunk_data)
    print(f"\n[test] summary HTML snippet:\n{html[:400]}")
    assert "<table" in html
    assert "sub_a"  in html
    assert "FDI DEF" in html


def test_mock_notifier_post(capsys=None):
    notifier = MockTeamsNotifier()
    rec = _dummy_record("venc_top", ppt="done", netlist="v")
    splunk_data = {"venc_top": {"netlist": True, "sdc": False, "ccf": False, "upf": False}}

    # Should not raise
    notifier.post_daily_summary([rec], splunk_data)
    notifier.send_eta_reminder(rec, "ppt", "2026-02-26")
    notifier.send_overdue_alert(rec, "sdc")
    print("[test] MockTeamsNotifier methods ran without error ✅")


def test_full_pipeline():
    """End-to-end: read Excel → mock Splunk → format Teams message."""
    from modules.splunk_connector import MockSplunkConnector
    reader  = ExcelReader()
    records = reader.get_all_records()
    splunk  = MockSplunkConnector()
    splunk_data = {r.subsys: splunk.get_latest_status(r.subsys) for r in records}
    notifier = MockTeamsNotifier()
    notifier.post_daily_summary(records, splunk_data)
    print("[test] Full pipeline test passed ✅")


if __name__ == "__main__":
    test_status_icon()
    test_ppt_icon()
    test_build_summary_html()
    test_mock_notifier_post()
    test_full_pipeline()
    print("\n✅ All teams_notifier tests passed.")
