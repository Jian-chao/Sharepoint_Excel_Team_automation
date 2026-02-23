"""
tests/test_splunk_connector.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from modules.splunk_connector import MockSplunkConnector


def test_mock_query():
    sc = MockSplunkConnector()
    df = sc.query()
    print(f"\n[test] Mock Splunk DataFrame shape: {df.shape}")
    print(df.head())
    assert not df.empty
    assert "MODULE" in df.columns
    assert "SUB_GROUP" in df.columns
    assert "Viol" in df.columns
    assert "Timestamp" in df.columns


def test_get_latest_status_uploaded():
    sc = MockSplunkConnector()
    # svppsys_top_par_wrap has all 4 uploaded (Viol=1)
    status = sc.get_latest_status("svppsys_top_par_wrap")
    print(f"\n[test] svppsys_top_par_wrap status: {status}")
    assert status["netlist"] is True
    assert status["sdc"]     is True
    assert status["ccf"]     is True
    assert status["upf"]     is True


def test_get_latest_status_partial():
    sc = MockSplunkConnector()
    # venc_top_par_wrap has netlist=0, sdc=1, ccf=0, upf=1
    status = sc.get_latest_status("venc_top_par_wrap")
    print(f"\n[test] venc_top_par_wrap status: {status}")
    assert status["netlist"] is False
    assert status["sdc"]     is True
    assert status["ccf"]     is False
    assert status["upf"]     is True


def test_get_latest_status_not_found():
    sc = MockSplunkConnector()
    status = sc.get_latest_status("nonexistent_module")
    print(f"\n[test] nonexistent_module status: {status}")
    assert all(v is None for v in status.values())


if __name__ == "__main__":
    test_mock_query()
    test_get_latest_status_uploaded()
    test_get_latest_status_partial()
    test_get_latest_status_not_found()
    print("\n✅ All splunk_connector tests passed.")
