"""
tests/test_excel_reader.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from modules.excel_reader import ExcelReader, _is_non_black


def test_parse_records():
    reader  = ExcelReader()
    records = reader.get_all_records(include_skipped=False)
    print(f"\n[test] Records returned: {len(records)}")
    for r in records:
        print(f"  Row {r.row}: {r.subsys!r:30s}  skip={r.skip}  font_color_B={r.font_color_b}")
        print(f"         ppt={r.ppt_status!r}  netlist={r.netlist!r}  sdc={r.sdc!r}  ccf={r.ccf!r}  upf={r.upf!r}")

    assert len(records) > 0, "Should return at least one record"
    subsys_names = [r.subsys for r in records]
    print(f"  Subsys list: {subsys_names}")

    # Row 7 (emi_infra_ext_par_wrap) should be skipped (grey font)
    assert "emi_infra_ext_par_wrap" not in subsys_names, \
        "emi_infra_ext_par_wrap should be skipped (grey font)"

    # First record should be venc_top_par_wrap
    assert records[0].subsys == "venc_top_par_wrap", f"Expected first subsys to be venc_top_par_wrap, got {records[0].subsys}"
    assert records[0].fe     == "Cbin Chen",         f"Expected FE=Cbin Chen, got {records[0].fe}"


def test_skip_logic_all_rows():
    """With include_skipped=True, verify skip flag is correctly set."""
    reader  = ExcelReader()
    records = reader.get_all_records(include_skipped=True)
    print(f"\n[test] All rows (incl. skipped): {len(records)}")
    for r in records:
        print(f"  Row {r.row}: {r.subsys!r:30s}  skip={r.skip}  font_color_B={r.font_color_b}")

    skipped = [r for r in records if r.skip]
    print(f"  Skipped rows: {[r.subsys for r in skipped]}")
    # There should be at least one skipped row
    assert len(skipped) >= 1, "Expected at least one skipped (grey-font) row"


def test_is_non_black():
    assert _is_non_black("FF000000") is False, "Black should not be non-black"
    assert _is_non_black("00000000") is False, "Default transparent should not be non-black"
    assert _is_non_black("FFD3D3D3") is True,  "Grey should be non-black"
    assert _is_non_black("FFBFBFBF") is True,  "Any non-black should return True"


def test_merged_cells():
    """G1:J1 merged — G2,H2,I2,J2 should all read the same master value."""
    reader = ExcelReader()
    # merged G1:J1 — master at G1
    val_g1 = reader._cell_value(1, 7)   # G1 master
    val_h1 = reader._cell_value(1, 8)   # H1 → resolves to G1
    val_i1 = reader._cell_value(1, 9)
    val_j1 = reader._cell_value(1, 10)
    print(f"\n[test] Merged G1:J1 → G1={val_g1!r}, H1={val_h1!r}, I1={val_i1!r}, J1={val_j1!r}")
    assert val_g1 == val_h1 == val_i1 == val_j1, "Merged G1:J1 cells should all return the same value"


def test_llm_prompt():
    reader = ExcelReader()
    prompt = reader.generate_llm_prompt("Test note: row 7 is out of scope for this release.")
    print(f"\n[test] LLM prompt length: {len(prompt)} chars")
    assert "EXCEL SHEET DESCRIPTION FOR LLM" in prompt
    assert "Test note:" in prompt
    assert "subsys" in prompt.lower()
    print("  First 300 chars:")
    print(prompt[:300])


if __name__ == "__main__":
    test_is_non_black()
    test_merged_cells()
    test_parse_records()
    test_skip_logic_all_rows()
    test_llm_prompt()
    print("\n✅ All excel_reader tests passed.")
