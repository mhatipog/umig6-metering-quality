"""The reusable checked projection is intentionally small and explicit."""
from pathlib import Path
import ast
def test_shared_projection_contract():
    source=(Path(__file__).resolve().parents[1]/"examples"/"p6269_shared_preparation.py").read_text()
    ast.parse(source)
    assert '"Datum_Startuur"' in source
    assert '"interval_start_utc"' in source
    assert "stored.unpersist()" in source
    assert "cached_setup_seconds_included" in source
    assert "if actual != reference:" in source
