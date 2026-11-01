"""Static test of recovery module safety and fixture contract."""
from pathlib import Path
import ast
def test_recovery_script_does_not_operate_on_user_tables():
    source = (Path(__file__).resolve().parents[1] / "examples" / "cloud_delta_recovery.py").read_text()
    tree = ast.parse(source)
    assert "DROP TABLE IF EXISTS " in source
    assert "umig6_recovery_" in source
    assert "CREATE DATABASE" not in source
    assert "TRUNCATE" not in source
    assert "UNPUBLISHED" in source and "PUBLISHED" in source
