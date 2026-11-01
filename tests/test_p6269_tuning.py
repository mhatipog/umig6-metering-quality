"""Pure aggregation checks for the public P6269 performance experiment."""
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "examples"))
from p6269_tuning import aggregate_source, semantic_signature, stage_totals


def _category(seconds):
    return {
        "metrics": {
            "input_rows": "10540800", "accepted_rows": "10540800",
            "quarantined_rows": "0", "accepted_import_kwh": "1.250000000",
            "accepted_injection_kwh": "0.300000000",
        },
        "row_screen_seconds": seconds,
        "utc_label_coverage": {
            "profiles": 300, "incomplete": 300,
            "utc_instant_mapping_verified": False, "seconds": seconds + 2,
        },
        "local_clock_coverage": {
            "profiles": 300, "complete": 300,
            "invalid_slots": 0, "seconds": seconds + 3,
        },
    }


def test_signature_ignores_performance_but_not_data():
    a = {"categories": [_category(10), _category(5)]}
    b = {"categories": [_category(6), _category(20)]}
    assert semantic_signature(a) == semantic_signature(b)
    assert aggregate_source(a) == {
        "rows": 21081600, "passed": 21081600, "quarantined": 0,
        "profiles_local_complete": 600, "profiles_utc_labels_incomplete": 600,
        "import_kwh": str(Decimal("2.500000000")),
        "injection_kwh": str(Decimal("0.600000000")),
    }
    c = {"categories": [_category(6), _category(20)]}
    c["categories"][1]["utc_label_coverage"]["incomplete"] = 299
    assert semantic_signature(a) != semantic_signature(c)


def test_scope_of_timing():
    measured = stage_totals({"categories": [_category(1), _category(2)]})
    assert measured == {
        "row_screen_seconds": 3,
        "strict_utc_coverage_seconds": 7,
        "local_clock_coverage_seconds": 9,
    }
