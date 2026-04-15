"""Layer 2 tests — filename → date extraction."""
import datetime as dt
import pathlib
import pytest
from airtel_availability.date_extractor import DateExtractor

# (basename, expected_date_or_None, expected_date_source)
CASES = [
    # YYYYMMDD embedded
    ("cbs_cdr_rch_20230412.add",            dt.date(2023, 4, 12),  "filename"),
    # YYYY-MM-DD
    ("cbs_cdr_rch_2023-04-12.csv.gz",       dt.date(2023, 4, 12),  "filename"),
    # YYYY_MM_DD
    ("cbs_cdr_rch_2023_04_12.add",          dt.date(2023, 4, 12),  "filename"),
    # YYYYMMDD followed by a seq suffix must still match
    ("cbs_cdr_rch_20230412_001.add",        dt.date(2023, 4, 12),  "filename"),
    # No date embedded → fall back to mtime
    ("cbs_cdr_rch.add",                     None,                  "mtime"),
    # DMY with dashes is supported
    ("12-04-2023_report.csv",               dt.date(2023, 4, 12),  "filename"),
    # Pre-1990 not expected but must parse if present (filter happens upstream)
    ("report_19991231.csv",                 dt.date(1999, 12, 31), "filename"),
    # Ambiguous 8-digit run that is NOT a valid YYYYMMDD must not be guessed
    ("12345678.csv",                         None,                 "mtime"),
    # 10-digit numeric that starts with what looks like a date must not over-match
    ("2023041299.csv",                       None,                 "mtime"),
    # Invalid month/day must not match
    ("report_20231301.csv",                  None,                 "mtime"),
    # Multi-date filename: first match wins (documents pattern-order behavior)
    ("20230412_to_20230413.csv",             dt.date(2023, 4, 12), "filename"),
    # 19xx-dashed is deliberately NOT supported by v1 seed patterns (see spec §4.2)
    ("1999-12-31_report.csv",                None,                 "mtime"),
    # Mixed separator in YMD-dashed is permitted by seed pattern (documents behavior)
    ("2023-04_12_report.csv",                dt.date(2023, 4, 12), "filename"),
]


@pytest.fixture(scope="module")
def extractor():
    root = pathlib.Path(__file__).parent.parent
    return DateExtractor.from_yaml(root / "config" / "date_patterns.yaml")


@pytest.mark.parametrize("basename,expected_date,expected_source", CASES)
def test_extract(extractor, basename, expected_date, expected_source):
    result = extractor.extract(basename)
    assert result.date == expected_date, f"{basename}: date"
    assert result.date_source == expected_source, f"{basename}: date_source"
