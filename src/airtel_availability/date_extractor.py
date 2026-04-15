"""Extracts a business date from a filename.

Returns a (date, date_source) pair where date_source is 'filename' when a
pattern matched and 'mtime' when no pattern matched (caller supplies the mtime).
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class DateResult:
    date: dt.date | None         # None when nothing matched
    date_source: str             # 'filename' or 'mtime'


class DateExtractor:
    def __init__(self, compiled_patterns: list[re.Pattern]):
        self._patterns = compiled_patterns

    @classmethod
    def from_yaml(cls, path: str | Path) -> "DateExtractor":
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(data, dict) or "patterns" not in data:
            raise ValueError(f"date_patterns YAML at {path} must be a mapping with a 'patterns' key")
        compiled = [re.compile(p["regex"]) for p in data["patterns"]]
        return cls(compiled)

    def extract(self, basename: str) -> DateResult:
        for pat in self._patterns:
            m = pat.search(basename)
            if not m:
                continue
            try:
                d = dt.date(int(m.group("year")), int(m.group("month")), int(m.group("day")))
                return DateResult(date=d, date_source="filename")
            except ValueError:
                # e.g. Feb 30 — invalid calendar date; keep trying other patterns
                continue
        return DateResult(date=None, date_source="mtime")
