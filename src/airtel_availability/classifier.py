"""Source-pattern classifier.

Maps a (basename, full_path) pair to a source_key from a YAML catalog.
Classification rules (in priority order):
  1. If an entry declares path_hints, at least one hint must be a substring
     of the full path; entries with empty path_hints skip this gate.
  2. Patterns are tried in catalog order. If exactly one entry matches, it wins.
  3. If 2+ entries match, first-listed wins, confidence is downgraded to 'ambiguous'.
  4. If zero entries match, source_key is empty string, confidence 'unknown'.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import yaml


@dataclass
class CatalogEntry:
    key: str
    label: str
    patterns: list[str]                    # raw regex strings (case flags baked in via (?i))
    path_hints: list[str] = field(default_factory=list)
    compiled: list[re.Pattern] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self):
        self.compiled = [re.compile(p) for p in self.patterns]

    def matches(self, basename: str, full_path: str) -> bool:
        if self.path_hints and not any(h in full_path for h in self.path_hints):
            return False
        return any(p.search(basename) for p in self.compiled)


@dataclass(frozen=True)
class ClassifyResult:
    source_key: str
    source_label: str
    confidence: str                        # 'high' | 'ambiguous' | 'unknown'


class Classifier:
    def __init__(self, entries: Iterable[CatalogEntry]):
        self._entries = list(entries)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Classifier":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError(f"catalog YAML at {path} must be a top-level list of entries")
        entries = [
            CatalogEntry(
                key=e["key"],
                label=e["label"],
                patterns=e["patterns"],
                path_hints=e.get("path_hints", []) or [],
            )
            for e in raw
        ]
        return cls(entries)

    def classify(self, basename: str, full_path: str) -> ClassifyResult:
        matched = [e for e in self._entries if e.matches(basename, full_path)]
        if not matched:
            return ClassifyResult(source_key="", source_label="", confidence="unknown")
        winner = matched[0]
        conf = "ambiguous" if len(matched) > 1 else "high"
        return ClassifyResult(source_key=winner.key, source_label=winner.label, confidence=conf)
