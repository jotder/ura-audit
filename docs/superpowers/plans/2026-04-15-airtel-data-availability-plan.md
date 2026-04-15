# Airtel URA Data Availability — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a two-part scanner (remote bash `find` + local Python pivot) that produces a `source × date` availability CSV for 19 Airtel data sources across a 5-year audit window, drawing from five messy filesystem roots on `192.168.30.24`.

**Architecture:** A bash script on the remote emits a raw file manifest. A local Python program pulls the manifest, classifies each row against a YAML pattern catalog, extracts a business date from the filename, pivots into an availability grid, and emits a DB-loadable CSV plus a human-readable xlsx. Classification and date extraction are local-only so a catalog fix never requires re-running the expensive remote scan.

**Tech Stack:** Python 3.11+, `pyyaml`, `openpyxl`, `paramiko`, `pytest`, GNU bash/find/tar/gzip (POSIX-standard, no extra deps on remote).

**Spec reference:** `docs/superpowers/specs/2026-04-15-airtel-data-availability-design.md`

**Environment assumptions:**
- Local host: Windows 11 with Git Bash (GNU coreutils available under bash).
- Remote host: `192.168.30.24:22`, user `gamma`. SSH + SFTP available. Credentials supplied via env var `AIRTEL_SSH_PASSWORD` or `~/.ssh/config`.
- Working directory: `C:\sandbox\URA\AIRTEL`. Not currently a git repo — Task 0 initializes it.

---

## File structure

```
C:\sandbox\URA\AIRTEL\
├── .gitignore                              # out/, __pycache__, .env, *.pyc
├── .python-version                          # 3.11
├── pyproject.toml                           # deps + pytest config
├── README.md                                # how to run + remote smoke procedure
├── config/
│   ├── catalog.yaml                         # 19 sources × regex patterns × embedded tests
│   └── date_patterns.yaml                   # date-regex catalog
├── src/airtel_availability/
│   ├── __init__.py
│   ├── scan.sh                              # remote-side POSIX bash; emits raw manifest
│   ├── runner.py                            # local orchestrator (SSH upload/run/pull + invoke pivot)
│   ├── pivot.py                             # local pipeline CLI (enrich manifest + write outputs)
│   ├── classifier.py                        # catalog-driven file → source_key mapper
│   ├── date_extractor.py                    # filename → (date, date_source)
│   ├── manifest_io.py                       # read raw TSV / write enriched TSV
│   ├── availability_writer.py               # writes availability.csv (the DB load target)
│   ├── unclassified_writer.py               # aggregates unknowns by signature → unclassified.csv
│   └── summary_writer.py                    # builds summary.xlsx with overview + per-source sheets
└── tests/
    ├── conftest.py
    ├── test_date_extractor.py               # Layer 2
    ├── test_classifier.py                   # Layer 1 (unit — small inline catalog)
    ├── test_catalog.py                      # Layer 1 (iterates real catalog.yaml `tests:`)
    ├── test_pivot_golden.py                 # Layer 3 — golden file diff
    ├── test_end_to_end_fake_tree.py         # Layer 4 — scan.sh + pivot on fake tree
    ├── test_reconciliation.py               # Layer 6 — post-run asserts
    └── fixtures/
        ├── tiny_manifest.tsv
        ├── tiny_availability.golden.csv
        ├── fake_tree/                       # synthesized locally at test-setup time
        └── fake_tree.golden.csv
```

Each src file has one clear responsibility. `pivot.py` is a thin CLI — all real logic lives in the focused modules. `scan.sh` is pure POSIX bash because the remote has no Python.

---

## Task 0: Project bootstrap

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\.gitignore`
- Create: `C:\sandbox\URA\AIRTEL\pyproject.toml`
- Create: `C:\sandbox\URA\AIRTEL\README.md`
- Create: `C:\sandbox\URA\AIRTEL\src\airtel_availability\__init__.py`
- Create: `C:\sandbox\URA\AIRTEL\tests\conftest.py`
- Create: `C:\sandbox\URA\AIRTEL\config\` (empty)

- [ ] **Step 1: Initialize git**

```bash
cd /c/sandbox/URA/AIRTEL
git init
git add docs/
git commit -m "chore: seed repo with existing docs and superpowers design spec"
```

Expected: `Initialized empty Git repository` and a first commit containing `docs/`.

- [ ] **Step 2: Create `.gitignore`**

Write to `C:\sandbox\URA\AIRTEL\.gitignore`:

```
# Run outputs — never committed
out/
tmp/

# Python
__pycache__/
*.pyc
*.pyo
.venv/
.pytest_cache/
*.egg-info/

# Secrets
.env
.env.*

# Editor
.idea/
.vscode/
```

- [ ] **Step 3: Create `pyproject.toml`**

Write to `C:\sandbox\URA\AIRTEL\pyproject.toml`:

```toml
[project]
name = "airtel-availability"
version = "0.1.0"
description = "Data availability scanner for the Airtel URA audit"
requires-python = ">=3.11"
dependencies = [
    "pyyaml>=6.0",
    "openpyxl>=3.1",
    "paramiko>=3.4",
]

[project.optional-dependencies]
dev = ["pytest>=8.0"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-q"

[tool.setuptools.packages.find]
where = ["src"]
```

- [ ] **Step 4: Create empty package + test skeletons**

```bash
cd /c/sandbox/URA/AIRTEL
mkdir -p src/airtel_availability tests/fixtures config
touch src/airtel_availability/__init__.py
touch tests/__init__.py
touch tests/conftest.py
```

Write to `C:\sandbox\URA\AIRTEL\tests\conftest.py`:

```python
"""Shared test fixtures."""
import sys
from pathlib import Path

# Make src/ importable without installing, and make the repo root importable
# so cross-test imports like `from tests.test_scan_sh import _build_fake_tree`
# work.
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))
```

- [ ] **Step 5: Create `README.md`**

Write to `C:\sandbox\URA\AIRTEL\README.md`:

```markdown
# Airtel URA Data Availability

Generates a `source × date` availability report across 19 Airtel data sources for a 5-year audit window.

## Install

```bash
python -m venv .venv
source .venv/Scripts/activate   # Git Bash on Windows
pip install -e '.[dev]'
```

## Run (full pipeline)

```bash
export AIRTEL_SSH_PASSWORD='...'
python -m airtel_availability.runner \
    --host 192.168.30.24 --user gamma \
    --window-start 2021-04-15 --window-end 2026-04-15 \
    --out out/
```

## Remote smoke test (Layer 5)

Before any full run, scan a single subdirectory:

```bash
python -m airtel_availability.runner \
    --host 192.168.30.24 --user gamma \
    --limit-subdir /master_data1/airtel_ftp/airtel_ftpu/2024/01 \
    --out out/smoke/
```

Review:
- `unclassified.csv` — is it dominated by unknowns? Add patterns to `config/catalog.yaml`.
- Extrapolate elapsed time × 60× for the full 5-year scan — tolerable?

## Run tests

```bash
pytest -q
```

See `docs/superpowers/specs/2026-04-15-airtel-data-availability-design.md` for the design spec.
```

- [ ] **Step 6: Verify setup works**

```bash
cd /c/sandbox/URA/AIRTEL
python -m venv .venv
source .venv/Scripts/activate
pip install -e '.[dev]'
pytest -q
```

Expected: `pytest` exits 0 with `no tests ran` (or similar — we haven't written any yet).

- [ ] **Step 7: Commit**

```bash
git add .gitignore pyproject.toml README.md src/ tests/ config/
git commit -m "chore: project skeleton + pyproject + test harness"
```

---

## Task 1: Date extractor

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\config\date_patterns.yaml`
- Create: `C:\sandbox\URA\AIRTEL\src\airtel_availability\date_extractor.py`
- Create: `C:\sandbox\URA\AIRTEL\tests\test_date_extractor.py`

- [ ] **Step 1: Write the failing test**

Write to `C:\sandbox\URA\AIRTEL\tests\test_date_extractor.py`:

```python
"""Layer 2 tests — filename → date extraction."""
import datetime as dt
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
]


@pytest.fixture(scope="module")
def extractor():
    return DateExtractor.from_yaml("config/date_patterns.yaml")


@pytest.mark.parametrize("basename,expected_date,expected_source", CASES)
def test_extract(extractor, basename, expected_date, expected_source):
    result = extractor.extract(basename)
    assert result.date == expected_date, f"{basename}: date"
    assert result.date_source == expected_source, f"{basename}: date_source"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_date_extractor.py -v
```

Expected: `ModuleNotFoundError: airtel_availability.date_extractor`.

- [ ] **Step 3: Create the date-patterns YAML**

Write to `C:\sandbox\URA\AIRTEL\config\date_patterns.yaml`:

```yaml
# Each pattern has named groups: year, month, day. First match wins.
# Lookarounds guard against spurious digit runs that include a valid subsequence.
patterns:
  - name: ymd_compact
    regex: '(?<![0-9])(?P<year>20\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])(?![0-9])'
  - name: ymd_dashed
    regex: '(?<![0-9])(?P<year>20\d{2})[-_](?P<month>0[1-9]|1[0-2])[-_](?P<day>0[1-9]|[12]\d|3[01])(?![0-9])'
  - name: ymd_19xx
    regex: '(?<![0-9])(?P<year>19\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])(?![0-9])'
  - name: dmy_dashed
    regex: '(?<![0-9])(?P<day>0[1-9]|[12]\d|3[01])[-_](?P<month>0[1-9]|1[0-2])[-_](?P<year>(?:19|20)\d{2})(?![0-9])'
```

- [ ] **Step 4: Implement `date_extractor.py`**

Write to `C:\sandbox\URA\AIRTEL\src\airtel_availability\date_extractor.py`:

```python
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
```

- [ ] **Step 5: Run test to verify it passes**

```bash
pytest tests/test_date_extractor.py -v
```

Expected: all 10 cases pass.

- [ ] **Step 6: Commit**

```bash
git add config/date_patterns.yaml src/airtel_availability/date_extractor.py tests/test_date_extractor.py
git commit -m "feat(date): filename date extractor with lookaround-guarded regex"
```

---

## Task 2: Classifier (core engine, small inline catalog)

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\src\airtel_availability\classifier.py`
- Create: `C:\sandbox\URA\AIRTEL\tests\test_classifier.py`

Goal here is to build the engine against a 3-entry inline catalog. Task 3 then expands to the full 19-source catalog.

- [ ] **Step 1: Write the failing test**

Write to `C:\sandbox\URA\AIRTEL\tests\test_classifier.py`:

```python
"""Layer 1 unit tests — classifier engine with a small inline catalog."""
import pytest
from airtel_availability.classifier import Classifier, CatalogEntry

INLINE_CATALOG = [
    CatalogEntry(
        key="air_recharge",
        label="AIR Recharge",
        patterns=[r"(?i)(^|[/_-])rch([_.-]|$)", r"(?i)cbs_cdr_rch_"],
        path_hints=[],
    ),
    CatalogEntry(
        key="momo_transactions",
        label="MoMo Transactions",
        patterns=[r"(?i)txn", r"(?i)transactions"],
        path_hints=["/airtel_mobile_money"],
    ),
    CatalogEntry(
        key="in_voice",
        label="IN Voice",
        patterns=[r"(?i)(^|[/_-])voi([_.-]|$)", r"(?i)cbs_cdr_voi_"],
        path_hints=[],
    ),
]


@pytest.fixture
def classifier():
    return Classifier(INLINE_CATALOG)


def test_exact_rch_match(classifier):
    r = classifier.classify("cbs_cdr_rch_20230412.add", "/master_data1/x/y/cbs_cdr_rch_20230412.add")
    assert r.source_key == "air_recharge"
    assert r.confidence == "high"


def test_path_hint_required_for_momo(classifier):
    # 'txn' pattern matches, but path hint requires /airtel_mobile_money
    r1 = classifier.classify("txn_20230412.csv", "/master_data1/x/txn_20230412.csv")
    assert r1.source_key == ""      # path hint not satisfied → no match
    r2 = classifier.classify("txn_20230412.csv", "/airtel_mobile_money/2023/txn_20230412.csv")
    assert r2.source_key == "momo_transactions"


def test_no_match_returns_empty(classifier):
    r = classifier.classify("random_file_xyz.csv", "/master_data1/random_file_xyz.csv")
    assert r.source_key == ""
    assert r.confidence == "unknown"


def test_ambiguous_first_wins(classifier):
    # Contrived: a filename containing both 'rch' and 'voi' tokens
    r = classifier.classify("rch_voi_bundle.csv", "/master_data1/rch_voi_bundle.csv")
    assert r.source_key == "air_recharge"        # first in catalog wins
    assert r.confidence == "ambiguous"


def test_case_insensitive(classifier):
    r = classifier.classify("CBS_CDR_RCH_20230412.ADD", "/master_data1/CBS_CDR_RCH_20230412.ADD")
    assert r.source_key == "air_recharge"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_classifier.py -v
```

Expected: `ModuleNotFoundError: airtel_availability.classifier`.

- [ ] **Step 3: Implement `classifier.py`**

Write to `C:\sandbox\URA\AIRTEL\src\airtel_availability\classifier.py`:

```python
"""Source-pattern classifier.

Maps a (basename, full_path) pair to a source_key from a YAML catalog.
Classification rules (in priority order):
  1. path_hints must all be substrings of the full path (or the entry's
     path_hints must be empty) before any pattern is considered.
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_classifier.py -v
```

Expected: 5/5 pass.

- [ ] **Step 5: Commit**

```bash
git add src/airtel_availability/classifier.py tests/test_classifier.py
git commit -m "feat(classifier): pattern + path-hint classification engine with ambiguity tracking"
```

---

## Task 3: Full seed catalog (19 sources with embedded tests)

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\config\catalog.yaml`
- Create: `C:\sandbox\URA\AIRTEL\tests\test_catalog.py`

This task codifies the entire spec §4.2 seed catalog plus embedded test cases per source. It also wires up the Catalog→test harness so that adding a new source requires adding its test cases.

- [ ] **Step 1: Write the failing test runner**

Write to `C:\sandbox\URA\AIRTEL\tests\test_catalog.py`:

```python
"""Layer 1 tests — runs every `tests:` case embedded in the real catalog.yaml."""
from pathlib import Path

import pytest
import yaml

from airtel_availability.classifier import Classifier


CATALOG_PATH = Path("config/catalog.yaml")


def _load_cases():
    data = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    cases = []
    for entry in data:
        for case in entry.get("tests", []):
            cases.append(
                pytest.param(
                    case["name"],
                    case.get("path", f"/master_data1/synthetic/{case['name']}"),
                    case["expect"],
                    id=f"{entry['key']}::{case['name']}",
                )
            )
    return cases


@pytest.fixture(scope="module")
def classifier():
    return Classifier.from_yaml(CATALOG_PATH)


@pytest.mark.parametrize("name,path,expected_key", _load_cases())
def test_catalog_entry(classifier, name, path, expected_key):
    result = classifier.classify(name, path)
    assert result.source_key == expected_key, (
        f"expected {expected_key!r}, got {result.source_key!r} (confidence={result.confidence})"
    )


def test_at_least_nineteen_sources():
    data = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    assert len(data) == 19, f"spec requires 19 sources, catalog has {len(data)}"


def test_every_source_has_tests():
    data = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    for entry in data:
        assert entry.get("tests"), f"catalog entry {entry['key']} has no tests:"
        assert len(entry["tests"]) >= 3, (
            f"catalog entry {entry['key']} has {len(entry['tests'])} tests; require ≥3"
        )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_catalog.py -v
```

Expected: `FileNotFoundError: config/catalog.yaml`.

- [ ] **Step 3: Write the full catalog YAML**

Write to `C:\sandbox\URA\AIRTEL\config\catalog.yaml`:

```yaml
# Source-pattern catalog. Order matters: first-listed entry wins on ambiguous match.
# Each entry MUST carry >=3 `tests:` cases (positive and negative) — enforced by test_catalog.py.

- key: air_recharge
  label: "AIR Recharge"
  patterns:
    - '(?i)(^|[/_-])rch([_.-]|$)'
    - '(?i)cbs_cdr_rch_'
    - '(?i)ocs_rch_'
    - '(?i)recharge'
  path_hints: []
  tests:
    - {name: "cbs_cdr_rch_20230412.add",             expect: air_recharge}
    - {name: "CBS_CDR_RCH_20230412.csv.gz",          expect: air_recharge}
    - {name: "ocs_rch_20230412.tar.gz",              expect: air_recharge}
    - {name: "random_report.csv",                    expect: ""}

- key: air_subscription
  label: "AIR Subscription"
  patterns:
    - '(?i)(^|[/_-])sub([_.-]|$)'
    - '(?i)cbs_cdr_sub_'
    - '(?i)subscription'
  path_hints: []
  tests:
    - {name: "cbs_cdr_sub_20230412.add",             expect: air_subscription}
    - {name: "subscription_20230412.csv",            expect: air_subscription}
    - {name: "submarine_cable.csv",                  expect: ""}   # 'sub' not at token boundary

- key: air_adjustment
  label: "AIR Adjustment"
  patterns:
    - '(?i)(^|[/_-])adj([_.-]|$)'
    - '(?i)cbs_cdr_adj_'
    - '(?i)(^|[/_-])mon([_.-]|$)'          # monetary adjustment CDR
    - '(?i)adjustment'
  path_hints: []
  tests:
    - {name: "cbs_cdr_adj_20230412.add",             expect: air_adjustment}
    - {name: "mon_20230412.csv.gz",                  expect: air_adjustment}
    - {name: "monday_summary.csv",                   expect: ""}   # 'mon' not at token boundary

- key: me2u_transfer
  label: "Me2U / Transfer"
  patterns:
    - '(?i)me2u'
    - '(?i)(^|[/_-])trf([_.-]|$)'
    - '(?i)transfer'
    - '(?i)cbs_cdr_trf_'
  path_hints: []
  tests:
    - {name: "me2u_20230412.csv",                    expect: me2u_transfer}
    - {name: "cbs_cdr_trf_20230412.add",             expect: me2u_transfer}
    - {name: "transfer_log_20230412.csv",            expect: me2u_transfer}
    - {name: "trfc_report.csv",                      expect: ""}   # 'trf' not at token boundary

- key: sdp
  label: "SDP"
  patterns:
    - '(?i)(^|[/_-])sdp([_.-]|$)'
    - '(?i)cbs_cdr_sdp_'
  path_hints: []
  tests:
    - {name: "sdp_20230412.csv",                     expect: sdp}
    - {name: "cbs_cdr_sdp_20230412.add",             expect: sdp}
    - {name: "nsdp_20230412.csv",                    expect: ""}   # 'sdp' not at token boundary

- key: momo_transactions
  label: "Mobile Money Transactions"
  patterns:
    - '(?i)(^|[/_-])(txn|trans|transactions)([_.-]|$)'
  path_hints: ["/airtel_mobile_money"]
  tests:
    - {name: "txn_20230412.csv",  path: "/airtel_mobile_money/2023/txn_20230412.csv",  expect: momo_transactions}
    - {name: "transactions_20230412.csv", path: "/airtel_mobile_money/trans/transactions_20230412.csv", expect: momo_transactions}
    - {name: "txn_20230412.csv",  path: "/master_data1/txn_20230412.csv",              expect: ""}   # path hint fails

- key: momo_balance_snapshot
  label: "Mobile Money Balance Snapshot"
  patterns:
    - '(?i)(^|[/_-])(bal|balance|snapshot|snap)([_.-]|$)'
  path_hints: ["/airtel_mobile_money"]
  tests:
    - {name: "bal_20230412.csv",      path: "/airtel_mobile_money/bal_20230412.csv",      expect: momo_balance_snapshot}
    - {name: "snapshot_20230412.csv", path: "/airtel_mobile_money/snapshot_20230412.csv", expect: momo_balance_snapshot}
    - {name: "bal_20230412.csv",      path: "/master_data1/bal_20230412.csv",             expect: ""}

- key: vms_physical
  label: "VMS Physical"
  patterns:
    - '(?i)vou[^/]*phys'
    - '(?i)physical[^/]*vou'
    - '(?i)(^|[/_-])pvou([_.-]|$)'
    - '(?i)vms[_-]?p([_.-]|$)'
  path_hints: []
  tests:
    - {name: "vou_physical_20230412.csv",            expect: vms_physical}
    - {name: "physical_vou_20230412.csv",            expect: vms_physical}
    - {name: "pvou_20230412.csv",                    expect: vms_physical}
    - {name: "vou_electronic_20230412.csv",          expect: vms_electronic}   # should go to VMS-E, not VMS-P

- key: vms_electronic
  label: "VMS Electronic"
  patterns:
    - '(?i)vou[^/]*elec'
    - '(?i)(^|[/_-])evou([_.-]|$)'
    - '(?i)vms[_-]?e([_.-]|$)'
    - '(?i)cbs_cdr_vou_'
    - '(?i)(^|[/_-])vou([_.-]|$)'       # generic — last resort for VOU
  path_hints: []
  tests:
    - {name: "vou_electronic_20230412.csv",          expect: vms_electronic}
    - {name: "cbs_cdr_vou_20230412.add",             expect: vms_electronic}
    - {name: "evou_20230412.csv",                    expect: vms_electronic}
    - {name: "voucher_bundle.csv",                   expect: ""}   # 'vou' not at token boundary

- key: dealer_management
  label: "Dealer Management System"
  patterns:
    - '(?i)(^|[/_-])dms([_.-]|$)'
    - '(?i)dealer'
  path_hints: []
  tests:
    - {name: "dms_20230412.csv",                     expect: dealer_management}
    - {name: "dealer_report_20230412.csv",           expect: dealer_management}
    - {name: "admsx_20230412.csv",                   expect: ""}   # 'dms' not at token boundary

- key: bank_momo_float
  label: "Bank Statement Holding MoMo Float"
  patterns:
    - '(?i)(^|[/_-])bank([_.-]|$)'
    - '(?i)(^|[/_-])(float|stmt|statement)([_.-]|$)'
  path_hints: []
  tests:
    - {name: "bank_stmt_20230412.csv",               expect: bank_momo_float}
    - {name: "float_20230412.csv",                   expect: bank_momo_float}
    - {name: "statement_20230412.csv",               expect: bank_momo_float}
    - {name: "banking_news.csv",                     expect: ""}

- key: hlr
  label: "HLR"
  patterns:
    - '(?i)(^|[/_-])hlr([_.-]|$)'
  path_hints: []
  tests:
    - {name: "hlr_20230412.csv",                     expect: hlr}
    - {name: "HLR_dump_20230412.add",                expect: hlr}
    - {name: "ahlrx_20230412.csv",                   expect: ""}

- key: in_voice
  label: "IN Voice"
  patterns:
    - '(?i)(^|[/_-])voi([_.-]|$)'
    - '(?i)cbs_cdr_voi_'
    - '(?i)ocs[^/]*voi'
    - '(?i)(^|[/_-])voice([_.-]|$)'
  path_hints: []
  tests:
    - {name: "cbs_cdr_voi_20230412.add",             expect: in_voice}
    - {name: "voi_20230412.csv.gz",                  expect: in_voice}
    - {name: "voice_cdr_20230412.csv",               expect: in_voice}
    - {name: "void_list.csv",                        expect: ""}   # 'voi' not at token boundary

- key: in_sms
  label: "IN SMS"
  patterns:
    - '(?i)(^|[/_-])sms([_.-]|$)'
    - '(?i)cbs_cdr_sms_'
  path_hints: []
  tests:
    - {name: "cbs_cdr_sms_20230412.add",             expect: in_sms}
    - {name: "sms_20230412.csv",                     expect: in_sms}
    - {name: "psms_20230412.csv",                    expect: vas_other}   # should be classified as VAS, not IN SMS

- key: in_gprs
  label: "IN GPRS"
  patterns:
    - '(?i)(^|[/_-])gprs([_.-]|$)'
    - '(?i)cbs_cdr_gprs_'
    - '(?i)data_cdr'
  path_hints: []
  tests:
    - {name: "cbs_cdr_gprs_20230412.add",            expect: in_gprs}
    - {name: "gprs_20230412.csv.gz",                 expect: in_gprs}
    - {name: "data_cdr_20230412.csv",                expect: in_gprs}
    - {name: "gprst_report.csv",                     expect: ""}

- key: vas_other
  label: "VAS (PSMS, RBT, MMS, etc.)"
  patterns:
    - '(?i)(^|[/_-])(psms|rbt|mms|vas)([_.-]|$)'
  path_hints: []
  tests:
    - {name: "psms_20230412.csv",                    expect: vas_other}
    - {name: "rbt_20230412.csv",                     expect: vas_other}
    - {name: "mms_20230412.csv",                     expect: vas_other}
    - {name: "vas_bundle.csv",                       expect: vas_other}

- key: msc
  label: "MSC"
  patterns:
    - '(?i)(^|[/_-])msc([_.-]|$)'
  path_hints: []
  tests:
    - {name: "msc_20230412.csv",                     expect: msc}
    - {name: "msc_dump_20230412.add",                expect: msc}
    - {name: "amsca_report.csv",                     expect: ""}

- key: postpaid_billing
  label: "Postpaid Billing / Invoicing"
  patterns:
    - '(?i)postpaid[^/]*(inv|bill)'
    - '(?i)(^|[/_-])pp[^/]*inv'
    - '(?i)(^|[/_-])invoice([_.-]|$)'
  path_hints: []
  tests:
    - {name: "postpaid_invoice_20230412.csv",        expect: postpaid_billing}
    - {name: "postpaid_billing_20230412.csv",        expect: postpaid_billing}
    - {name: "pp_inv_20230412.csv",                  expect: postpaid_billing}
    - {name: "invoice_20230412.csv",                 expect: postpaid_billing}

- key: postpaid_payments
  label: "Postpaid Payments"
  patterns:
    - '(?i)postpaid[^/]*pay'
    - '(?i)(^|[/_-])pp[^/]*pay'
    - '(?i)(^|[/_-])payment([_.-]|$)'
  path_hints: []
  tests:
    - {name: "postpaid_payments_20230412.csv",       expect: postpaid_payments}
    - {name: "pp_pay_20230412.csv",                  expect: postpaid_payments}
    - {name: "payment_20230412.csv",                 expect: postpaid_payments}
    - {name: "paypal_invoice.csv",                   expect: ""}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_catalog.py -v
```

Expected: every parametrized test passes; `test_at_least_nineteen_sources` and `test_every_source_has_tests` pass.

If any test fails, the pattern is wrong (or the test case is wrong). Fix the YAML and rerun — never disable a test to go green.

- [ ] **Step 5: Commit**

```bash
git add config/catalog.yaml tests/test_catalog.py
git commit -m "feat(catalog): seed 19-source catalog with embedded test cases"
```

---

## Task 4: Raw manifest I/O

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\src\airtel_availability\manifest_io.py`
- Create: `C:\sandbox\URA\AIRTEL\tests\test_manifest_io.py`

- [ ] **Step 1: Write the failing test**

Write to `C:\sandbox\URA\AIRTEL\tests\test_manifest_io.py`:

```python
"""Manifest reader/writer tests."""
from pathlib import Path

import pytest

from airtel_availability.manifest_io import ManifestRow, read_raw_manifest, write_enriched_manifest, EnrichedRow


RAW_TSV = """\
path\torigin\tsize\tmtime\tarchive_parent\tarchive_entry
/master_data1/airtel_ftp/airtel_ftpu/2023/04/cbs_cdr_rch_20230412.add\tmaster_data1\t1234\t1681286400\t\t
/airtel_mobile_money/2023/04/txn_20230412.csv\tairtel_mobile_money\t5678\t1681286401\t\t
/resubmited_data/bundle.tar.gz\tresubmited\t999999\t1681300000\t/resubmited_data/bundle.tar.gz\tcbs_cdr_voi_20230412.add
"""


def test_read_raw_manifest(tmp_path):
    raw = tmp_path / "manifest.raw.tsv"
    raw.write_text(RAW_TSV, encoding="utf-8")
    rows = list(read_raw_manifest(raw))
    assert len(rows) == 3
    assert rows[0].origin == "master_data1"
    assert rows[0].size == 1234
    assert rows[2].archive_parent.endswith("bundle.tar.gz")
    assert rows[2].archive_entry == "cbs_cdr_voi_20230412.add"


def test_write_enriched_manifest_roundtrip(tmp_path):
    out = tmp_path / "manifest.tsv"
    enriched = [
        EnrichedRow(
            path="/x/cbs_cdr_rch_20230412.add",
            origin="master_data1",
            size=1234,
            mtime=1681286400,
            archive_parent="",
            archive_entry="",
            source_key="air_recharge",
            confidence="high",
            date="2023-04-12",
            date_source="filename",
        )
    ]
    write_enriched_manifest(out, enriched)
    text = out.read_text(encoding="utf-8").splitlines()
    assert text[0].startswith("path\torigin\tsize\tmtime")
    assert "source_key" in text[0]
    assert "air_recharge" in text[1]
    assert "\t" in text[1] and "," not in text[1]    # TSV not CSV
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_manifest_io.py -v
```

Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement `manifest_io.py`**

Write to `C:\sandbox\URA\AIRTEL\src\airtel_availability\manifest_io.py`:

```python
"""Raw and enriched manifest I/O.

Raw manifest columns (written by scan.sh, read here):
    path, origin, size, mtime, archive_parent, archive_entry

Enriched manifest columns (written by pivot.py):
    raw columns + source_key, confidence, date (YYYY-MM-DD or ''), date_source
"""
from __future__ import annotations

import csv
from dataclasses import dataclass, asdict, fields
from pathlib import Path
from typing import Iterable, Iterator


RAW_HEADERS = ["path", "origin", "size", "mtime", "archive_parent", "archive_entry"]
ENRICHED_HEADERS = RAW_HEADERS + ["source_key", "confidence", "date", "date_source"]


@dataclass(frozen=True)
class ManifestRow:
    path: str
    origin: str
    size: int
    mtime: int
    archive_parent: str
    archive_entry: str


@dataclass(frozen=True)
class EnrichedRow:
    path: str
    origin: str
    size: int
    mtime: int
    archive_parent: str
    archive_entry: str
    source_key: str
    confidence: str
    date: str            # YYYY-MM-DD or empty string
    date_source: str


def read_raw_manifest(path: str | Path) -> Iterator[ManifestRow]:
    with Path(path).open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        missing = set(RAW_HEADERS) - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"raw manifest missing columns: {missing}")
        for row in reader:
            yield ManifestRow(
                path=row["path"],
                origin=row["origin"],
                size=int(row["size"] or 0),
                mtime=int(row["mtime"] or 0),
                archive_parent=row["archive_parent"] or "",
                archive_entry=row["archive_entry"] or "",
            )


def write_enriched_manifest(path: str | Path, rows: Iterable[EnrichedRow]) -> None:
    with Path(path).open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ENRICHED_HEADERS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for r in rows:
            writer.writerow(asdict(r))
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_manifest_io.py -v
```

Expected: 2/2 pass.

- [ ] **Step 5: Commit**

```bash
git add src/airtel_availability/manifest_io.py tests/test_manifest_io.py
git commit -m "feat(manifest): raw/enriched TSV reader + writer with dataclass rows"
```

---

## Task 5: Availability writer (the pivot)

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\src\airtel_availability\availability_writer.py`
- Create: `C:\sandbox\URA\AIRTEL\tests\fixtures\tiny_manifest.tsv`
- Create: `C:\sandbox\URA\AIRTEL\tests\fixtures\tiny_availability.golden.csv`
- Create: `C:\sandbox\URA\AIRTEL\tests\test_pivot_golden.py`

- [ ] **Step 1: Write the failing test**

Write to `C:\sandbox\URA\AIRTEL\tests\test_pivot_golden.py`:

```python
"""Layer 3 — pivot golden-file test."""
import datetime as dt
from pathlib import Path

from airtel_availability.availability_writer import pivot_to_availability


FIXTURES = Path("tests/fixtures")


def _normalize(text: str) -> str:
    """CRLF/LF agnostic comparison, strip trailing blank lines."""
    return "\n".join(line.rstrip("\r") for line in text.splitlines()).rstrip() + "\n"


def test_pivot_golden(tmp_path):
    manifest = FIXTURES / "tiny_manifest.tsv"
    golden = FIXTURES / "tiny_availability.golden.csv"
    out = tmp_path / "availability.csv"

    pivot_to_availability(
        manifest_path=manifest,
        out_path=out,
        run_id="2026-04-15T00-00-00Z",
        window_start=dt.date(2023, 4, 1),
        window_end=dt.date(2023, 4, 5),
    )

    actual = _normalize(out.read_text(encoding="utf-8"))
    expected = _normalize(golden.read_text(encoding="utf-8"))
    assert actual == expected, f"\n--- ACTUAL ---\n{actual}\n--- EXPECTED ---\n{expected}"
```

- [ ] **Step 2: Build the tiny enriched manifest fixture**

The tiny manifest has the *enriched* columns already filled in — pivot is tested in isolation from classification. (End-to-end flow is tested in Task 9.) The fixture has 4 source×date combinations in the window plus 1 out-of-window row (must be excluded).

Write to `C:\sandbox\URA\AIRTEL\tests\fixtures\tiny_manifest.tsv` (use actual tab characters between fields — most editors will preserve these; verify with `cat -A`):

```
path	origin	size	mtime	archive_parent	archive_entry	source_key	confidence	date	date_source
/master_data1/cbs_cdr_rch_20230401.add	master_data1	1000	1680307200			air_recharge	high	2023-04-01	filename
/master_data1/cbs_cdr_rch_20230401_part2.add	master_data1	500	1680307300			air_recharge	high	2023-04-01	filename
/resubmited_data/cbs_cdr_rch_20230401.add	resubmited	1000	1681000000			air_recharge	high	2023-04-01	filename
/master_data1/cbs_cdr_voi_20230402.add	master_data1	2000	1680393600			in_voice	high	2023-04-02	filename
/airtel_mobile_money/txn_20230403.csv	airtel_mobile_money	3000	1680480000			momo_transactions	high	2023-04-03	filename
/master_data2/cbs_cdr_rch_mtimeonly.add	master_data2	800	1680307200			air_recharge	high	2023-04-01	mtime
/master_data1/cbs_cdr_rch_20220101.add	master_data1	1200	1641000000			air_recharge	high	2022-01-01	filename
```

- [ ] **Step 3: Build the expected golden CSV**

Compute by hand from the fixture, for window 2023-04-01 → 2023-04-05. Every source × date in window gets a row (including zero-file rows).

Sources present in the tiny-manifest catalog: for the test, use exactly the three keys that appear (`air_recharge`, `in_voice`, `momo_transactions`). The pivot is driven by a `sources` arg in the next step; for this fixture test we pass those three.

Write to `C:\sandbox\URA\AIRTEL\tests\fixtures\tiny_availability.golden.csv`:

```
run_id,source_key,source_label,date,files_total,bytes_total,files_master_data1,files_master_data2,files_mobile_money,files_resubmited,files_unique_trnx,bytes_master_data1,bytes_master_data2,bytes_mobile_money,bytes_resubmited,bytes_unique_trnx,has_archive,date_confidence,min_mtime,max_mtime
2026-04-15T00-00-00Z,air_recharge,AIR Recharge,2023-04-01,4,3300,2,1,0,1,0,1500,800,0,1000,0,False,mixed,2023-04-01T00:00:00+00:00,2023-04-08T21:46:40+00:00
2026-04-15T00-00-00Z,air_recharge,AIR Recharge,2023-04-02,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,air_recharge,AIR Recharge,2023-04-03,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,air_recharge,AIR Recharge,2023-04-04,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,air_recharge,AIR Recharge,2023-04-05,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,in_voice,IN Voice,2023-04-01,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,in_voice,IN Voice,2023-04-02,1,2000,1,0,0,0,0,2000,0,0,0,0,False,filename,2023-04-02T00:00:00+00:00,2023-04-02T00:00:00+00:00
2026-04-15T00-00-00Z,in_voice,IN Voice,2023-04-03,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,in_voice,IN Voice,2023-04-04,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,in_voice,IN Voice,2023-04-05,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,momo_transactions,Mobile Money Transactions,2023-04-01,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,momo_transactions,Mobile Money Transactions,2023-04-02,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,momo_transactions,Mobile Money Transactions,2023-04-03,1,3000,0,0,1,0,0,0,0,3000,0,0,False,filename,2023-04-03T00:00:00+00:00,2023-04-03T00:00:00+00:00
2026-04-15T00-00-00Z,momo_transactions,Mobile Money Transactions,2023-04-04,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
2026-04-15T00-00-00Z,momo_transactions,Mobile Money Transactions,2023-04-05,0,0,0,0,0,0,0,0,0,0,0,0,False,,,
```

Note: the `mtime` epoch values in the fixture convert to the UTC timestamps above. If the engineer's tests produce different timestamps, verify timezone handling (must use UTC).

- [ ] **Step 4: Implement `availability_writer.py`**

Write to `C:\sandbox\URA\AIRTEL\src\airtel_availability\availability_writer.py`:

```python
"""Pivots an enriched manifest into the source × date availability CSV."""
from __future__ import annotations

import csv
import datetime as dt
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from .manifest_io import EnrichedRow, ENRICHED_HEADERS
from .classifier import Classifier


# Canonical origin tags (match scan.sh's output for the `origin` column)
ORIGINS = ["master_data1", "master_data2", "airtel_mobile_money", "resubmited", "unique_trnx"]
ORIGIN_COL_SUFFIX = {
    "master_data1":        "master_data1",
    "master_data2":        "master_data2",
    "airtel_mobile_money": "mobile_money",
    "resubmited":          "resubmited",
    "unique_trnx":         "unique_trnx",
}


AVAILABILITY_HEADERS = [
    "run_id", "source_key", "source_label", "date",
    "files_total", "bytes_total",
    "files_master_data1", "files_master_data2", "files_mobile_money", "files_resubmited", "files_unique_trnx",
    "bytes_master_data1", "bytes_master_data2", "bytes_mobile_money", "bytes_resubmited", "bytes_unique_trnx",
    "has_archive", "date_confidence", "min_mtime", "max_mtime",
]


def _read_enriched(path: str | Path) -> Iterable[EnrichedRow]:
    with Path(path).open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            yield EnrichedRow(
                path=row["path"],
                origin=row["origin"],
                size=int(row["size"] or 0),
                mtime=int(row["mtime"] or 0),
                archive_parent=row["archive_parent"] or "",
                archive_entry=row["archive_entry"] or "",
                source_key=row["source_key"] or "",
                confidence=row["confidence"] or "unknown",
                date=row["date"] or "",
                date_source=row["date_source"] or "",
            )


def _daterange(start: dt.date, end: dt.date):
    d = start
    one = dt.timedelta(days=1)
    while d <= end:
        yield d
        d += one


def _sources_from_catalog(catalog_path: str | Path | None) -> list[tuple[str, str]]:
    if catalog_path is None:
        return []
    cl = Classifier.from_yaml(catalog_path)
    return [(e.key, e.label) for e in cl._entries]     # noqa: SLF001 — intentional


def pivot_to_availability(
    *,
    manifest_path: str | Path,
    out_path: str | Path,
    run_id: str,
    window_start: dt.date,
    window_end: dt.date,
    catalog_path: str | Path | None = None,
    sources: list[tuple[str, str]] | None = None,
) -> None:
    """Write availability.csv to out_path.

    Exactly one of catalog_path or sources must be supplied. When catalog_path
    is given, the full 19-source grid is emitted; when sources is given (test
    path), only those sources are emitted.
    """
    if sources is None:
        sources = _sources_from_catalog(catalog_path)
    if not sources:
        raise ValueError("either catalog_path or sources must be supplied and non-empty")

    # Bucket enriched rows by (source_key, date). Ignore out-of-window & unknown.
    bucket = defaultdict(lambda: {
        **{f"files_{suf}": 0 for suf in ORIGIN_COL_SUFFIX.values()},
        **{f"bytes_{suf}": 0 for suf in ORIGIN_COL_SUFFIX.values()},
        "has_archive": False,
        "date_sources": set(),        # collected for date_confidence roll-up
        "min_mtime": None,
        "max_mtime": None,
    })

    for row in _read_enriched(manifest_path):
        if not row.source_key or not row.date:
            continue
        try:
            d = dt.date.fromisoformat(row.date)
        except ValueError:
            continue
        if not (window_start <= d <= window_end):
            continue

        suf = ORIGIN_COL_SUFFIX.get(row.origin)
        if suf is None:
            continue      # unknown origin → ignore

        agg = bucket[(row.source_key, d)]
        agg[f"files_{suf}"] += 1
        agg[f"bytes_{suf}"] += row.size
        if row.archive_parent:
            agg["has_archive"] = True
        agg["date_sources"].add(row.date_source)
        if agg["min_mtime"] is None or row.mtime < agg["min_mtime"]:
            agg["min_mtime"] = row.mtime
        if agg["max_mtime"] is None or row.mtime > agg["max_mtime"]:
            agg["max_mtime"] = row.mtime

    # Emit full grid.
    with Path(out_path).open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=AVAILABILITY_HEADERS, lineterminator="\n")
        writer.writeheader()
        for (skey, slabel) in sources:
            for d in _daterange(window_start, window_end):
                agg = bucket.get((skey, d))
                row = {
                    "run_id": run_id,
                    "source_key": skey,
                    "source_label": slabel,
                    "date": d.isoformat(),
                }
                if agg is None:
                    # zero-file day: still emit a row
                    for suf in ORIGIN_COL_SUFFIX.values():
                        row[f"files_{suf}"] = 0
                        row[f"bytes_{suf}"] = 0
                    row["files_total"] = 0
                    row["bytes_total"] = 0
                    row["has_archive"] = False
                    row["date_confidence"] = ""
                    row["min_mtime"] = ""
                    row["max_mtime"] = ""
                else:
                    files_total = sum(agg[f"files_{suf}"] for suf in ORIGIN_COL_SUFFIX.values())
                    bytes_total = sum(agg[f"bytes_{suf}"] for suf in ORIGIN_COL_SUFFIX.values())
                    ds = agg["date_sources"]
                    if ds == {"filename"}:
                        conf = "filename"
                    elif ds == {"mtime"}:
                        conf = "mtime"
                    else:
                        conf = "mixed"
                    for suf in ORIGIN_COL_SUFFIX.values():
                        row[f"files_{suf}"] = agg[f"files_{suf}"]
                        row[f"bytes_{suf}"] = agg[f"bytes_{suf}"]
                    row["files_total"] = files_total
                    row["bytes_total"] = bytes_total
                    row["has_archive"] = agg["has_archive"]
                    row["date_confidence"] = conf
                    row["min_mtime"] = dt.datetime.fromtimestamp(agg["min_mtime"], tz=dt.timezone.utc).isoformat()
                    row["max_mtime"] = dt.datetime.fromtimestamp(agg["max_mtime"], tz=dt.timezone.utc).isoformat()
                writer.writerow(row)
```

Note: the test passes `sources=[...]` directly via the `pivot_to_availability(..., sources=[(k, l), ...])` arg. Tweak the test to pass those three sources:

Update `tests/test_pivot_golden.py` Step 1 to call:
```python
pivot_to_availability(
    manifest_path=manifest,
    out_path=out,
    run_id="2026-04-15T00-00-00Z",
    window_start=dt.date(2023, 4, 1),
    window_end=dt.date(2023, 4, 5),
    sources=[
        ("air_recharge", "AIR Recharge"),
        ("in_voice", "IN Voice"),
        ("momo_transactions", "Mobile Money Transactions"),
    ],
)
```

- [ ] **Step 5: Run test to verify it passes**

```bash
pytest tests/test_pivot_golden.py -v
```

Expected: pass. If the diff is off by timestamp formatting, adjust the golden — but do NOT silently fudge. The assertion message prints both sides for a clean diff.

- [ ] **Step 6: Commit**

```bash
git add src/airtel_availability/availability_writer.py tests/fixtures/tiny_manifest.tsv tests/fixtures/tiny_availability.golden.csv tests/test_pivot_golden.py
git commit -m "feat(availability): pivot enriched manifest → full-grid availability.csv"
```

---

## Task 6: Unclassified writer

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\src\airtel_availability\unclassified_writer.py`
- Create: `C:\sandbox\URA\AIRTEL\tests\test_unclassified.py`

- [ ] **Step 1: Write the failing test**

Write to `C:\sandbox\URA\AIRTEL\tests\test_unclassified.py`:

```python
"""Unclassified aggregator tests."""
from pathlib import Path

from airtel_availability.unclassified_writer import compute_signature, write_unclassified


def test_signature_strips_digits_and_extensions():
    # Dated filename with sequence suffix, gzipped
    assert compute_signature("CBS_CDR_XYZ_20230412_001.add.gz") == "cbs_cdr_xyz_#"
    # Simple dated file
    assert compute_signature("report_2023-04-12.csv") == "report_#"
    # No digits, no extension
    assert compute_signature("dealer_report") == "dealer_report"
    # Only digits and extension
    assert compute_signature("20230412.csv") == "#"


def test_write_unclassified_aggregates_and_sorts(tmp_path):
    from airtel_availability.manifest_io import EnrichedRow
    rows = [
        EnrichedRow("/master_data1/foo_20230412.csv",  "master_data1", 100, 1680307200, "", "", "", "unknown", "", ""),
        EnrichedRow("/master_data1/foo_20230413.csv",  "master_data1", 200, 1680393600, "", "", "", "unknown", "", ""),
        EnrichedRow("/master_data2/bar_20230412.dat",  "master_data2", 50,  1680307200, "", "", "", "unknown", "", ""),
        EnrichedRow("/master_data1/cbs_rch_20230412.add", "master_data1", 1000, 1680307200, "", "", "air_recharge", "high", "2023-04-12", "filename"),
    ]
    out = tmp_path / "unclassified.csv"
    write_unclassified(out, rows)
    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "signature,origin,count,earliest_mtime,latest_mtime,example_path_1,example_path_2,example_path_3"
    # foo_# in master_data1 should have count=2 and sort before bar_# (count=1 in master_data2)
    assert lines[1].startswith("foo_#,master_data1,2,")
    assert lines[2].startswith("bar_#,master_data2,1,")
    # classified row must not appear
    assert "cbs_rch" not in out.read_text(encoding="utf-8")
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_unclassified.py -v
```

Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement `unclassified_writer.py`**

Write to `C:\sandbox\URA\AIRTEL\src\airtel_availability\unclassified_writer.py`:

```python
"""Aggregates unclassified manifest rows into unclassified.csv."""
from __future__ import annotations

import csv
import datetime as dt
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from .manifest_io import EnrichedRow


_EXT_STRIP = re.compile(r"(\.gz|\.bz2|\.xz|\.tgz|\.tar|\.csv|\.add|\.dat|\.log|\.txt|\.json|\.xml)+$", re.I)
_DIGIT_RUN = re.compile(r"\d+")


def compute_signature(basename: str) -> str:
    """Derive a stable signature from a filename for prefix-style aggregation.

    Lowercase, strip recognised data/archive extensions (repeated), then
    collapse every digit run into '#'. Preserves internal separators.
    """
    name = basename.lower()
    # Strip repeated extensions (e.g. foo.csv.gz → foo)
    while True:
        stripped = _EXT_STRIP.sub("", name)
        if stripped == name:
            break
        name = stripped
    return _DIGIT_RUN.sub("#", name)


def write_unclassified(path: str | Path, rows: Iterable[EnrichedRow]) -> None:
    buckets = defaultdict(lambda: {"count": 0, "min_mtime": None, "max_mtime": None, "examples": []})
    for row in rows:
        if row.source_key:        # only unclassified here
            continue
        sig = compute_signature(os.path.basename(row.path))
        key = (sig, row.origin)
        b = buckets[key]
        b["count"] += 1
        if b["min_mtime"] is None or row.mtime < b["min_mtime"]:
            b["min_mtime"] = row.mtime
        if b["max_mtime"] is None or row.mtime > b["max_mtime"]:
            b["max_mtime"] = row.mtime
        if len(b["examples"]) < 3:
            b["examples"].append(row.path)

    # Sort by count desc, then signature asc (stable output)
    ordered = sorted(buckets.items(), key=lambda kv: (-kv[1]["count"], kv[0][0], kv[0][1]))

    with Path(path).open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["signature", "origin", "count", "earliest_mtime", "latest_mtime",
                    "example_path_1", "example_path_2", "example_path_3"])
        for (sig, origin), b in ordered:
            ex = b["examples"] + ["", "", ""]
            w.writerow([
                sig, origin, b["count"],
                dt.datetime.fromtimestamp(b["min_mtime"], tz=dt.timezone.utc).isoformat() if b["min_mtime"] else "",
                dt.datetime.fromtimestamp(b["max_mtime"], tz=dt.timezone.utc).isoformat() if b["max_mtime"] else "",
                ex[0], ex[1], ex[2],
            ])
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_unclassified.py -v
```

Expected: 2/2 pass.

- [ ] **Step 5: Commit**

```bash
git add src/airtel_availability/unclassified_writer.py tests/test_unclassified.py
git commit -m "feat(unclassified): signature-based aggregation of unknown files"
```

---

## Task 7: Summary xlsx writer

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\src\airtel_availability\summary_writer.py`
- Create: `C:\sandbox\URA\AIRTEL\tests\test_summary.py`

- [ ] **Step 1: Write the failing test**

Write to `C:\sandbox\URA\AIRTEL\tests\test_summary.py`:

```python
"""summary.xlsx smoke test — correct sheets exist, with expected structure."""
from pathlib import Path

import openpyxl

from airtel_availability.summary_writer import write_summary


def test_summary_structure(tmp_path):
    availability_csv = Path("tests/fixtures/tiny_availability.golden.csv")
    out = tmp_path / "summary.xlsx"
    write_summary(
        availability_path=availability_csv,
        out_path=out,
        run_stats={
            "run_id": "2026-04-15T00-00-00Z",
            "total_files": 100,
            "classified": 80,
            "unclassified": 20,
            "ambiguous": 0,
            "archives": 0,
            "scan_errors": 0,
            "elapsed_seconds": 42.0,
        },
    )
    wb = openpyxl.load_workbook(out)

    # Overview + per-source + run stats
    assert "Overview" in wb.sheetnames
    assert "Run stats" in wb.sheetnames
    # The tiny availability has 3 sources — expect one sheet each
    assert "air_recharge" in wb.sheetnames
    assert "in_voice" in wb.sheetnames
    assert "momo_transactions" in wb.sheetnames

    # Overview row count: header + 3 sources
    ov = wb["Overview"]
    assert ov.max_row == 4

    # Per-source sheet has at least a header + some date rows
    sh = wb["air_recharge"]
    assert sh.max_row >= 2
    assert sh["A1"].value == "date"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_summary.py -v
```

Expected: `ModuleNotFoundError`.

- [ ] **Step 3: Implement `summary_writer.py`**

Write to `C:\sandbox\URA\AIRTEL\src\airtel_availability\summary_writer.py`:

```python
"""Builds summary.xlsx: Overview + per-source + Run stats sheets."""
from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

import openpyxl
from openpyxl.styles import Font, PatternFill


ORIGIN_FILE_COLS = [
    "files_master_data1", "files_master_data2", "files_mobile_money",
    "files_resubmited", "files_unique_trnx",
]


def write_summary(*, availability_path: str | Path, out_path: str | Path, run_stats: dict[str, Any]) -> None:
    # Read the full availability.csv grid once.
    rows = []
    with Path(availability_path).open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    wb = openpyxl.Workbook()
    # Default sheet
    ws_overview = wb.active
    ws_overview.title = "Overview"
    _write_overview(ws_overview, rows)

    # Per-source sheets
    by_source = defaultdict(list)
    for r in rows:
        by_source[(r["source_key"], r["source_label"])].append(r)

    for (skey, slabel), srows in by_source.items():
        ws = wb.create_sheet(title=_sheet_name(skey))
        _write_source_sheet(ws, slabel, srows)

    # Run stats sheet
    ws_stats = wb.create_sheet(title="Run stats")
    _write_run_stats(ws_stats, run_stats)

    wb.save(out_path)


def _sheet_name(key: str) -> str:
    # Excel sheet names are limited to 31 chars; our keys are all shorter
    return key[:31]


def _write_overview(ws, rows: list[dict]) -> None:
    header = ["source_key", "source_label", "first_date", "last_date",
              "covered_days", "total_days", "coverage_pct",
              "total_files", "total_bytes", "gap_count", "date_confidence_mix"]
    ws.append(header)
    for cell in ws[1]:
        cell.font = Font(bold=True)

    by_source = defaultdict(list)
    for r in rows:
        by_source[(r["source_key"], r["source_label"])].append(r)

    for (skey, slabel), srows in by_source.items():
        covered = [r for r in srows if int(r["files_total"]) > 0]
        total_days = len(srows)
        covered_days = len(covered)
        first = covered[0]["date"] if covered else ""
        last = covered[-1]["date"] if covered else ""
        total_files = sum(int(r["files_total"]) for r in srows)
        total_bytes = sum(int(r["bytes_total"]) for r in srows)
        gap_count = total_days - covered_days
        confs = {r["date_confidence"] for r in covered if r["date_confidence"]}
        conf_mix = "/".join(sorted(confs)) or ""
        coverage_pct = round(100.0 * covered_days / total_days, 2) if total_days else 0.0
        ws.append([skey, slabel, first, last, covered_days, total_days, coverage_pct,
                   total_files, total_bytes, gap_count, conf_mix])


def _write_source_sheet(ws, slabel: str, srows: list[dict]) -> None:
    ws.append(["date"] + ORIGIN_FILE_COLS + ["files_total", "date_confidence"])
    for cell in ws[1]:
        cell.font = Font(bold=True)

    gap_fill = PatternFill(start_color="FFEECCCC", end_color="FFEECCCC", fill_type="solid")
    for r in srows:
        row = [r["date"]] + [int(r[c]) for c in ORIGIN_FILE_COLS] + [int(r["files_total"]), r["date_confidence"]]
        ws.append(row)
        if int(r["files_total"]) == 0:
            for cell in ws[ws.max_row]:
                cell.fill = gap_fill


def _write_run_stats(ws, stats: dict[str, Any]) -> None:
    ws.append(["metric", "value"])
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for k in ["run_id", "total_files", "classified", "unclassified", "ambiguous",
              "archives", "scan_errors", "elapsed_seconds"]:
        ws.append([k, stats.get(k, "")])
```

- [ ] **Step 4: Run test to verify it passes**

```bash
pytest tests/test_summary.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/airtel_availability/summary_writer.py tests/test_summary.py
git commit -m "feat(summary): xlsx workbook with overview + per-source + run stats"
```

---

## Task 8: `scan.sh` (remote-side POSIX bash)

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\src\airtel_availability\scan.sh`
- Create: `C:\sandbox\URA\AIRTEL\tests\test_scan_sh.py`

scan.sh emits `path\torigin\tsize\tmtime\tarchive_parent\tarchive_entry` for every file. For tar/tar.gz files, it additionally emits one row per archive entry.

- [ ] **Step 1: Write the failing test**

Write to `C:\sandbox\URA\AIRTEL\tests\test_scan_sh.py`:

```python
"""Tests scan.sh against a fake tree built under tmp_path."""
import gzip
import subprocess
import tarfile
from pathlib import Path

import pytest


def _build_fake_tree(root: Path) -> None:
    """Mirrors the five-root layout with a handful of files incl. gzip and tar.gz."""
    (root / "master_data1/airtel_ftp/airtel_ftpu/2023/04").mkdir(parents=True)
    (root / "master_data1/airtel_ftp/airtel_ftpu/2023/04/cbs_cdr_rch_20230412.add").write_bytes(b"x" * 100)

    (root / "master_data2/airtel_ftp/airtel_ftpu/2023/04").mkdir(parents=True)
    # A gzipped CSV
    with gzip.open(root / "master_data2/airtel_ftp/airtel_ftpu/2023/04/cbs_cdr_voi_20230412.csv.gz", "wb") as f:
        f.write(b"a,b,c\n1,2,3\n")

    (root / "airtel_mobile_money/2023/04").mkdir(parents=True)
    (root / "airtel_mobile_money/2023/04/txn_20230412.csv").write_bytes(b"a,b,c\n1,2,3\n")

    (root / "resubmited_data").mkdir(parents=True)
    # A tar.gz containing two files
    tar_path = root / "resubmited_data/bundle_20230412.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        # Write real files for tarring
        p1 = root / "_staging_cbs_cdr_rch_20230412.add"
        p2 = root / "_staging_cbs_cdr_voi_20230412.add"
        p1.write_bytes(b"x" * 50)
        p2.write_bytes(b"x" * 60)
        tar.add(p1, arcname="cbs_cdr_rch_20230412.add")
        tar.add(p2, arcname="cbs_cdr_voi_20230412.add")
        p1.unlink()
        p2.unlink()

    (root / "unique_trnx").mkdir(parents=True)
    (root / "unique_trnx/cbs_cdr_rch_20230412.add").write_bytes(b"x" * 80)


SCAN_SH = Path("src/airtel_availability/scan.sh")


def _px(p: Path) -> str:
    """Convert a pathlib.Path to a bash-friendly POSIX-style string.

    On Windows (Git Bash) this yields `C:/Users/...` with forward slashes,
    which MSYS understands. On Linux/macOS it's a plain POSIX path.
    """
    return p.as_posix()


@pytest.mark.skipif(not SCAN_SH.exists(), reason="scan.sh not yet written")
def test_scan_sh_emits_expected_manifest(tmp_path):
    _build_fake_tree(tmp_path)
    out_path = tmp_path / "manifest.tsv"
    log_path = tmp_path / "scan.log"

    r = subprocess.run(
        ["bash", _px(SCAN_SH),
         "--root", f"master_data1={_px(tmp_path / 'master_data1')}",
         "--root", f"master_data2={_px(tmp_path / 'master_data2')}",
         "--root", f"airtel_mobile_money={_px(tmp_path / 'airtel_mobile_money')}",
         "--root", f"resubmited={_px(tmp_path / 'resubmited_data')}",
         "--root", f"unique_trnx={_px(tmp_path / 'unique_trnx')}",
         "--out", _px(out_path),
         "--log", _px(log_path)],
        check=True, capture_output=True, text=True,
    )

    lines = out_path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split("\t")
    assert header == ["path", "origin", "size", "mtime", "archive_parent", "archive_entry"]

    data = [dict(zip(header, line.split("\t"))) for line in lines[1:]]
    # 4 plain files + 1 tarball + 2 entries = 7 rows
    assert len(data) == 7

    paths = {d["path"].rsplit("/", 1)[-1] for d in data}
    assert "cbs_cdr_rch_20230412.add" in paths
    assert "cbs_cdr_voi_20230412.csv.gz" in paths
    assert "txn_20230412.csv" in paths
    # Tarball itself and both entries
    assert "bundle_20230412.tar.gz" in paths

    # Tarball entries must have archive_parent set
    entries = [d for d in data if d["archive_entry"]]
    assert len(entries) == 2
    for e in entries:
        assert e["archive_parent"].endswith("bundle_20230412.tar.gz")
        assert e["origin"] == "resubmited"
```

- [ ] **Step 2: Run test to verify it is skipped/fails**

```bash
pytest tests/test_scan_sh.py -v
```

Expected: skipped (scan.sh doesn't exist yet).

- [ ] **Step 3: Implement `scan.sh`**

Write to `C:\sandbox\URA\AIRTEL\src\airtel_availability\scan.sh`:

```bash
#!/usr/bin/env bash
# scan.sh — emit a raw file manifest for Airtel URA availability audit.
#
# USAGE:
#   scan.sh --root <origin>=<path> [--root ...] --out <file> --log <file>
#
# Emits TSV to --out with columns:
#   path  origin  size  mtime  archive_parent  archive_entry
#
# For .tar/.tar.gz/.tgz files we emit the archive itself AND one row per entry
# (archive_parent set to the archive path, archive_entry set to the entry name).
#
# Errors (permission denied, corrupt archives, etc.) go to --log.
# No classification or date parsing here — that happens locally in pivot.py.

set -uo pipefail

OUT=""
LOG=""
declare -a ROOTS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --root)
            ROOTS+=("$2"); shift 2;;
        --out)
            OUT="$2"; shift 2;;
        --log)
            LOG="$2"; shift 2;;
        *)
            echo "unknown arg: $1" >&2; exit 2;;
    esac
done

if [[ -z "$OUT" || -z "$LOG" || ${#ROOTS[@]} -eq 0 ]]; then
    echo "usage: scan.sh --root <origin>=<path> [--root ...] --out <file> --log <file>" >&2
    exit 2
fi

: > "$OUT"
: > "$LOG"

# Header
printf 'path\torigin\tsize\tmtime\tarchive_parent\tarchive_entry\n' >> "$OUT"

emit_file() {
    # $1=path  $2=origin  $3=size  $4=mtime  $5=archive_parent  $6=archive_entry
    # Tabs in paths are not permitted on ext4 etc., but guard anyway.
    local p="${1//$'\t'/ }"
    local ap="${5//$'\t'/ }"
    local ae="${6//$'\t'/ }"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$p" "$2" "$3" "$4" "$ap" "$ae" >> "$OUT"
}

enumerate_tar() {
    local tarball="$1" origin="$2"
    # tar lists members; -v --full-time gives sizes + mtimes via numeric stat form.
    # But for maximum portability we use `tar -tzvf` for tar.gz, `tar -tvf` for plain tar.
    # Columns (roughly): perms owner/group size date time name
    local tflag="-tvf"
    case "$tarball" in
        *.tar.gz|*.tgz) tflag="-tzvf";;
    esac
    tar $tflag "$tarball" 2>>"$LOG" | awk -v origin="$origin" -v parent="$tarball" '
        # Skip blank lines
        !$0 { next }
        {
            # tar verbose format: perms owner size date time name
            # name is fields 6.. (may contain spaces); size is field 3.
            size = $3
            # Reconstruct name: everything after field 5.
            name = ""
            for (i = 6; i <= NF; i++) {
                if (name != "") name = name " ";
                name = name $i
            }
            # mtime: we don't have epoch here; use the tarball's mtime upstream as
            # a coarse fallback — but to keep things simple, report 0 and let
            # pivot.py infer from filename or fall back to the tarball mtime.
            printf("__ENTRY__\t%s\t%s\t0\t%s\t%s\n", origin, size, parent, name)
        }
    ' >> "$OUT.tmp_entries"
}

for entry in "${ROOTS[@]}"; do
    origin="${entry%%=*}"
    path="${entry#*=}"
    [[ -d "$path" ]] || { echo "root not a dir: $path" >>"$LOG"; continue; }

    # %p=path %s=size %T@=mtime (as float seconds since epoch)
    find "$path" -type f -printf '%p\t%s\t%T@\n' 2>>"$LOG" | while IFS=$'\t' read -r fpath fsize fmtime; do
        # Truncate fractional seconds: mtime as integer.
        fmtime_int="${fmtime%.*}"
        emit_file "$fpath" "$origin" "$fsize" "$fmtime_int" "" ""

        case "$fpath" in
            *.tar|*.tar.gz|*.tgz)
                enumerate_tar "$fpath" "$origin"
                ;;
        esac
    done
done

# Now merge archive-entry rows, patching __ENTRY__ placeholder with an actual
# (non-meaningful) path column value: we use the archive_parent + "!" + entry.
if [[ -s "$OUT.tmp_entries" ]]; then
    awk -F'\t' 'BEGIN{OFS="\t"} { sub(/^__ENTRY__/, $5 "!" $6); print }' "$OUT.tmp_entries" >> "$OUT"
    rm -f "$OUT.tmp_entries"
fi

# Drop sentinel
: > "$(dirname "$OUT")/$(basename "$OUT" .tsv).done"
```

Wait — the awk step builds the synthetic path `archive_parent!entry`. But the test expects `archive_entry` to be the clean entry name. Let me adjust the awk to emit the right columns. Revise the end of `enumerate_tar` and the merge:

Replace the `enumerate_tar` awk block and the final merge with:

```bash
# Inside enumerate_tar, write rows directly to OUT with synthetic path.
enumerate_tar() {
    local tarball="$1" origin="$2"
    local tflag="-tvf"
    case "$tarball" in
        *.tar.gz|*.tgz) tflag="-tzvf";;
    esac
    tar $tflag "$tarball" 2>>"$LOG" | awk -v origin="$origin" -v parent="$tarball" -v outfile="$OUT" '
        !$0 { next }
        {
            size = $3
            name = ""
            for (i = 6; i <= NF; i++) {
                if (name != "") name = name " ";
                name = name $i
            }
            # Synthetic path = parent + "!" + entry (useful for tracing)
            synth = parent "!" name
            printf("%s\t%s\t%s\t0\t%s\t%s\n", synth, origin, size, parent, name) >> outfile
        }
    '
}
```

And remove the final `awk ... $OUT.tmp_entries` merge block. Final version:

```bash
#!/usr/bin/env bash
set -uo pipefail

OUT=""
LOG=""
declare -a ROOTS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --root) ROOTS+=("$2"); shift 2;;
        --out)  OUT="$2"; shift 2;;
        --log)  LOG="$2"; shift 2;;
        *) echo "unknown arg: $1" >&2; exit 2;;
    esac
done

if [[ -z "$OUT" || -z "$LOG" || ${#ROOTS[@]} -eq 0 ]]; then
    echo "usage: scan.sh --root <origin>=<path> [--root ...] --out <file> --log <file>" >&2
    exit 2
fi

: > "$OUT"
: > "$LOG"
printf 'path\torigin\tsize\tmtime\tarchive_parent\tarchive_entry\n' >> "$OUT"

emit_file() {
    local p="${1//$'\t'/ }"
    local ap="${5//$'\t'/ }"
    local ae="${6//$'\t'/ }"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$p" "$2" "$3" "$4" "$ap" "$ae" >> "$OUT"
}

enumerate_tar() {
    local tarball="$1" origin="$2"
    local tflag="-tvf"
    case "$tarball" in
        *.tar.gz|*.tgz) tflag="-tzvf";;
    esac
    tar $tflag "$tarball" 2>>"$LOG" | awk -v origin="$origin" -v parent="$tarball" -v outfile="$OUT" '
        !$0 { next }
        {
            size = $3
            name = ""
            for (i = 6; i <= NF; i++) {
                if (name != "") name = name " ";
                name = name $i
            }
            synth = parent "!" name
            printf("%s\t%s\t%s\t0\t%s\t%s\n", synth, origin, size, parent, name) >> outfile
        }
    '
}

for entry in "${ROOTS[@]}"; do
    origin="${entry%%=*}"
    path="${entry#*=}"
    [[ -d "$path" ]] || { echo "root not a dir: $path" >>"$LOG"; continue; }

    find "$path" -type f -printf '%p\t%s\t%T@\n' 2>>"$LOG" | while IFS=$'\t' read -r fpath fsize fmtime; do
        fmtime_int="${fmtime%.*}"
        emit_file "$fpath" "$origin" "$fsize" "$fmtime_int" "" ""
        case "$fpath" in
            *.tar|*.tar.gz|*.tgz) enumerate_tar "$fpath" "$origin";;
        esac
    done
done

: > "$(dirname "$OUT")/.$(basename "$OUT").done"
```

- [ ] **Step 4: Make executable and run test**

```bash
cd /c/sandbox/URA/AIRTEL
chmod +x src/airtel_availability/scan.sh
pytest tests/test_scan_sh.py -v
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/airtel_availability/scan.sh tests/test_scan_sh.py
git commit -m "feat(scan): POSIX bash scan.sh that emits raw manifest + tar enumeration"
```

---

## Task 9: Local end-to-end pipeline + CLI

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\src\airtel_availability\pivot.py`
- Modify: `C:\sandbox\URA\AIRTEL\src\airtel_availability\manifest_io.py` — add enrichment helper
- Create: `C:\sandbox\URA\AIRTEL\tests\test_end_to_end_fake_tree.py`

- [ ] **Step 1: Write the failing end-to-end test**

Write to `C:\sandbox\URA\AIRTEL\tests\test_end_to_end_fake_tree.py`:

```python
"""Layer 4 — full local pipeline against a fake tree on disk."""
import subprocess
import sys
from pathlib import Path

from tests.test_scan_sh import _build_fake_tree, _px   # reuse builder + path helper


def test_full_pipeline(tmp_path):
    tree = tmp_path / "tree"
    tree.mkdir()
    _build_fake_tree(tree)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    r = subprocess.run(
        [sys.executable, "-m", "airtel_availability.pivot",
         "--scan-roots",
         f"master_data1={_px(tree / 'master_data1')}",
         f"master_data2={_px(tree / 'master_data2')}",
         f"airtel_mobile_money={_px(tree / 'airtel_mobile_money')}",
         f"resubmited={_px(tree / 'resubmited_data')}",
         f"unique_trnx={_px(tree / 'unique_trnx')}",
         "--window-start", "2023-04-01",
         "--window-end", "2023-04-30",
         "--catalog", "config/catalog.yaml",
         "--date-patterns", "config/date_patterns.yaml",
         "--run-id", "2026-04-15T00-00-00Z",
         "--out", str(out_dir)],
        check=True, capture_output=True, text=True,
    )

    av = out_dir / "availability.csv"
    assert av.exists(), r.stderr
    body = av.read_text(encoding="utf-8")
    # 2023-04-12 must have a row for air_recharge with files_total >= 2 (master_data1 + unique_trnx)
    rch_0412 = [l for l in body.splitlines() if ",air_recharge," in l and ",2023-04-12," in l]
    assert rch_0412, "expected an air_recharge row for 2023-04-12"
    cols = rch_0412[0].split(",")
    # 19 sources × 30 days = 570 grid rows
    rows = body.splitlines()
    assert len(rows) - 1 == 19 * 30, f"expected full grid of 570 rows; got {len(rows)-1}"

    assert (out_dir / "manifest.raw.tsv").exists()
    assert (out_dir / "manifest.tsv").exists()
    assert (out_dir / "unclassified.csv").exists()
    assert (out_dir / "summary.xlsx").exists()
    assert (out_dir / "run_summary.txt").exists()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_end_to_end_fake_tree.py -v
```

Expected: ModuleNotFoundError for `airtel_availability.pivot`.

- [ ] **Step 3: Implement `pivot.py` CLI**

Write to `C:\sandbox\URA\AIRTEL\src\airtel_availability\pivot.py`:

```python
"""Local pipeline CLI.

Given either:
  - a pre-produced raw manifest (--manifest PATH), OR
  - local roots to scan (--scan-roots origin=path ...),

this script produces the full set of output artifacts in --out/:
  manifest.raw.tsv  manifest.tsv  availability.csv  unclassified.csv  summary.xlsx  run_summary.txt
  catalog.snapshot.yaml  date_patterns.snapshot.yaml
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from .availability_writer import pivot_to_availability
from .classifier import Classifier
from .date_extractor import DateExtractor
from .manifest_io import ENRICHED_HEADERS, EnrichedRow, read_raw_manifest, write_enriched_manifest
from .summary_writer import write_summary
from .unclassified_writer import write_unclassified


SCAN_SH = Path(__file__).parent / "scan.sh"


def enrich(*, raw_path: Path, catalog: Classifier, dates: DateExtractor) -> list[EnrichedRow]:
    out: list[EnrichedRow] = []
    for row in read_raw_manifest(raw_path):
        # Name used for classification/date: archive_entry if present, else basename of path.
        name = row.archive_entry if row.archive_entry else os.path.basename(row.path)
        cl = catalog.classify(name, row.path)
        de = dates.extract(name)
        if de.date is not None:
            date_str = de.date.isoformat()
            date_source = "filename"
        elif row.mtime > 0:
            date_str = dt.date.fromtimestamp(row.mtime).isoformat()
            date_source = "mtime"
        else:
            date_str = ""
            date_source = "unknown"
        out.append(EnrichedRow(
            path=row.path, origin=row.origin, size=row.size, mtime=row.mtime,
            archive_parent=row.archive_parent, archive_entry=row.archive_entry,
            source_key=cl.source_key, confidence=cl.confidence,
            date=date_str, date_source=date_source,
        ))
    return out


def run_local_scan(roots: list[str], out_path: Path, log_path: Path) -> None:
    args = ["bash", str(SCAN_SH)]
    for r in roots:
        args.extend(["--root", r])
    args.extend(["--out", str(out_path), "--log", str(log_path)])
    subprocess.run(args, check=True)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="airtel-availability-pivot")
    grp = ap.add_mutually_exclusive_group(required=True)
    grp.add_argument("--manifest", type=Path,
                     help="Path to a pre-produced raw manifest (remote-pulled).")
    grp.add_argument("--scan-roots", nargs="+", default=None,
                     help="Origin=path entries; scans locally via scan.sh (used in tests).")
    ap.add_argument("--window-start", required=True, type=lambda s: dt.date.fromisoformat(s))
    ap.add_argument("--window-end",   required=True, type=lambda s: dt.date.fromisoformat(s))
    ap.add_argument("--catalog", required=True, type=Path)
    ap.add_argument("--date-patterns", required=True, type=Path)
    ap.add_argument("--run-id", default=None, help="Defaults to current UTC ISO-8601.")
    ap.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)

    a.out.mkdir(parents=True, exist_ok=True)
    run_id = a.run_id or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

    raw_manifest = a.out / "manifest.raw.tsv"
    scan_log = a.out / "scan.log"

    started = time.time()
    if a.scan_roots:
        run_local_scan(a.scan_roots, raw_manifest, scan_log)
    else:
        shutil.copyfile(a.manifest, raw_manifest)

    catalog = Classifier.from_yaml(a.catalog)
    dates = DateExtractor.from_yaml(a.date_patterns)

    enriched = enrich(raw_path=raw_manifest, catalog=catalog, dates=dates)
    enriched_path = a.out / "manifest.tsv"
    write_enriched_manifest(enriched_path, enriched)

    pivot_to_availability(
        manifest_path=enriched_path,
        out_path=a.out / "availability.csv",
        run_id=run_id,
        window_start=a.window_start,
        window_end=a.window_end,
        catalog_path=a.catalog,
    )
    write_unclassified(a.out / "unclassified.csv", enriched)

    # Run stats for summary.xlsx + banner
    total = len(enriched)
    classified = sum(1 for e in enriched if e.source_key)
    unclassified = total - classified
    ambiguous = sum(1 for e in enriched if e.confidence == "ambiguous")
    archives = sum(1 for e in enriched if e.archive_parent and not e.archive_entry)
    archive_entries = sum(1 for e in enriched if e.archive_entry)
    scan_errors = _count_lines(scan_log)
    elapsed = time.time() - started
    stats = dict(
        run_id=run_id, total_files=total, classified=classified, unclassified=unclassified,
        ambiguous=ambiguous, archives=archives, archive_entries=archive_entries,
        scan_errors=scan_errors, elapsed_seconds=round(elapsed, 1),
    )

    write_summary(availability_path=a.out / "availability.csv",
                  out_path=a.out / "summary.xlsx", run_stats=stats)

    # Snapshot the configs applied
    shutil.copyfile(a.catalog, a.out / "catalog.snapshot.yaml")
    shutil.copyfile(a.date_patterns, a.out / "date_patterns.snapshot.yaml")

    banner = _format_banner(stats)
    (a.out / "run_summary.txt").write_text(banner, encoding="utf-8")
    print(banner)

    _reconcile(enriched, a.out / "availability.csv", a.window_start, a.window_end)


def _count_lines(p: Path) -> int:
    try:
        return sum(1 for _ in p.open("r", encoding="utf-8", errors="replace"))
    except FileNotFoundError:
        return 0


def _format_banner(s: dict) -> str:
    cls_rate = 100.0 * s["classified"] / s["total_files"] if s["total_files"] else 0.0
    return (
        f"RUN OK     run_id={s['run_id']}\n"
        f"scanned    {s['total_files']:>12,} files     elapsed={s['elapsed_seconds']}s\n"
        f"classified {s['classified']:>12,} files  →  ({cls_rate:.1f}%)\n"
        f"unclassified {s['unclassified']:>10,} files  → see unclassified.csv\n"
        f"ambiguous  {s['ambiguous']:>12,} files\n"
        f"archives   {s['archives']:>12,} tarballs → {s['archive_entries']:,} entries enumerated\n"
        f"scan errors {s['scan_errors']:>11,} (see scan.log)\n"
    )


def _reconcile(enriched: list[EnrichedRow], availability_csv: Path, window_start: dt.date, window_end: dt.date) -> None:
    """Post-run reconciliation: sum(files_total in availability) == count(in-window classified rows)."""
    import csv
    expected = 0
    for e in enriched:
        if not e.source_key or not e.date:
            continue
        try:
            d = dt.date.fromisoformat(e.date)
        except ValueError:
            continue
        if window_start <= d <= window_end:
            expected += 1
    with availability_csv.open("r", encoding="utf-8", newline="") as f:
        actual = sum(int(r["files_total"]) for r in csv.DictReader(f))
    if actual != expected:
        raise AssertionError(
            f"reconciliation failed: availability sum={actual}, enriched in-window classified={expected}"
        )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the end-to-end test**

```bash
pytest tests/test_end_to_end_fake_tree.py -v
```

Expected: pass. If the "full grid = 570 rows" assert fires, the window math is off — fix `_daterange` or the window bounds, not the assert.

- [ ] **Step 5: Run the full test suite**

```bash
pytest -q
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/airtel_availability/pivot.py tests/test_end_to_end_fake_tree.py
git commit -m "feat(pivot): end-to-end CLI — scan → enrich → pivot → availability/summary"
```

---

## Task 10: Remote runner (`runner.py`)

**Files:**
- Create: `C:\sandbox\URA\AIRTEL\src\airtel_availability\runner.py`
- Create: `C:\sandbox\URA\AIRTEL\tests\test_runner_ssh_stub.py`

Runner uses paramiko to: upload `scan.sh` to `/tmp` on the remote, run it under `nohup`, wait for the `.done` sentinel, `scp` the manifest and log back, then invoke the same pivot pipeline as Task 9.

Per the spec, **no mocking of SSH**. For unit tests of the orchestration layer, we inject a tiny protocol (`RemoteSession`) implementation — this is a real interface, not a mock framework. The real paramiko-backed implementation is untested at the unit level; it is exercised only via Task 11's Layer 5 remote smoke procedure.

- [ ] **Step 1: Write the failing test**

Write to `C:\sandbox\URA\AIRTEL\tests\test_runner_ssh_stub.py`:

```python
"""Orchestration tests for runner.py — uses a local RemoteSession stub, not paramiko."""
import shutil
import subprocess
from pathlib import Path
from dataclasses import dataclass

import pytest

from airtel_availability.runner import run_remote_scan, RemoteSession


@dataclass
class LocalShellSession(RemoteSession):
    """Stand-in RemoteSession that runs commands locally in a given cwd."""
    root: Path

    def upload(self, local_path: Path, remote_path: str) -> None:
        (self.root / remote_path.lstrip("/")).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, self.root / remote_path.lstrip("/"))

    def run(self, command: str, *, check: bool = True) -> tuple[int, str]:
        # Force bash explicitly so Windows Git Bash + Python doesn't dispatch to cmd.exe
        r = subprocess.run(["bash", "-c", command], cwd=self.root, capture_output=True, text=True)
        if check and r.returncode != 0:
            raise RuntimeError(r.stderr)
        return r.returncode, r.stdout

    def download(self, remote_path: str, local_path: Path) -> None:
        shutil.copyfile(self.root / remote_path.lstrip("/"), local_path)

    def exists(self, remote_path: str) -> bool:
        return (self.root / remote_path.lstrip("/")).exists()


def test_runner_uploads_runs_and_downloads(tmp_path):
    # Build a fake remote root with a tiny tree
    remote_root = tmp_path / "remote"
    remote_root.mkdir()
    # Seed remote roots matching what scan.sh expects
    for p in ["master_data1/airtel_ftp/airtel_ftpu",
              "master_data2/airtel_ftp/airtel_ftpu",
              "airtel_mobile_money",
              "resubmited_data",
              "unique_trnx"]:
        (remote_root / p).mkdir(parents=True)
    (remote_root / "master_data1/airtel_ftp/airtel_ftpu/cbs_cdr_rch_20230412.add").write_bytes(b"x" * 10)

    session = LocalShellSession(root=remote_root)
    local_out = tmp_path / "out"
    local_out.mkdir()

    manifest_path, log_path = run_remote_scan(
        session=session,
        remote_scan_sh_target="/tmp/airtel_scan.sh",
        remote_out_path="/tmp/airtel_manifest.tsv",
        remote_log_path="/tmp/airtel_scan.log",
        roots=[
            "master_data1=/master_data1/airtel_ftp/airtel_ftpu",
            "master_data2=/master_data2/airtel_ftp/airtel_ftpu",
            "airtel_mobile_money=/airtel_mobile_money",
            "resubmited=/resubmited_data",
            "unique_trnx=/unique_trnx",
        ],
        local_out_dir=local_out,
    )
    assert manifest_path.exists()
    assert log_path.exists()
    assert "cbs_cdr_rch_20230412.add" in manifest_path.read_text(encoding="utf-8")
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/test_runner_ssh_stub.py -v
```

Expected: ModuleNotFoundError.

- [ ] **Step 3: Implement `runner.py`**

Write to `C:\sandbox\URA\AIRTEL\src\airtel_availability\runner.py`:

```python
"""Remote-scan orchestrator.

Defines a RemoteSession protocol so the orchestration logic is testable with a
local shell stand-in. The paramiko-backed implementation is a thin adapter.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import shlex
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from .pivot import main as pivot_main


SCAN_SH_SRC = Path(__file__).parent / "scan.sh"


@runtime_checkable
class RemoteSession(Protocol):
    def upload(self, local_path: Path, remote_path: str) -> None: ...
    def run(self, command: str, *, check: bool = True) -> tuple[int, str]: ...
    def download(self, remote_path: str, local_path: Path) -> None: ...
    def exists(self, remote_path: str) -> bool: ...


def run_remote_scan(
    *,
    session: RemoteSession,
    remote_scan_sh_target: str,
    remote_out_path: str,
    remote_log_path: str,
    roots: list[str],
    local_out_dir: Path,
    poll_interval_seconds: float = 10.0,
    max_wait_seconds: float = 24 * 3600,
) -> tuple[Path, Path]:
    """Uploads scan.sh, runs it under nohup, waits for the .done sentinel, downloads artifacts.

    Returns (local_manifest_path, local_log_path).
    """
    session.upload(SCAN_SH_SRC, remote_scan_sh_target)
    session.run(f"chmod +x {shlex.quote(remote_scan_sh_target)}")

    root_args = " ".join(f"--root {shlex.quote(r)}" for r in roots)
    # Run under nohup so the scan survives our disconnect.
    done_sentinel = (
        f"{remote_out_path.rsplit('/', 1)[0]}/.{remote_out_path.rsplit('/', 1)[-1]}.done"
    )
    session.run(
        f"nohup bash {shlex.quote(remote_scan_sh_target)} "
        f"{root_args} --out {shlex.quote(remote_out_path)} --log {shlex.quote(remote_log_path)} "
        f">/dev/null 2>&1 </dev/null & disown"
    )

    # Wait for sentinel
    start = time.time()
    while not session.exists(done_sentinel):
        if time.time() - start > max_wait_seconds:
            raise TimeoutError(f"remote scan did not complete within {max_wait_seconds}s")
        time.sleep(poll_interval_seconds)

    local_manifest = local_out_dir / "manifest.raw.tsv"
    local_log = local_out_dir / "scan.log"
    session.download(remote_out_path, local_manifest)
    session.download(remote_log_path, local_log)
    return local_manifest, local_log


# --- paramiko-backed adapter (exercised only via remote smoke) ---

@dataclass
class ParamikoSession(RemoteSession):
    host: str
    user: str
    port: int = 22
    password: str | None = None

    def __post_init__(self):
        import paramiko
        self._client = paramiko.SSHClient()
        self._client.load_system_host_keys()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.connect(self.host, port=self.port, username=self.user, password=self.password)
        self._sftp = self._client.open_sftp()

    def upload(self, local_path: Path, remote_path: str) -> None:
        self._sftp.put(str(local_path), remote_path)

    def run(self, command: str, *, check: bool = True) -> tuple[int, str]:
        stdin, stdout, stderr = self._client.exec_command(command)
        rc = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        if check and rc != 0:
            raise RuntimeError(f"remote command failed ({rc}): {command}\nstderr: {err}")
        return rc, out

    def download(self, remote_path: str, local_path: Path) -> None:
        self._sftp.get(remote_path, str(local_path))

    def exists(self, remote_path: str) -> bool:
        try:
            self._sftp.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def close(self) -> None:
        self._sftp.close()
        self._client.close()


def main(argv=None):
    ap = argparse.ArgumentParser(prog="airtel-availability")
    ap.add_argument("--host", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--port", type=int, default=22)
    ap.add_argument("--window-start", required=True)
    ap.add_argument("--window-end", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--catalog", default="config/catalog.yaml", type=Path)
    ap.add_argument("--date-patterns", default="config/date_patterns.yaml", type=Path)
    ap.add_argument("--limit-subdir",
                    help="Smoke-test mode: scan only the given remote subdirectory instead of the 5 roots.")
    a = ap.parse_args(argv)
    password = os.environ.get("AIRTEL_SSH_PASSWORD")
    session = ParamikoSession(host=a.host, user=a.user, port=a.port, password=password)
    try:
        a.out.mkdir(parents=True, exist_ok=True)
        run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        run_dir = a.out / f"run_{run_id}"
        run_dir.mkdir()

        if a.limit_subdir:
            roots = [f"smoke={a.limit_subdir}"]
        else:
            roots = [
                "master_data1=/master_data1/airtel_ftp/airtel_ftpu",
                "master_data2=/master_data2/airtel_ftp/airtel_ftpu",
                "airtel_mobile_money=/airtel_mobile_money",
                "resubmited=/resubmited_data",
                "unique_trnx=/unique_trnx",
            ]
        manifest_path, log_path = run_remote_scan(
            session=session,
            remote_scan_sh_target=f"/tmp/airtel_scan_{run_id}.sh",
            remote_out_path=f"/tmp/airtel_manifest_{run_id}.tsv",
            remote_log_path=f"/tmp/airtel_scan_{run_id}.log",
            roots=roots,
            local_out_dir=run_dir,
        )
        pivot_main([
            "--manifest", str(manifest_path),
            "--window-start", a.window_start,
            "--window-end", a.window_end,
            "--catalog", str(a.catalog),
            "--date-patterns", str(a.date_patterns),
            "--run-id", run_id,
            "--out", str(run_dir),
        ])
    finally:
        session.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test**

```bash
pytest tests/test_runner_ssh_stub.py -v
```

Expected: pass. (The test exercises `run_remote_scan` against `LocalShellSession`; it does not touch paramiko.)

- [ ] **Step 5: Run the full test suite**

```bash
pytest -q
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/airtel_availability/runner.py tests/test_runner_ssh_stub.py
git commit -m "feat(runner): SSH orchestrator with injectable RemoteSession + paramiko adapter"
```

---

## Task 11: Remote smoke test procedure + v1 sign-off checklist

**Files:**
- Modify: `C:\sandbox\URA\AIRTEL\README.md` (append Remote Smoke section)
- Create: `C:\sandbox\URA\AIRTEL\docs\superpowers\RUNBOOK.md`

This task produces no new code. It produces an operator runbook that an auditor can follow to execute v1, review results, and sign off.

- [ ] **Step 1: Write RUNBOOK.md**

Write to `C:\sandbox\URA\AIRTEL\docs\superpowers\RUNBOOK.md`:

```markdown
# Airtel URA Data Availability — Operator Runbook

This runbook takes you from a fresh clone to a signed-off v1 availability report.

## Preflight (once)

1. `python -m venv .venv && source .venv/Scripts/activate`
2. `pip install -e '.[dev]'`
3. `pytest -q` — all tests pass.
4. Set the SSH password:

   ```bash
   export AIRTEL_SSH_PASSWORD='Ugam()&49'
   ```

## Step 1 — Remote smoke test (Layer 5)

Scan a single small subdirectory first. Use a directory you expect to contain a handful of files from a handful of sources.

```bash
python -m airtel_availability.runner \
    --host 192.168.30.24 --user gamma \
    --limit-subdir /master_data1/airtel_ftp/airtel_ftpu/2024/01 \
    --window-start 2024-01-01 --window-end 2024-01-31 \
    --out out/smoke/
```

Review `out/smoke/run_<timestamp>/`:

- **`run_summary.txt`** — classification rate is ≥ 80% on the smoke set? (90% threshold is for the full run; smoke set is allowed to be noisier.)
- **`unclassified.csv`** — look at the top signatures. For each, decide: is this a real source that needs a catalog entry, or is it junk (e.g., `.bak`, `.old`)?
- **Extrapolate elapsed time.** If the smoke set took N seconds and covered ~1/60th of the full 5-year window, the full run is ~60 × N. Acceptable?

**If classification rate is below 80% on the smoke,** edit `config/catalog.yaml`, add patterns, add test cases, rerun `pytest` (must stay green), rerun the smoke.

## Step 2 — Full 5-year run

```bash
python -m airtel_availability.runner \
    --host 192.168.30.24 --user gamma \
    --window-start 2021-04-15 --window-end 2026-04-15 \
    --out out/
```

This can run for hours. `scan.sh` runs on the remote under `nohup`; if your SSH session drops, re-run the command — it will detect the existing run and resume waiting for the sentinel. (If you deliberately want a fresh scan, delete the `.done` sentinel on the remote first.)

## Step 3 — Review & sign-off

Inspect `out/run_<timestamp>/`:

- **`run_summary.txt`**
  - `classified` rate ≥ 90%? (Hard target from the spec.)
  - `scan errors` == 0? If not, open `scan.log` and check each one.
- **`unclassified.csv`**
  - Any single signature with thousands of rows? That's almost certainly a missing catalog entry. Add it, then re-run only the local pivot:

    ```bash
    python -m airtel_availability.pivot \
        --manifest out/run_<timestamp>/manifest.raw.tsv \
        --window-start 2021-04-15 --window-end 2026-04-15 \
        --catalog config/catalog.yaml \
        --date-patterns config/date_patterns.yaml \
        --run-id <timestamp> \
        --out out/run_<timestamp>.v2/
    ```

    Re-running the pivot takes seconds — no remote scan needed.

- **`availability.csv`** — eyeball: which sources show `files_total = 0` for long stretches? Are those genuine gaps (delivery never occurred) or classification misses (files exist but pattern didn't match)? Cross-reference against `unclassified.csv`.
- **`summary.xlsx`** — Overview sheet: coverage % per source. Flag anything at 0%.

## Step 4 — Load to audit DB

```sql
-- See §6.2 of the design spec for the canonical schema.
-- Example (PostgreSQL):
\copy airtel_data_availability FROM 'out/run_<timestamp>/availability.csv' WITH CSV HEADER;
```

## Step 5 — Sign-off checklist

Copy this list into your handover:

- [ ] All Layer 1–4 tests green (`pytest -q`).
- [ ] Remote smoke test executed and reviewed; `out/smoke/.../run_summary.txt` attached.
- [ ] Full run classification rate ≥ 90%.
- [ ] Full run scan errors == 0 (or explained in a one-page addendum).
- [ ] `unclassified.csv` top-10 signatures reviewed; backlog captured in the project tracker.
- [ ] `availability.csv` loaded into target audit DB; row count matches.
- [ ] `catalog.snapshot.yaml` archived alongside the run for reproducibility.
```

- [ ] **Step 2: Append runbook link to README.md**

Edit `C:\sandbox\URA\AIRTEL\README.md`, append to the end:

```markdown
## Operator runbook

See [`docs/superpowers/RUNBOOK.md`](docs/superpowers/RUNBOOK.md) for step-by-step operator instructions, including the remote smoke procedure and v1 sign-off checklist.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/RUNBOOK.md README.md
git commit -m "docs: add operator runbook with smoke + full-run + sign-off procedures"
```

---

## Spec coverage self-review

Walking each spec section against the plan:

- **§1 Goal, §2 Inputs** — covered by README and RUNBOOK + runner CLI flags.
- **§2.4 Audit window** — `--window-start` / `--window-end` flags, defaults documented in README.
- **§3 Architecture** — Tasks 8 (scan.sh), 9 (pivot), 10 (runner). Manifest is the sole intermediate artifact.
- **§3.2 Component responsibilities** — files match one-for-one: scan.sh, runner.py, pivot.py, classifier.py, date_extractor.py, catalog.yaml, date_patterns.yaml.
- **§4.1 Catalog file format** — Task 3 encodes the format in YAML.
- **§4.2 Seed patterns (19 sources)** — Task 3, every row represented.
- **§4.3 Classification rules** — Task 2's `Classifier.classify` implements them in priority order.
- **§4.4 Archive handling** — Task 8 enumerates tar entries; Task 9 classifies each entry independently.
- **§5 Date extraction** — Tasks 1 (engine + patterns YAML) and 9 (mtime fallback with `date_source` tracking).
- **§6.1 availability.csv columns** — Task 5 writes all 19 columns in order.
- **§6.2 DB schema** — Reference only; documented in RUNBOOK §Step 4.
- **§6.3 Secondary artifacts** — manifest.tsv/raw.tsv (Task 4/9), unclassified.csv (Task 6), summary.xlsx (Task 7).
- **§6.4 Output directory layout** — runner.py creates `run_<run_id>/` subdirectories; snapshots of configs copied in Task 9.
- **§7.1 Remote-side failures** — `nohup` + `.done` sentinel (Task 10), find errors to `--log` (Task 8).
- **§7.2 Classification failures** — Task 2 handles ambiguous/no-match; Task 8 archives, Task 9 enriches with mtime fallback.
- **§7.3 Local-side failures** — encoding='utf-8', errors='replace' (Task 4); run_id-named output dirs (Task 10).
- **§7.4 Reconciliation asserts** — Task 9's `_reconcile`.
- **§7.5 Run-summary banner** — Task 9's `_format_banner`.
- **§8.1–8.6 Testing layers** — Layer 1 (Task 3), Layer 2 (Task 1), Layer 3 (Task 5), Layer 4 (Task 9), Layer 5 (Task 11, manual), Layer 6 (Task 9's `_reconcile`).
- **§9 Risks** — addressed by unclassified.csv surfacing and date_confidence rollup.
- **§10 Acceptance criteria** — RUNBOOK §Step 5 checklist.

No gaps found.

**Placeholder scan:** checked — every step has concrete code, exact commands, and expected output. No "TBD" / "TODO" / "similar to above." `unclassified.csv` fixture for Task 6's second test inline-builds its `EnrichedRow`s; no shared fixtures skipped over.

**Type consistency:** classifier returns `ClassifyResult` (key, label, confidence); availability_writer consumes `EnrichedRow.source_key` and `EnrichedRow.confidence`; the column names in `AVAILABILITY_HEADERS` match the keys in the golden CSV. All consistent.

---

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-15-airtel-data-availability-plan.md`.

Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — execute tasks in this session with checkpoints.

Which approach would you like?
