# Airtel URA Data Availability Report — Design Spec

**Status:** Draft — awaiting user review
**Date:** 2026-04-15
**Audit purpose:** Generate a consolidated, DB-loadable data availability report covering 19 data sources submitted by Airtel across a 5-year audit window, scattered across five remote-filesystem roots and three submission "generations."

---

## 1. Goal

Produce, from a remote Airtel file-submission archive, a single CSV that answers per calendar day and per source: **"did a file land, how many, how big, and from which submission origin?"** The CSV is the load target for a downstream audit database.

**Explicit non-goals for v1 (this spec):**

- Opening files to validate column counts, schema, or row counts. *(Step B in the phased roadmap; separate spec.)*
- Parsing record contents or building a harmonized, deduplicated transaction set. *(Step C / Step D; separate specs.)*
- Verifying correctness of reported amounts, MSISDNs, or any business-level content.

---

## 2. Inputs

### 2.1 Remote access

- **Host:** `192.168.30.24`, port 22
- **User:** `gamma`
- **Access:** SSH and SFTP (confirmed).

Credentials are supplied by the operator at run time (env var, `~/.ssh/config`, or prompt). Credentials are **not** committed to this repository.

### 2.2 Filesystem roots to scan

```
/master_data1/airtel_ftp/airtel_ftpu
/master_data2/airtel_ftp/airtel_ftpu
/airtel_mobile_money
/resubmited_data
/unique_trnx
```

These represent three submission generations plus the canonical `unique_trnx` set plus the MoMo tree, which per operator note is "more better organized" than the rest.

### 2.3 Data sources to account for (19)

| # | Source | # | Source |
|---|---|---|---|
| 1 | AIR Recharge | 11 | Bank Statement Holding MoMo Float |
| 2 | AIR Subscription | 12 | HLR |
| 3 | AIR Adjustment | 13 | IN Voice |
| 4 | Me2U / Transfer | 14 | IN SMS |
| 5 | SDP | 15 | IN GPRS |
| 6 | Mobile Money Transactions | 16 | VAS (PSMS, RBT, MMS, …) |
| 7 | Mobile Money Balance Snapshot | 17 | MSC |
| 8 | Voucher Management System (Physical) | 18 | Postpaid Billing / Invoicing |
| 9 | Voucher Management System (Electronic) | 19 | Postpaid Payments |
| 10 | Dealer Management System | | |

### 2.4 Audit window

Default: `2021-04-15` through `2026-04-15` (rolling 5 years ending today). Operator may override via CLI flag.

### 2.5 Format signals already known

- Files exist as plain `.csv` / `.add`, as `*.csv.gz`, and as `.tar` / `.tar.gz` archives containing multiple files (some sub-files independently gzipped).
- Delimiter and date formats vary between submissions.
- Distinctive CBS/OCS CDR column counts observed: **76, 116, 136, 533, 534, 536, 537**. *(Captured only where embedded in the filename in v1; full column validation is deferred to Step B.)*
- Per operator note, Voucher files are identifiable by `vou` / `VOU` in the filename.

---

## 3. Architecture

```
   ┌──────────────────────────┐       scp        ┌───────────────────────────┐
   │ REMOTE  192.168.30.24    │   manifest.tsv   │ LOCAL  C:\sandbox\URA\... │
   │ (runs as user `gamma`)   │ ───────────────▶ │                           │
   │                          │                  │                           │
   │  scan.sh                 │                  │   pivot.py                │
   │   ├─ find(5 roots)       │                  │    ├─ reads manifest.tsv  │
   │   ├─ classify(patterns)  │                  │    ├─ applies filename→   │
   │   ├─ enumerate archives  │                  │    │   date regex         │
   │   └─ emit manifest.tsv   │                  │    ├─ pivots to           │
   │                          │                  │    │   (source × date ×   │
   │  OUTPUT:                 │                  │    │    origin) matrix    │
   │   /tmp/airtel_manifest_  │                  │    └─ writes:             │
   │    <runid>.tsv           │                  │         availability.csv  │
   │                          │                  │         summary.xlsx      │
   │   /tmp/airtel_scan_      │                  │         unclassified.csv  │
   │    <runid>.log           │                  │                           │
   └──────────────────────────┘                  └───────────────────────────┘
```

Two artifacts on the remote; three on Windows. The remote **manifest.tsv** is the single point of truth — every number in the downstream CSVs is derivable from it. Re-running the Windows pivot is cheap; re-running the remote scan is the expensive step, so it is designed to be restart-safe and to emit a complete, persistent intermediate artifact.

### 3.1 Why this shape

Chosen over two alternatives:

- **Python-over-SSH streaming** (no intermediate file) — rejected because a dropped SSH session at hour N means restarting from zero. The manifest-on-remote approach keeps the expensive work server-side and resilient.
- **SFTP directory walk from Windows** — rejected at scale: each directory traversal is a round trip, and the tree holds millions of files across 5 years.

### 3.2 Component responsibilities

- **`scan.sh`** (remote) — single bash script. Walks the five roots with `find -type f`, enumerates entries of any tar/tar.gz archives, writes one TSV row per file (or per archive entry) to `/tmp/airtel_manifest_<runid>.tsv`. **No classification and no date parsing happens on the remote** — `scan.sh` emits raw metadata only: `path, origin, size, mtime, archive_parent, archive_entry`. This is deliberate so that catalog fixes and date-pattern fixes never require re-running the expensive remote scan. Writes errors to `/tmp/airtel_scan_<runid>.log`. Drops a `.done` sentinel on success.
- **`runner.py`** (local) — orchestrates: opens SSH, uploads `scan.sh` to `/tmp`, runs it under `nohup`, waits for the `.done` sentinel (or reconnects if the SSH link breaks), `scp`-pulls the manifest and log, invokes the pivot.
- **`pivot.py`** (local) — reads the raw manifest, applies the catalog (source classification) and date-pattern regex (date extraction), enriches each row with `source_key, confidence, date, date_source`, emits `availability.csv`, `unclassified.csv`, and a `summary.xlsx` workbook. Also emits an *enriched* `manifest.tsv` copy (raw + classification/date columns) into the run directory — this is the full audit trail.
- **`catalog.yaml`** (local, edited by hand) — the authoritative source-pattern catalog. Re-read every run.
- **`date_patterns.yaml`** (local) — regex catalog for extracting dates from filenames.

---

## 4. Source-pattern catalog

### 4.1 Catalog file format

```yaml
- key: air_recharge
  label: "AIR Recharge"
  patterns:
    - '(?i)(^|[/_-])rch([_.-]|$)'
    - '(?i)cbs_cdr_rch_'
    - '(?i)ocs_rch_'
  path_hints: []     # optional — require the path to contain any of these substrings
  confidence: high   # informational only; downgraded to 'ambiguous' at run time if multiple catalog entries match
  tests:
    - {name: "cbs_cdr_rch_20230412.add",              expect: air_recharge}
    - {name: "merchant_20230412.csv",                 expect: ''}
```

- Patterns are Python regex strings. Case-insensitive by convention (`(?i)` prefix).
- `path_hints` supports MoMo-style rules: "only classify as MoMo if under `/airtel_mobile_money`."
- `tests` embedded in the catalog are run by `tests/test_catalog.py` — seeding ~5 cases per source, positive and negative.

### 4.2 v1 seed patterns

| # | Source key | Pattern hints (regex, case-insensitive) | Path hint |
|---|---|---|---|
| 1 | `air_recharge` | `rch`, `cbs_cdr_rch`, `recharge` | |
| 2 | `air_subscription` | `sub`, `cbs_cdr_sub`, `subscription` | |
| 3 | `air_adjustment` | `adj`, `mon` (monetary), `cbs_cdr_adj` | |
| 4 | `me2u_transfer` | `me2u`, `trf`, `transfer`, `cbs_cdr_trf` | |
| 5 | `sdp` | `sdp`, `cbs_cdr_sdp` | |
| 6 | `momo_transactions` | `txn`, `trans`, `transactions` | `/airtel_mobile_money` required |
| 7 | `momo_balance_snapshot` | `bal`, `snapshot`, `snap` | `/airtel_mobile_money` required |
| 8 | `vms_physical` | `vou.*phys`, `pvou`, `vms_p`, `physical.*vou` | |
| 9 | `vms_electronic` | `vou.*elec`, `evou`, `vms_e`, `cbs_cdr_vou` | |
| 10 | `dealer_management` | `dms`, `dealer` | |
| 11 | `bank_momo_float` | `bank`, `float`, `stmt`, `statement` | |
| 12 | `hlr` | `hlr` | |
| 13 | `in_voice` | `voi`, `cbs_cdr_voi`, `ocs.*voi`, `voice` | |
| 14 | `in_sms` | `sms`, `cbs_cdr_sms` (excluding `psms` which → VAS) | |
| 15 | `in_gprs` | `gprs`, `cbs_cdr_gprs`, `data_cdr` | |
| 16 | `vas_other` | `psms`, `rbt`, `mms`, `vas` | |
| 17 | `msc` | `msc` | |
| 18 | `postpaid_billing` | `postpaid.*(inv|bill)`, `pp.*inv`, `invoice` | |
| 19 | `postpaid_payments` | `postpaid.*pay`, `pp.*pay`, `payment` | |

*These are seed patterns. They will be refined in v1.1 once the first scan's `unclassified.csv` is reviewed.*

### 4.3 Classification rules (applied in priority order)

1. **Explicit pattern match.** If exactly one catalog entry's regex matches the basename (after path-hint filtering), assign `source_key` to that entry.
2. **Path-hint override.** `path_hints` must match before pattern consideration — MoMo rules never bleed into non-MoMo roots.
3. **Ambiguous match.** If 2+ catalog entries match, first-listed wins; row is tagged `confidence = ambiguous`. Ambiguous pair counts are surfaced in `summary.xlsx`.
4. **No match.** `source_key = ''`. Row still recorded in `manifest.tsv`. Aggregated by prefix in `unclassified.csv`. Does not contribute to any source's availability count.

### 4.4 Archive handling

- `.gz` (single-file): treated as one unit; classification and date extraction use the un-suffixed filename (`foo_20230412.csv.gz` → as if `foo_20230412.csv`).
- `.tar`, `.tar.gz`, `.tgz`: entries are enumerated via `tar -tzf` (metadata only — no extraction). Each entry is an independent manifest row, classified independently, with `archive_parent` recording the tarball path.
- Nested archives (tarball inside tarball): enumerated one level deep. Deeper-nested items recorded as a single opaque row. Extending this is out of scope for v1.
- Corrupt archive (tar exits non-zero): one row with `source_key = ''`, `date_source = 'archive_unreadable'`. Visible in `unclassified.csv`, not silently dropped.

---

## 5. Date extraction

### 5.1 Regex patterns

Applied in order to the basename (and to archive-entry names for archive members):

1. `(?<![0-9])(20\d{2})[-_]?(0[1-9]|1[0-2])[-_]?(0[1-9]|[12]\d|3[01])(?![0-9])`
   — matches `20230412`, `2023-04-12`, `2023_04_12`, with a negative-lookaround guard so `20230412999` or `120230412` do **not** match.
2. `(?<![0-9])(0[1-9]|[12]\d|3[01])[-_](0[1-9]|1[0-2])[-_](20\d{2})(?![0-9])`
   — matches `12-04-2023` style (DMY).

If any pattern matches, `date_source = 'filename'`. Otherwise, fall back to file `mtime`, set `date_source = 'mtime'`.

### 5.2 Date confidence roll-up

At pivot time, each `(source, date)` row gets a `date_confidence`:

- `filename` — 100% of contributing rows had filename-derived dates.
- `mtime` — 100% fell back to `mtime`.
- `mixed` — any mix.

A source with heavy `mtime` reliance is flagged in the Overview sheet because "date of delivery" is a weaker signal than "business date."

---

## 6. Output schema

### 6.1 `availability.csv` — primary DB-load artifact

One row per `(source × date)` in the audit window. Full grid — dates with `files_total = 0` are emitted so gaps are first-class, queryable rows.

| Column | Type | Description |
|---|---|---|
| `run_id` | string (ISO-8601) | Timestamp of this scan. Allows multiple runs in one table. |
| `source_key` | string | Catalog key, e.g. `air_recharge`. |
| `source_label` | string | Human label. |
| `date` | date (`YYYY-MM-DD`) | Calendar date. |
| `files_total` | int | Sum across all origins. Zero indicates a gap. |
| `bytes_total` | int | Sum of file sizes across all origins. |
| `files_master_data1` | int | Count in `/master_data1/airtel_ftp/airtel_ftpu`. |
| `files_master_data2` | int | Count in `/master_data2/airtel_ftp/airtel_ftpu`. |
| `files_mobile_money` | int | Count in `/airtel_mobile_money`. |
| `files_resubmited` | int | Count in `/resubmited_data`. |
| `files_unique_trnx` | int | Count in `/unique_trnx`. |
| `bytes_master_data1`…`bytes_unique_trnx` | int × 5 | Byte totals per origin. |
| `has_archive` | bool | At least one contributing file was a tar/gz member. |
| `date_confidence` | enum | `filename` / `mtime` / `mixed`. |
| `min_mtime` | ISO-8601 | Earliest mtime of contributing files. |
| `max_mtime` | ISO-8601 | Latest mtime of contributing files. |

Approximate size: 19 sources × 1,825 days ≈ 34,675 rows. Single-digit MB.

### 6.2 Downstream DB table (informational)

```sql
CREATE TABLE airtel_data_availability (
    run_id              TIMESTAMP    NOT NULL,
    source_key          VARCHAR(64)  NOT NULL,
    source_label        VARCHAR(128) NOT NULL,
    date                DATE         NOT NULL,
    files_total         INTEGER      NOT NULL,
    bytes_total         BIGINT       NOT NULL,
    files_master_data1  INTEGER      NOT NULL,
    files_master_data2  INTEGER      NOT NULL,
    files_mobile_money  INTEGER      NOT NULL,
    files_resubmited    INTEGER      NOT NULL,
    files_unique_trnx   INTEGER      NOT NULL,
    bytes_master_data1  BIGINT       NOT NULL,
    bytes_master_data2  BIGINT       NOT NULL,
    bytes_mobile_money  BIGINT       NOT NULL,
    bytes_resubmited    BIGINT       NOT NULL,
    bytes_unique_trnx   BIGINT       NOT NULL,
    has_archive         BOOLEAN      NOT NULL,
    date_confidence     VARCHAR(16)  NOT NULL,
    min_mtime           TIMESTAMP,
    max_mtime           TIMESTAMP,
    PRIMARY KEY (run_id, source_key, date)
);
```

### 6.3 Secondary artifacts

- **`manifest.tsv`** — the enriched manifest emitted by `pivot.py` (written into the run directory alongside a raw copy pulled from the remote). One row per file or archive entry. Columns: `path, origin, size, mtime, archive_parent, archive_entry, source_key, confidence, date, date_source`. Audit trail behind every number in `availability.csv`. The remote-produced raw manifest (without classification columns) is preserved as `manifest.raw.tsv` in the same directory.
- **`unclassified.csv`** — files where `source_key = ''`. Aggregated by **signature + origin**. A signature is computed from the basename by lowercasing, stripping any embedded date, trimming extensions, and replacing runs of digits with `#` (e.g. `CBS_CDR_XYZ_20230412_001.add` → `cbs_cdr_xyz_#`). Columns: `signature, origin, count, earliest_mtime, latest_mtime, example_path_1, example_path_2, example_path_3`. Sorted by count descending — the biggest unknown buckets are the highest-leverage catalog edits.
- **`summary.xlsx`** — human-readable workbook:
  - **Sheet 1 — Overview:** one row per source (coverage %, first date with data, last date with data, total files, total GB, gap count, date-source mix, ambiguous count).
  - **Sheets 2–20 — per source:** heatmap-style pivot of `date (rows) × origin (cols)` with conditional formatting so gaps pop visually.
  - **Final sheet — Run stats:** classification rate, date-source mix, ambiguous-pair counts, pre-window file counts, scan elapsed time, scan-error count.

### 6.4 Output directory layout

```
C:\sandbox\URA\AIRTEL\out\
  run_2026-04-15T12-34-56Z\
    manifest.raw.tsv          # pulled verbatim from remote; no classification
    manifest.tsv              # enriched by pivot.py (adds source_key, date, etc.)
    scan.log                  # remote-side find / tar stderr
    availability.csv          # primary DB-load artifact
    unclassified.csv
    summary.xlsx
    run_summary.txt           # the console summary, also saved
    catalog.snapshot.yaml     # the exact catalog applied (for reproducibility)
    date_patterns.snapshot.yaml
```

Nothing is ever overwritten. Each run gets its own directory named by `run_id`.

---

## 7. Error handling

### 7.1 Remote-side

| Failure | Handling |
|---|---|
| SSH session drops mid-scan | `scan.sh` is run under `nohup` with output to a file. Reconnect and `scp`. A `.done` sentinel file is flipped only at the very end; its absence means the manifest is partial. |
| `find` hits a permission-denied directory | Captured via `2>` redirect into `scan.log`. Scan continues past it. If error count > 0, run summary surfaces it at the top. |
| Broken symlink / dangling inode | Excluded by `find -type f`. Logged. |
| Corrupt archive unreadable to `tar` | One manifest row with `source_key = ''`, `date_source = 'archive_unreadable'`. Visible in `unclassified.csv`. |
| Filesystem still being written during scan | `find` is a snapshot of "now"; `run_id` documents the moment. Later runs pick up newer files. |
| `/tmp` low on disk | `df /tmp` checked at start; abort with clear error if under 5 GB free. |

### 7.2 Classification-side

| Situation | Handling |
|---|---|
| Multiple catalog entries match | First-listed wins, `confidence = ambiguous`. Ambiguous-pair counts in `summary.xlsx`. |
| No catalog entry matches | `source_key = ''`. Row kept in manifest; aggregated in `unclassified.csv`. |
| Filename date unparseable | Fall back to `mtime`, `date_source = 'mtime'`. |
| Filename date outside audit window | Row kept in manifest. Excluded from `availability.csv` grid. Counted as `pre_window_files` / `post_window_files` in Overview. |
| Nested archive beyond one level | Enumerated one level; deeper items recorded as one opaque row. Logged. |

### 7.3 Local-side (pivot)

| Failure | Handling |
|---|---|
| Encoding oddities in manifest | Python opens with `encoding='utf-8', errors='replace'`. Count of replaced bytes logged. |
| Output collision | `run_id` is part of directory name; nothing overwritten. |
| Source with zero files | Still emits full grid (1,825 rows). Overview sheet flags `coverage = 0%`. |

### 7.4 Post-run reconciliation (hard asserts)

Computed at the end of every run; a failure aborts with non-zero exit so a wrapper script notices:

- `sum(files_total) in availability.csv == count of in-window, classified rows in manifest.tsv`. If not equal, pivot has a bug.
- Classification rate = classified / total. Warn if < 90%.
- Date-source mix warning if > 30% of rows use `mtime`.

### 7.5 Run summary banner

Printed to stdout at the end, also saved to `run_summary.txt`:

```
RUN OK     run_id=2026-04-15T12-34-56Z
scanned    24,182,104 files     across 5 origins   elapsed=00:47:32
classified 23,950,881 files  →  19 sources
unclassified  231,223 files  →  47 prefix groups  (see unclassified.csv)
ambiguous       12,804 files  →  3 overlapping source pairs (see summary.xlsx)
archives        18,402 tarballs  → 2,341,009 entries enumerated
scan errors          0          (from scan.log)
```

---

## 8. Testing

### 8.1 Layer 1 — Catalog tests (fast, no SSH)

Every catalog entry carries `tests:` cases. `tests/test_catalog.py` iterates and asserts. Target: ~5 cases per source (positive + negative), ~100 total. Adding a source requires adding its tests.

### 8.2 Layer 2 — Date extractor tests

`tests/test_date_extraction.py` with a table of `(filename, expected_date, expected_source)` tuples. Every case carries a one-line comment documenting why.

### 8.3 Layer 3 — Pivot logic golden file

`tests/fixtures/tiny_manifest.tsv` holds ~50 synthetic rows. Running `pivot.py` against it must match `tests/fixtures/tiny_availability.golden.csv` after line-ending normalization (both files read as text with universal newlines before diff). Covers: multi-source-same-date, ambiguous rows, archive entries, zero-file sources, mixed date sources.

### 8.4 Layer 4 — Local end-to-end smoke

`tests/fixtures/fake_tree/` mimics the five-root layout with ~100 files (including gzips, one tarball, a simulated permission-denied folder). `scan.sh --roots tests/fixtures/fake_tree` + `pivot.py` produces output that matches `tests/fixtures/fake_tree.golden.csv` under the same normalized-text comparison. Run times (mtime-dependent columns `min_mtime` / `max_mtime`) are excluded from the comparison and asserted separately against a tolerance.

### 8.5 Layer 5 — Remote smoke

Before any full run, run `scan.sh --limit-one-subdir /master_data1/airtel_ftp/airtel_ftpu/2024/01/` (or similar small subset). Verify:

1. Manifest has a plausible number of rows.
2. At least one row resolves into each expected source for that window.
3. `unclassified.csv` is not dominated by unknowns.
4. Extrapolated full-run elapsed time is acceptable.

### 8.6 Layer 6 — Full-run automatic sanity

The hard asserts from §7.4 are part of `pivot.py` and abort the run on failure.

### 8.7 Testing philosophy

- Cannot unit-test "did we scan the real server correctly" — only Layer 5 can. So the tool is designed to make the remote step cheap to re-run.
- Catalog tests are the highest-leverage tests — they encode every "forgot about `vou_prepaid_…`" moment so regressions are immediate.
- No filesystem or SSH mocking. Layer 4 uses a real small local tree; Layer 5 uses the real remote.

---

## 9. Risks and open questions

### 9.1 Risks

- **Pattern catalog incompleteness** — guaranteed to be wrong on the first run; mitigated by `unclassified.csv` being a first-class deliverable.
- **`mtime` reliance for files without embedded dates** — produces availability rows anchored to delivery day, not business day. Flagged via `date_confidence` so downstream auditors can discount affected sources.
- **Tarballs with heterogeneous contents** — a single tarball may legitimately contain rows for multiple sources and dates; handled by independent classification of each tar entry.
- **Remote-side Python / tool availability** — `scan.sh` deliberately depends on `bash`, `find`, `awk`, `tar`, `gzip`, `df` — all POSIX-standard. No Python dependency on remote.

### 9.2 Deferred to future specs

- Step B — integrity: open files, verify non-empty, verify column counts 76/116/136/533/534/536/537, verify delimiter, verify encoding.
- Step C — record counts: count rows per file, extract first/last business dates from contents, dedup signal across generations.
- Step D — harmonization: parse into a unified schema per source, compute canonical transaction set.

---

## 10. Acceptance criteria for v1

- `availability.csv` produced covering all 19 sources × 1,825 days (full grid) for the audit window.
- Classification rate ≥ 90% on the full real scan (measured on the Run summary banner).
- All Layer 1–4 tests pass.
- Layer 5 remote smoke test documented with its output attached to the v1 sign-off.
- `unclassified.csv` reviewed; follow-up catalog additions captured as v1.1 backlog items (not blocking v1).
- Final artifacts load cleanly into the target audit DB via a documented `LOAD DATA` / equivalent statement.
