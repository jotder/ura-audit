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
