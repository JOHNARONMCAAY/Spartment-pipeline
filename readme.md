# Spartment Pipeline

Operational and analytical batch data pipeline for the Spartment apartment management system.

Stage 1 extracts the operational tables from Supabase into `rawdata/`, validates them, and records an acceptance decision. Stage 2 (transformation) is specified but not implemented; Stage 3 (load) is reserved.

## Documentation

- [Stage 1: Data Extraction](docs/extraction.md) — extraction specification, data contract, and operations runbook
- [Stage 2: Data Transformation](docs/transformation.md) — transformation specification and output contract

## Layout

```
config/extraction.toml        non secret settings
dags/                         Airflow DAGs
src/spartment/
  common/                     config, models, JSON writers, retention
  stage1_extract/             contract, source, guard, extract, validate, handover, cli
  stage2_transform/           reserved
  stage3_load/                reserved
migrations/                   SQL migrations for the source database
docs/                         stage specifications
rawdata/                      staged output, not version controlled
```

Stages are independent packages. Stage 2 reads `rawdata/` and imports `spartment.common.models`; it never imports Stage 1 internals.

## Setup

```bash
pip install -e .
cp .env.example .env      # then fill in SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY
```

The `.env` file lives at the repository root and is excluded from version control. A database superuser password is never required.

If the console scripts are not on your PATH, use the module form:

```bash
python -m spartment.stage1_extract.cli
python -m spartment.common.retention
```

## Commands

```bash
spartment-extract                        # extract, validate, gate
spartment-extract --tables rooms,bills   # subset of tables
spartment-extract --strict-contract      # reject on any schema drift
spartment-extract --no-extra-columns     # contract columns only
spartment-extract --skip-validation      # debugging only

spartment-retain                         # retention dry run, nothing removed
spartment-retain --apply                 # perform deletions
```

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | `APPROVED` |
| 1 | Extraction failure |
| 2 | Configuration or argument error |
| 3 | Schema unreachable |
| 4 | `REJECTED` |
| 5 | `MANUAL REVIEW` |

## Output

```
rawdata/<table>/<run_date>/<table>.csv   staged dataset
rawdata/_metadata/<run_id>.json          per table run record
rawdata/_validation/<run_id>.json        validation checks
rawdata/_handover/<run_id>.json          acceptance decision and handover package
```

Run ids are `spartment-extract-<date>-<HHMM>`, with a counter suffix if the same minute is reused, so no record is overwritten.

## Schedule

The Airflow DAG `spartment_daily_extraction` runs at 01:00 Asia/Manila daily with 3 retries and exponential backoff. Tasks: `extract_and_validate` → `acceptance_gate` → `retain`. Credentials come from the Airflow connection `spartment_postgres`.