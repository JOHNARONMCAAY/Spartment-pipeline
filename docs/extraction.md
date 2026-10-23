# Spartment: Operational and Analytical Batch Data Pipeline

## Stage 1: Data Extraction

### Objective

Retrieve the operational records required from the Spartment Supabase database, validate them at extraction level, and hand a documented, traceable, and accepted dataset to the transformation stage.

The extracted data supports room availability and occupancy monitoring, tenant assignment tracking, billing monitoring, prospective tenant inquiries, and tenant request analysis.

> Extraction performs retrieval and extraction level validation only. Data cleaning, standardization, analytical joins and business calculations belong to the transformation stage.

This document describes the pipeline as implemented in `src/spartment/` and `dags/`. Every column list and data type below was verified against the live PostgREST schema, not inferred. The downstream stage is specified in [transformation.md](transformation.md).

---

## 1. Data Source and Extraction Specification

### 1.1 Source System

| Item | Description |
| --- | --- |
| **Source System** | Spartment Apartment Management System |
| **Purpose** | Manages apartment buildings and rooms, tenant assignments, billing records, prospective tenant inquiries, and tenant requests. |
| **Source Database** | Supabase PostgreSQL |
| **Database management system** | PostgreSQL (Supabase managed) |
| **Access interface** | Supabase PostgREST API over HTTPS |
| **File format** | Not applicable at source — direct database retrieval. |
| **Staged format** | CSV with a companion JSON column type manifest |

PostgREST was selected over a direct `psycopg` connection deliberately. It avoids distributing a database superuser password, and the client in `src/spartment/stage1_extract/source.py` constructs GET requests only, so an extraction run is structurally incapable of mutating the source even though it authenticates with a privileged role.

### 1.2 Extraction Method

| Item | Description |
| --- | --- |
| **Extraction tool** | Python 3.13, standard library only. No third party runtime dependency. |
| **Orchestration tool** | Apache Airflow 2.7 or later |
| **Extraction type** | Scheduled batch |
| **Extraction mode** | Full extraction. Incremental is not enabled, see 1.3. |
| **Read mechanism** | PostgREST with `Prefer: count=exact` and `Range` pagination ordered by `id` |
| **Configuration** | `config/extraction.toml` for non secret settings |
| **Raw staging** | `rawdata/<table_name>/<run_date>/<table_name>.csv` |
| **Run records** | `rawdata/_metadata/`, `rawdata/_validation/`, `rawdata/_handover/` |

Extraction sequence:

1. Airflow starts the scheduled DAG at 01:00 Asia/Manila.
2. The CLI loads credentials from the environment and authenticates to PostgREST.
3. The schema guard fetches the OpenAPI schema root and diffs it against `contract.py`.
4. Python pages each conforming table into a CSV file under `rawdata/`.
5. Extraction level validation reads the staged CSV files and runs the checks in section 3.
6. The section 7 acceptance gate records a decision and writes the handover package.

**No transformation is performed here.** The only value adaptations are format level and required by CSV itself: a SQL `NULL` becomes an empty field, a boolean becomes `true` or `false`, and a `jsonb` value becomes compact JSON text. No trimming, case folding, type coercion, deduplication or unit conversion occurs. Original column names and values are preserved.

### 1.3 Extraction Window

Full extraction of all records from the selected tables. Incremental extraction is not enabled because no table yet has both a reliable change tracking field and verified maintenance of that field.

| Table | Candidate watermark | Status |
| --- | --- | --- |
| `rooms` | `updated_at` | **Unverified.** Values are populated and vary per row, but no trigger maintaining the column exists in `migrations/`. |
| `tenant_requests` | `updated_at` | **Best candidate.** A `set_updated_at()` trigger is referenced but the function is not defined in `migrations/`, so it must be confirmed in the deployed database. |
| `inquiries` | `created_at` | New inquiries only. Captures no later status change. |
| `tenant_occupancy_events` | `created_at` | New events only. |
| `payments` | `created_at` | New payments only. |
| `payment_corrections` | `created_at` | New corrections only. |
| `profiles` | `created_at` | New profiles only. |
| `buildings` | `created_at` | New buildings only. |
| `leases` | none identified | Full extraction required. |
| `tenant_assignments` | none identified | Full extraction required. |
| `bills` | none identified | Full extraction required. |

When incremental extraction is introduced it must use a stored successful run watermark, an overlap window where a watermark is not unique, and primary key based deduplication. The watermark must advance only after extraction and validation both succeed. No candidate field may be assumed to capture every change until its maintenance has been confirmed against the deployed database.

### 1.4 Extraction Schedule

| Item | Value |
| --- | --- |
| Frequency | Daily |
| Airflow DAG | `spartment_daily_extraction` |
| Scheduled time | 01:00 Asia/Manila daily (`0 1 * * *`) |
| Concurrency | `max_active_runs=1` |
| Retry | Up to 3 retries, 5 minute base delay, exponential backoff, 1 hour maximum |
| Catchup | Disabled |
| DAG tasks | `extract_and_validate` → `acceptance_gate` → `retain` |

The gate task fails the run when the decision is `REJECTED`, and logs a warning while continuing when the decision is `MANUAL REVIEW`.

### 1.5 Extraction Scope

| Table | Purpose |
| --- | --- |
| `profiles` | Identify tenant and staff records needed to interpret assignments and requests. |
| `buildings` | Identify buildings for room level and building level reporting. |
| `rooms` | Monitor room availability, capacity, location and rental rates. |
| `leases` | The tenancy spine. Required to resolve `bills.lease_id` and both lease references on occupancy events. |
| `tenant_assignments` | Monitor tenant to room assignments and occupancy periods. |
| `bills` | Monitor billing periods, bill components, due dates and bill status. |
| `payments` | The cash receipt ledger. All revenue measures derive from payments, not from bill status. |
| `payment_corrections` | Audit record of payments mutated in place. Without it a corrected payment is invisible. |
| `inquiries` | Monitor prospective tenant inquiries and room interest. |
| `tenant_requests` | Monitor tenant requests, priorities and request status. |
| `tenant_occupancy_events` | Track occupancy and lease transition events. |
| `notifications` | Polymorphic audit trail recording bills, payments, request status changes and tenancy transfers. |

> **Scope note.** Authentication and password recovery data — `auth.users`, `password_recovery_sessions`, `password_recovery_limits`, `login_attempt_limits` — is out of scope. Two relationship columns target `auth.users` and are reported as unverifiable rather than silently skipped.

---

## 2. Source Tables and Column Specification

### 2.1 Extraction Tables and Verified Columns

12 tables, 98 columns. Types were read from the live PostgREST schema root at run time and are reproduced in each run's `_handover` record under `data_contract.column_manifest`.

#### `profiles` — 5 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Unique profile identifier |
| `full_name` | `text` | — | Profile name |
| `phone` | `text` | — | Contact reference |
| `role` | `public.app_role` | — | User role; observed `admin`, `tenant` |
| `created_at` | `timestamptz` | — | Record creation timestamp |

#### `buildings` — 4 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Unique building identifier |
| `name` | `text` | — | Building name |
| `address` | `text` | — | Building address |
| `created_at` | `timestamptz` | — | Record creation timestamp |

#### `rooms` — 11 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Unique room identifier |
| `building_id` | `uuid` | → `buildings.id` | Building containing the room |
| `room_number` | `text` | — | Room number |
| `floor` | `integer` | — | Floor location |
| `status` | `public.room_status` | — | Room status; observed `available`, `occupied` |
| `size_sqm` | `numeric` | — | Room size in square meters |
| `capacity` | `text` | — | Capacity as defined by the schema |
| `monthly_rate` | `numeric` | — | Monthly room rate |
| `created_at` | `timestamptz` | — | Record creation timestamp |
| `updated_at` | `timestamptz` | — | Last update timestamp, not trigger verified |
| `description` | `text` | — | Room description |

#### `leases` — 6 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Lease identifier |
| `tenant_id` | `uuid` | → `profiles.id` | Tenant holding the lease |
| `room_id` | `uuid` | → `rooms.id` | Leased room |
| `move_in_date` | `date` | — | Lease start |
| `lease_end_date` | `date` | — | Lease end |
| `status` | `public.lease_status` | — | Lease state; observed `active` |

#### `tenant_assignments` — 13 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Assignment identifier, also references `auth.users.id` |
| `room_id` | `uuid` | → `rooms.id` | Assigned room; NULL when the assignment is `ended` |
| `full_name` | `text` | — | Tenant name |
| `email` | `text` | — | Tenant email |
| `phone` | `text` | — | Tenant phone number |
| `monthly_rent` | `numeric` | — | Monthly rent |
| `move_in` | `date` | — | Move in date |
| `lease_end` | `date` | — | Lease end date, may be NULL |
| `status` | `text` | — | Assignment state; observed `active`, `ended` |
| `created_at` | `timestamptz` | — | Record creation timestamp |
| `emergency_name` | `text` | — | Emergency contact name |
| `emergency_phone` | `text` | — | Emergency contact phone |
| `relationship` | `text` | — | Relationship of emergency contact |

`tenant_assignments` has **no** `updated_at` column and no `lease_id`. The assignment to lease link is implicit: `leases.tenant_id = tenant_assignments.id`.

#### `bills` — 10 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Unique bill identifier |
| `lease_id` | `uuid` | → `leases.id` | Lease the bill applies to |
| `bill_type` | `text` | — | Bill category |
| `billing_period` | `date` | — | Billing period |
| `due_date` | `date` | — | Payment due date |
| `rent_amount` | `numeric` | — | Rent component |
| `electricity_amount` | `numeric` | — | Electricity component |
| `water_amount` | `numeric` | — | Water component |
| `status` | `public.bill_status` | — | Bill status; observed `pending`, `partially_paid`, `paid` |
| `created_at` | `timestamptz` | — | Record creation timestamp |

#### `payments` — 6 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Unique payment identifier |
| `bill_id` | `uuid` | → `bills.id` | Bill the payment settles |
| `amount` | `numeric` | — | Amount received |
| `payment_date` | `date` | — | Date of payment |
| `reference_note` | `text` | — | Free text reference |
| `created_at` | `timestamptz` | — | Record creation timestamp |

#### `payment_corrections` — 7 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Correction identifier |
| `payment_id` | `uuid` | → `payments.id` | Corrected payment |
| `corrected_by` | `uuid` | → `auth.users.id` | Actor; may be NULL |
| `reason` | `text` | — | Reason for the correction |
| `previous_values` | `jsonb` | — | Values before correction |
| `corrected_values` | `jsonb` | — | Values after correction |
| `created_at` | `timestamptz` | — | Record creation timestamp |

#### `inquiries` — 10 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Unique inquiry identifier |
| `full_name` | `text` | — | Inquirer's name |
| `email` | `text` | — | Inquirer's email |
| `phone` | `text` | — | Inquirer's phone number, may be NULL |
| `preferred_room` | `text` | — | Preferred room |
| `move_in_date` | `date` | — | Requested move in date |
| `status` | `text` | — | Inquiry status; observed `New`, `Contacted`, `Resolved` |
| `created_at` | `timestamptz` | — | Inquiry creation timestamp |
| `message` | `text` | — | Inquiry body |
| `room_id` | `uuid` | → `rooms.id` | Room associated with the inquiry |

`room_id` is **optional** in the contract. It is absent from every file in `migrations/` but present in the deployed database, so the extractor selects it when the schema exposes it and omits it otherwise.

#### `tenant_requests` — 11 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Unique request identifier |
| `tenant_id` | `uuid` | → `profiles.id` | Submitting tenant |
| `room_id` | `uuid` | → `rooms.id` | Related room; NULL when `kind` is `other` |
| `kind` | `text` | — | Request category; observed `maintenance`, `other` |
| `subject` | `text` | — | Request subject |
| `priority` | `text` | — | `Normal`, `High`, `Urgent` |
| `status` | `text` | — | `New`, `Scheduled`, `In progress`, `Resolved` |
| `created_at` | `timestamptz` | — | Request creation timestamp |
| `updated_at` | `timestamptz` | — | Last update timestamp, trigger maintained |
| `message` | `text` | — | Request body |
| `admin_note` | `text` | — | Administrator note |

#### `tenant_occupancy_events` — 7 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Unique event identifier |
| `tenant_id` | `uuid` | → `profiles.id` | Tenant involved |
| `previous_lease_id` | `uuid` | → `leases.id` | Prior lease |
| `new_lease_id` | `uuid` | → `leases.id` | New lease; NULL indicates move out |
| `performed_by` | `uuid` | → `profiles.id` | Actor |
| `created_at` | `timestamptz` | — | Event timestamp |
| `details` | `jsonb` | — | Move date, room, rent and end date payload |

#### `notifications` — 8 columns

| Column | Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `uuid` | PK | Unique notification identifier |
| `tenant_id` | `uuid` | → `profiles.id` | Recipient |
| `title` | `text` | — | Notification title |
| `message` | `text` | — | Notification body |
| `kind` | `text` | — | `bill`, `payment`, `request`, `tenancy` |
| `source_id` | `uuid` | — | Polymorphic pointer to the originating record |
| `is_read` | `boolean` | — | Read state |
| `created_at` | `timestamptz` | — | Record creation timestamp |

### 2.2 Verification Notes

- `migrations/` does not fully describe the deployed database. It creates only `tenant_assignments`, `tenant_requests`, `tenant_occupancy_events`, `payment_corrections`, `password_recovery_sessions`, `password_recovery_limits` and `login_attempt_limits`. Eight of the twelve extracted tables are assumed to pre-exist, and `inquiries.room_id` exists in the database without appearing in any migration.
- The live schema is the source of truth. The schema guard reconciles the contract against the PostgREST schema root on every run and quarantines any non conforming table.

### 2.3 Contract Summary

The machine readable source of truth is `src/spartment/stage1_extract/contract.py`; this section is its human readable companion.

| Table | Columns | Required identifiers | Purpose |
| --- | --- | --- | --- |
| `profiles` | 5 | `id`, `role` | Tenant and staff identity |
| `buildings` | 4 | `id`, `name` | Building identification |
| `rooms` | 11 | `id`, `status` | Availability, capacity, rates |
| `leases` | 6 | `id`, `tenant_id`, `move_in_date` | Tenancy spine |
| `tenant_assignments` | 13 | `id`, `full_name`, `email`, `phone`, `monthly_rent`, `move_in`, `status` | Tenant to room assignment |
| `bills` | 10 | `id`, `billing_period`, `due_date` | Billing obligations |
| `payments` | 6 | `id`, `bill_id`, `amount`, `payment_date` | Cash receipts |
| `payment_corrections` | 7 | `id`, `payment_id`, `reason`, `created_at` | Payment audit trail |
| `inquiries` | 10 | `id`, `email`, `status` | Prospective tenants |
| `tenant_requests` | 11 | `id`, `tenant_id`, `kind`, `subject`, `priority`, `status` | Tenant requests |
| `tenant_occupancy_events` | 7 | `id`, `tenant_id`, `previous_lease_id`, `performed_by`, `created_at` | Occupancy transitions |
| `notifications` | 8 | `id`, `tenant_id`, `title`, `created_at` | Event audit trail |

#### Relationships

17 edges. Null values are permitted and are not treated as orphans.

| Child | Column | Parent |
| --- | --- | --- |
| `rooms` | `building_id` | `buildings.id` |
| `leases` | `tenant_id` | `profiles.id` |
| `leases` | `room_id` | `rooms.id` |
| `tenant_assignments` | `room_id` | `rooms.id` |
| `tenant_assignments` | `id` | `auth.users.id` — unverifiable |
| `bills` | `lease_id` | `leases.id` |
| `payments` | `bill_id` | `bills.id` |
| `payment_corrections` | `payment_id` | `payments.id` |
| `payment_corrections` | `corrected_by` | `auth.users.id` — unverifiable |
| `inquiries` | `room_id` | `rooms.id` |
| `tenant_requests` | `tenant_id` | `profiles.id` |
| `tenant_requests` | `room_id` | `rooms.id` |
| `tenant_occupancy_events` | `tenant_id` | `profiles.id` |
| `tenant_occupancy_events` | `previous_lease_id` | `leases.id` |
| `tenant_occupancy_events` | `new_lease_id` | `leases.id` |
| `tenant_occupancy_events` | `performed_by` | `profiles.id` |
| `notifications` | `tenant_id` | `profiles.id` |

`auth.users` is outside extraction scope, so those two edges are reported as `target_table_unavailable` and excluded from the acceptance decision.

#### Legitimate nulls

These nulls are correct and must not be flagged:

| Column | When null |
| --- | --- |
| `tenant_assignments.room_id` | assignment status is `ended` |
| `tenant_assignments.lease_end` | open ended tenancy |
| `tenant_requests.room_id` | request kind is `other` |
| `tenant_occupancy_events.new_lease_id` | move out |
| `inquiries.phone` | not supplied |

`inquiries.room_id` and `leases.lease_end_date` are permitted but not expected to be null.

#### Optional and extra columns

`inquiries.room_id` is optional: it is present in the deployed database, absent from every file in `migrations/`, and selected only when the live schema exposes it.

Columns the original contract omitted are selected by default. Disable them with `--no-extra-columns`.

| Table | Columns |
| --- | --- |
| `rooms` | `description` |
| `tenant_assignments` | `emergency_name`, `emergency_phone`, `relationship` |
| `inquiries` | `message` |
| `tenant_requests` | `message`, `admin_note` |
| `tenant_occupancy_events` | `details` |

`tenant_occupancy_events.details` is `NOT NULL` and holds the actual move date, room, rent and end date. Without it that table has no payload.

#### Mapping and domain rules

All 98 columns map `Direct`. Source names, values and types are preserved. No renaming, no type coercion, no trimming, no unit conversion. Column types are recorded in every run manifest because CSV stores values as text.

| Rule | Scope |
| --- | --- |
| Observed enum domains | `profiles.role`, `rooms.status`, `leases.status`, `bills.status` |
| Monetary, must not be negative | `rooms.monthly_rate`, `tenant_assignments.monthly_rent`, `bills.rent_amount`/`electricity_amount`/`water_amount`, `payments.amount` |
| Numeric bounds | `rooms.size_sqm` between 8 and 14 |
| Date ordering | `tenant_assignments.lease_end >= move_in`, `leases.lease_end_date >= move_in_date` |
| Invariant | `tenant_assignments`: `active` requires a `room_id`, `ended` requires none |

---

## 3. Extraction Validation and Data Quality Checks

Validation reads the staged CSV datasets rather than issuing a second live read, so it judges exactly the artefact handed to the transformation stage. All checks are extraction level.

| # | Check name | Target | Purpose | Pass / Warning / Fail |
| --- | --- | --- | --- | --- |
| 1 | `source_connection_accessibility` | PostgREST endpoint | Confirm the source is reachable and serving rows | **PASS** at least one table extracted; **FAIL** none extracted |
| 2 | `required_tables_and_columns_exist` | Contract versus live schema | Detect schema drift before promotion | **PASS** all present; **FAIL** any table non conforming |
| 3 | `extraction_errors_or_interruptions` | Run metadata | Identify partial or failed extractions | **PASS** no failures; **FAIL** any table failed |
| 4 | `required_identifiers_present` | Primary keys and required relationship fields | Ensure records can be traced and related | **PASS** no nulls; **WARNING** nulls counted and referenced |
| 5 | `primary_key_uniqueness` | `id` per table | Detect duplicates introduced by extraction | **PASS** unique; **FAIL** duplicates found |
| 6 | `record_count_completeness` | Staged rows versus `Prefer: count=exact` | Detect incomplete extraction | **PASS** match; **WARNING** unexplained difference |
| 7 | `referential_completeness` | 17 relationship edges | Detect orphaned references | **PASS** resolve; **WARNING** unresolved recorded |
| 8 | `numeric_range_sanity` | Monetary and size columns | Flag negative or unparseable amounts | **PASS** clean; **WARNING** violations flagged |
| 9 | `date_sanity` | Date columns and ordering rules | Flag invalid or inconsistent dates | **PASS** clean; **WARNING** suspicious dates flagged |
| 10 | `assignment_room_invariant` | `tenant_assignments.status` and `room_id` | Confirm active assignments hold a room and ended ones do not | **PASS** consistent; **WARNING** violations flagged |
| 11 | `room_size_bounds` | `rooms.size_sqm` | Confirm sizes remain inside the database constraint of 8 to 14 | **PASS** in range; **WARNING** outside |
| 12 | `observed_enum_domains` | Enum typed columns | Report the value domains actually observed | **NOT_APPLICABLE** report only |

### 3.1 Criticality

Criticality follows the section 7.1 enumeration rather than an independent judgement. Checks 1, 2, 3, 5 and 6 are critical: any failure produces `REJECTED`. Checks 4, 7, 8, 9, 10 and 11 are non critical and produce `MANUAL REVIEW`. Check 12 never affects the decision.

### 3.2 Validation Limits

- **`auth.users` is not reachable.** Two edges, `tenant_assignments.id` and `payment_corrections.corrected_by`, cannot be resolved. They are reported as `target_table_unavailable` and excluded from the decision, because their absence is a fixed scope decision rather than a data quality signal.
- **Enum members are not exposed.** PostgREST publishes type names but not labels, so values are reported as observed rather than asserted.
- **No cross request snapshot.** Pages are ordered by the immutable `id`, so nothing is skipped or duplicated, but the run is not a single consistent database instant.

---

## 4. Extraction Metadata and Log Specification

Three records are written per run, keyed by run id `spartment-extract-<YYYY-MM-DD-HHMM>`. A counter suffix is appended if a run id is already in use within the same minute, so no record is ever overwritten.

### 4.1 Per Table Record — `rawdata/_metadata/<run_id>.json`

| Field | Purpose | Example |
| --- | --- | --- |
| `pipeline_run_id` | Unique identifier for the run | `spartment-extract-2026-10-18-0100` |
| `table_name` | Table extracted | `rooms` |
| `extraction_started_at` | Start timestamp | `2026-10-18T01:00:00+08:00` |
| `extraction_ended_at` | End timestamp | `2026-10-18T01:02:15+08:00` |
| `extraction_status` | Table status | `SUCCESS`, `WARNING`, `FAILED` |
| `records_read` | Records written from the source | `350` |
| `records_extracted` | Records written to staging | `350` |
| `records_read_total` | Exact source count reported by the API | `350` |
| `records_rejected` | Records flagged or quarantined | `0` |
| `extraction_window_start` | Window start, NULL for full extraction | `null` |
| `extraction_window_end` | Window end or run end | `2026-10-18T01:02:15+08:00` |
| `validation_result` | Validation outcome | `PASS`, `FAIL` |
| `error_message` | Failure detail | `null` |
| `error_code` | Machine readable failure code | `null`, `SCHEMA_DRIFT` |
| `extraction_duration_ms` | Duration in milliseconds | `135000` |
| `output_path` | Staged dataset location | `rooms\2026-10-18\rooms.csv` |

The run record additionally carries the column type manifest, the schema drift list, and the acceptance decision.

### 4.2 Validation Record — `rawdata/_validation/<run_id>.json`

Overall result; per check the attributes required by section 3 — name, target, purpose, result, detail — criticality, evidence capped at 50 rows per check, and rejected row references.

### 4.3 Handover Record — `rawdata/_handover/<run_id>.json`

The section 7.3 package: acceptance decision, per table validation summary, staged dataset paths, extraction metadata, validation checks, rejected and quarantined references, the standardized column mapping, the column type manifest, and the processing period.

### 4.4 Usage

- **Monitoring** — Airflow task status and the acceptance decision.
- **Troubleshooting** — run id, counts and error codes.
- **Auditing** — what was extracted, when, and which checks ran.
- **Recovery** — which tables must be re-extracted after a failed run.

Database passwords, access tokens and service role keys are never written to any record, log or CSV.

---

## 5. Log Retention and Access

| Item | Policy | Justification |
| --- | --- | --- |
| **Operational retention** | 90 days for staged datasets and per table run metadata | Covers a full billing cycle plus investigation time, and satisfies the most common local operational audit expectation without storing personal data longer than the billing and tenancy records it describes. |
| **Audit retention** | 365 days for validation and handover records | Acceptance decisions, the checks that justified them and the schema the run saw must be defensible for a full year, because financial and tenancy disputes surface after year end and a rejected or held run must still be explainable. |
| **Enforcement** | `spartment-retain`, dry run by default, `--apply` to delete | Making deletion an explicit operator action prevents silent data loss; a dry run shows exactly what would go before anything is removed. |
| **Storage location** | Local `rawdata/` on the scheduler volume, and Airflow task logs | Staged datasets are intermediate and reproducible, so they do not warrant offsite replication. |
| **Access permissions** | Read restricted to data pipeline maintainers and database owners. Write limited to the pipeline service account | The datasets contain tenant names, emails, phone numbers, home addresses and payment records. Access is restricted on a least privilege basis because the data is personal. |
| **Archiving** | Archive records required for audit or recovery before removal, preserving run id and date | Retention loss must never remove the only evidence that a decision was made. |
| **Deletion rules** | Nothing is deleted inside its retention window. Approved deletion is recorded | Retention is a minimum, not a target. Deletion inside the window would destroy audit evidence. |

Retention periods require organisational approval before they are enforced with `--apply`. The dry run is safe at any time.

---

## 6. Extraction Data Contract and Naming Convention

### 6.1 Required Tables

`profiles`, `buildings`, `rooms`, `leases`, `tenant_assignments`, `bills`, `payments`, `payment_corrections`, `inquiries`, `tenant_requests`, `tenant_occupancy_events`, `notifications`.

Required identifiers are each table's primary key plus the required relationship fields declared in `contract.py`. Nullable relationship fields are deliberately excluded, because a null is legal for ended assignments, `other` requests and move out events.

### 6.2 Naming Convention

- Source table and column names are used as defined in the database.
- `snake_case` throughout.
- The source identifier name `id` is preserved for primary keys.
- Relationship fields such as `building_id`, `room_id`, `tenant_id`, `lease_id` and `performed_by` are preserved.
- No field is renamed or abbreviated without a documented mapping.
- PostgreSQL types are preserved in the run manifest even though CSV stores them as text.

### 6.3 Unexpected Schema Changes

1. The schema guard detects a missing or changed table or column before any dataset is written.
2. The pipeline logs the discrepancy with the specific missing columns and quarantines that table.
3. Other tables continue to extract, so one drift does not block the whole run.
4. `--strict-contract` converts any non conforming table into a `REJECTED` run instead.
5. The contract is updated only after the change is confirmed against the deployed database, the affected table is re-extracted, and both runs remain traceable by run id.

### 6.4 Source to Standardized Column Mapping

The mapping is emitted in full in every handover record under `data_contract.standardized_column_mapping`. Every extracted column is carried through unchanged, so the mapping type is `Direct` for all 98 columns across the 12 tables and no renaming occurs.

| Source column | Standardized column | Source type | Target type | Mapping type |
| --- | --- | --- | --- | --- |
| All columns of all 12 tables | Identical name | As per section 2.1 | Unchanged | `Direct` |

Value standardization, meaning date parsing, currency normalisation, category mapping and unit conversion, is explicitly deferred to the transformation stage.

---

## 7. Extraction Acceptance and Handover Rules

### 7.1 Decision Rules

| Overall condition | Acceptance decision | Pipeline action | Responsible |
| --- | --- | --- | --- |
| All critical checks pass and no warnings are raised | **APPROVED** | Pass raw data, metadata and validation results to transformation | Automated, no human sign off required |
| Any non critical warning remains | **MANUAL REVIEW** | Hold promotion until the warning is reviewed | Data Pipeline Maintainer, within one business day |
| Any critical check fails | **REJECTED** | Stop promotion. Retry transient failures per 1.4, or quarantine the affected extract | Data Pipeline Maintainer and Database Owner |

### 7.2 Critical Failures

A run is `REJECTED` when connectivity was lost, a required table or column is missing, duplicate primary keys exist, extraction is incomplete, or any table failed to extract. Duplicate keys and interrupted extraction are treated as critical because both can silently corrupt downstream aggregates.

Transient connection and query failures are retried up to three times with exponential backoff before the run is rejected.

### 7.3 Handover Contents

- Raw extracted datasets under `rawdata/<table>/<run_date>/`.
- Extraction metadata including run id, timestamps, counts and durations.
- Validation results per check, plus rejected row and quarantine references.
- The standardized column mapping and the column type manifest.
- The acceptance decision, its rationale, its pipeline action and its responsible party.
- The processing period.

### 7.4 Exit Codes

| Code | Meaning |
| --- | --- |
| 0 | `APPROVED` |
| 1 | Extraction failure |
| 2 | Configuration or argument error |
| 3 | Schema unreachable |
| 4 | `REJECTED` |
| 5 | `MANUAL REVIEW` |

### 7.5 Verified Behaviour

| Scenario | Result | Exit |
| --- | --- | --- |
| Clean extraction | `APPROVED` | 0 |
| Duplicate primary key in a staged table | `REJECTED`, cause `primary_key_uniqueness` | 4 |
| Negative amount and `lease_end` earlier than `move_in` | `MANUAL REVIEW` | 5 |
| Null required identifier | `WARNING`, counted in `records_rejected` with row references | 5 |

---

## 8. Common Extraction Problems and Mitigation

| # | Problem | Possible cause | Impact | Mitigation |
| --- | --- | --- | --- | --- |
| 1 | PostgREST is unreachable | Network outage, invalid credential, platform maintenance | No data extracted | Retry 3 times with backoff, log the final error, stop handover if retries fail |
| 2 | Required table or column is missing | Schema change, or documentation lagging the deployment | Table quarantined | Schema guard diffs before writing, quarantines only the affected table, `--strict-contract` rejects the run |
| 3 | Extracted count differs from the source | Interrupted pagination or concurrent writes | Dataset silently incomplete | Compare staged rows against `Prefer: count=exact` |
| 4 | Duplicate primary keys appear | Pagination defect or repeated data | Duplicates distort aggregates | Uniqueness check fails the run as critical; re-extract |
| 5 | Required identifier is null | Incomplete source record | Record cannot be identified | Counted as rejected with row references, never invented |
| 6 | Relationship points to a missing record | Orphaned reference or target out of scope | Joins incomplete | Recorded and held; `auth.users` edges reported as unverifiable |
| 7 | Large table times out | Too many records at once | Partial data | Page by `Range` ordered by `id`, stream rows straight to disk |
| 8 | Credentials are exposed | Key hard coded or printed in logs | Unauthorized access | Read from environment only, never logged or written to any artefact |
| 9 | Expected table returns no rows | Empty table, permission issue or wrong query | Missing data mistaken for valid | Count reconciliation against the source total |
| 10 | Enum domain drifts unexpectedly | Application release changed a status label | Silent category change | Observed domains reported per run |
| 11 | Incremental extraction misses updates | No reliable change tracking field | Changes never arrive | Full extraction until verified, per 1.3 |
| 12 | No consistent snapshot across pages | Concurrent writes during a multi page read | Counts may differ from a single instant | Paged by immutable `id`; limitation recorded in 3.2 |
| 13 | CSV flattens PostgreSQL types | CSV has no type system | Type fidelity lost | Column type manifest in every run record |

---

## 9. Implementation Map

| Component | File |
| --- | --- |
| Contract, relationships, domain rules, mapping | `src/spartment/stage1_extract/contract.py` |
| Configuration, credentials, run id | `src/spartment/common/config.py` |
| GET only PostgREST client, pagination, retry | `src/spartment/stage1_extract/source.py` |
| Schema guard and drift handling | `src/spartment/stage1_extract/guard.py` |
| Extraction and CSV staging | `src/spartment/stage1_extract/extract.py` |
| Twelve validation checks | `src/spartment/stage1_extract/validate.py` |
| Acceptance gate and handover package | `src/spartment/stage1_extract/handover.py` |
| Shared records | `src/spartment/common/models.py` |
| JSON writers | `src/spartment/common/io.py` |
| Retention enforcement | `src/spartment/common/retention.py` |
| Command line entry point | `src/spartment/stage1_extract/cli.py` |
| Scheduled DAG | `dags/spartment_daily_extraction.py` |
| Non secret settings | `config/extraction.toml` |

### Commands

```
spartment-extract                        full run: extract, validate, gate
spartment-extract --tables rooms,bills   subset of tables
spartment-extract --strict-contract      reject on any schema drift
spartment-extract --no-extra-columns     contract columns only
spartment-extract --skip-validation      debugging only
spartment-retain                         retention dry run
spartment-retain --apply                 perform deletions
```

If the console scripts are not on `PATH`, use the module form:

```
python -m spartment.stage1_extract.cli
python -m spartment.common.retention
```

### Credentials

`SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are read from a `.env` file at the repository root, which is excluded from version control. In Airflow the same values are supplied by the `spartment_postgres` connection. A database superuser password is never required and must never be placed in this repository. See `.env.example`.

The anon key is not used. If the service role key is ever committed or pasted into a channel, rotate it in Supabase under Project Settings, then API.

---

## 10. Operations Runbook

Day to day operation of the extraction stage.

### 10.1 Daily run

The Airflow DAG `spartment_daily_extraction` runs at 01:00 Asia/Manila with up to 3 retries. Nothing is required manually on a normal day.

To run it by hand:

```bash
spartment-extract
```

### 10.2 Reading the result

```
acceptance    APPROVED          promote to transformation
acceptance    MANUAL REVIEW     promotion held, exit 5
acceptance    REJECTED          promotion blocked, exit 4
```

The handover record is the entry point for every investigation:

```
rawdata/_handover/<run_id>.json
```

It contains the acceptance decision and its rationale, the responsible party, per table validation summary, staged dataset paths, extraction metadata, all validation checks with evidence, rejected and quarantined references, the standardized column mapping, the column type manifest, and the processing period.

The exit codes are listed in section 7.4.

### 10.3 Rejection

A `REJECTED` run has a critical failure. Do not promote.

| Cause | Where to look | Recovery |
| --- | --- | --- |
| Connectivity lost | `source_connection_accessibility` evidence | Confirm the endpoint and credentials, re-run |
| Schema drift | `required_tables_and_columns_exist` evidence | Compare the live schema with `contract.py`, confirm the change, update the contract, re-extract |
| Duplicate primary keys | `primary_key_uniqueness` evidence | Indicates a pagination defect or concurrent source writes. Re-extract; if it repeats, stop and investigate |
| Incomplete extraction | `record_count_completeness` or `extraction_errors_or_interruptions` | Re-run the affected tables |
| Null required identifier | `required_identifiers_present` evidence | Flagged rows are listed with their CSV line number. Do not invent identifiers |

Escalate to the Database Owner when the defect is on the source side.

### 10.4 Manual review

Non critical warnings hold promotion without failing the run. Common causes are negative monetary values, inverted dates, unresolved relationship references, room sizes outside the permitted range, and assignment status inconsistent with the room reference.

Review the evidence in the handover record, then either correct the source data and re-run, or record a decision to promote with the warning accepted.

### 10.5 Recovery and re-extraction

```bash
spartment-extract --tables rooms
```

Every run is independent and full. To rebuild, re-run the affected tables. Raw datasets are reproducible from the source, so no backup of `rawdata/` is required.

Both the failed run and the recovery run remain traceable by run id. Run ids are `spartment-extract-<date>-<HHMM>` with a counter suffix when a minute is reused, so nothing is overwritten.

Retention and its commands are described in section 5.

### 10.6 Airflow

The DAG needs the package installed in the Airflow environment:

```bash
pip install -e .
```

Airflow places only its own `dags_folder` on `sys.path`, so a repository relative import would not resolve. The DAG reads credentials from the Airflow connection `spartment_postgres`.

Point the scheduler at the repository:

```bash
airflow dags list          # with --dags-folder configured
```

`max_active_runs=1`, so overlapping runs cannot occur. The gate task fails the run on `REJECTED` and logs a warning on `MANUAL REVIEW`.

### 10.7 Known limitations

- `auth.users` is not reachable, so `tenant_assignments.id` and `payment_corrections.corrected_by` cannot be referentially validated.
- Enum members are not exposed by PostgREST, so domains are reported as observed rather than asserted.
- Pages are ordered by the immutable `id`, so nothing is skipped or duplicated, but a run is not a single consistent database instant.
- `rooms.updated_at` is populated in practice but has no trigger in `migrations/`, so it is not trusted as a watermark.
- The DAG has not been executed in this environment; it is syntax checked only.