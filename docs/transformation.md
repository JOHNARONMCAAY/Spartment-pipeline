# Spartment: Operational and Analytical Batch Data Pipeline

## Stage 2: Data Transformation Documentation

### Objective

The objective of the Spartment Data Transformation Stage is to take the validated dataset handed over by the Extraction Stage (`docs/extraction.md`) and transform it into a cleaned, standardized, documented, and analytically usable dataset for room availability and occupancy monitoring, tenant assignment tracking, billing and collection monitoring, prospective tenant inquiry analysis, and tenant request analysis.

> This document specifies Stage 2. The Stage 2 implementation remains reserved in `src/spartment/stage2_transform/`; no Stage 2 code is added by this document alone. The specification is written so that it can be implemented without changing the Stage 1 contract.

Transformation does not connect to Supabase, does not call PostgREST, and does not re-extract a single record. Its only input is the Stage 1 handover.

---

## 1. Transformation Input and Processing Specification

### 1.1 Input Source

| Item | Description |
| --- | --- |
| **Input origin** | Stage 1 handover only. No source database or API connection is opened by this stage. |
| **Staged datasets** | `rawdata/<table>/<run_date>/<table>.csv` |
| **Parent run record** | `rawdata/_handover/<pipeline_run_id>.json` |
| **Parent metadata** | `rawdata/_metadata/<pipeline_run_id>.json` |
| **Parent validation** | `rawdata/_validation/<pipeline_run_id>.json` |
| **File format** | CSV, UTF-8, header row, all values represented as text |
| **Column types** | Read from `data_contract.column_manifest` in the Stage 1 handover. Stage 1 writes **no per-table `.schema.json` sidecar**. |
| **Input tables** | `profiles`, `buildings`, `rooms`, `leases`, `tenant_assignments`, `bills`, `payments`, `payment_corrections`, `inquiries`, `tenant_requests`, `tenant_occupancy_events`, `notifications` |
| **Parent run id** | Stage 1 `pipeline_run_id`, for example `spartment-extract-2026-10-18-0100` |
| **Batch date** | The Stage 1 `run_date` directory, for example `2026-10-18` |

Concrete handover artifacts consumed by this stage:

| Artifact | Path pattern | Use in Transformation |
| --- | --- | --- |
| Handover record | `rawdata/_handover/<parent_run_id>.json` | Parent acceptance decision, validation summary, raw dataset paths, column manifest, processing period |
| Metadata record | `rawdata/_metadata/<parent_run_id>.json` | Per-table extraction status, record counts, output paths, manifest |
| Validation record | `rawdata/_validation/<parent_run_id>.json` | Parent validation evidence referenced by the handover summary |
| Staged profiles | `rawdata/profiles/<run_date>/profiles.csv` | Build `dim_tenant` |
| Staged buildings | `rawdata/buildings/<run_date>/buildings.csv` | Build `dim_building` |
| Staged rooms | `rawdata/rooms/<run_date>/rooms.csv` | Build `dim_room` |
| Staged leases | `rawdata/leases/<run_date>/leases.csv` | Build `dim_lease`, `fact_billing` attribution |
| Staged tenant assignments | `rawdata/tenant_assignments/<run_date>/tenant_assignments.csv` | Build `fact_assignment`, enrich `dim_tenant` |
| Staged bills | `rawdata/bills/<run_date>/bills.csv` | Build `fact_billing` |
| Staged payments | `rawdata/payments/<run_date>/payments.csv` | Build `fact_payment`, enrich `fact_billing` |
| Staged payment corrections | `rawdata/payment_corrections/<run_date>/payment_corrections.csv` | Build `fact_payment_correction` |
| Staged inquiries | `rawdata/inquiries/<run_date>/inquiries.csv` | Build `fact_inquiry` |
| Staged tenant requests | `rawdata/tenant_requests/<run_date>/tenant_requests.csv` | Build `fact_tenant_request` |
| Staged occupancy events | `rawdata/tenant_occupancy_events/<run_date>/tenant_occupancy_events.csv` | Build `fact_occupancy_event` |
| Staged notifications | `rawdata/notifications/<run_date>/notifications.csv` | Build `fact_notification` |

**Example:** Stage 1 extracts all 12 tables for run `spartment-extract-2026-10-18-0100`. Stage 2 receives the 12 staged CSVs, the handover record, the metadata record, and the validation record. It profiles the inputs, applies the cleaning rules in Section 3, the standardization rules in Section 4, and the business rules in Section 5, then emits the model defined in Section 6.

> **Requirement:** the input must match the Stage 1 handover. Stage 2 must not re-extract, must not repair source data by querying the source, and must not duplicate the Stage 1 source description.

### 1.2 Batch Processing Method

| Item | Description |
| --- | --- |
| **Transformation tool** | Python 3.11+, standard library only (`csv`, `decimal`, `hashlib`, `json`, `datetime`, `pathlib`) |
| **Orchestration tool** | Apache Airflow: DAG `spartment_daily_transformation` |
| **Frequency / schedule** | Daily at 02:00 Asia/Manila, one hour after the Stage 1 01:00 extraction |
| **Batch size** | All records staged by one Stage 1 run. One extraction run equals one transformation batch. |
| **Retry** | Up to 3 retries with 5-minute exponential backoff on file-read or parse failure. Contract and data defects are not retried automatically. |
| **Rerun model** | Re-read the same Stage 1 batch and overwrite the output for that run date |
| **Idempotency** | Yes. Running the same batch twice produces the same transformed content and never duplicates rows. See Section 1.4. |
| **Database access** | None. Stage 2 is a pure file-to-file transformation. |
| **Load access** | None. Persistence belongs to the Load Stage. |

**Processing order** (dimensions and parents before facts and children):

```
dim_date
  -> dim_building
  -> dim_room
  -> dim_tenant
  -> dim_lease
  -> fact_assignment
  -> fact_billing (base)
  -> fact_payment
  -> fact_payment_correction
  -> fact_inquiry
  -> fact_tenant_request
  -> fact_occupancy_event
  -> fact_notification
  -> billing enrichment (amount_paid, balance, status, aging)
  -> building and tenant rollups (occupancy, outstanding bills)
```

`dim_room` depends on `dim_building`; `dim_lease` depends on `dim_tenant` and `dim_room`; `fact_billing` depends on `dim_lease`; `fact_payment` depends on `fact_billing`; `fact_payment_correction` depends on `fact_payment`. Billing enrichment runs after payments because `amount_paid` and `balance` are derived from the payment ledger. Building and tenant rollups run last because they require both dimensions and facts.

A failure at step *n* leaves steps 1..*n-1* already written. Recovery is a rerun of the whole batch, which is safe because reruns are idempotent.

### 1.3 Transformation Scope

| Source table | Transformed to | Operation |
| --- | --- | --- |
| `profiles` | `dim_tenant` | Identity, role, contact; merged with assignment attributes |
| `buildings` | `dim_building` | Building identity plus room and occupancy rollups |
| `rooms` | `dim_room` | Availability, capacity, rate, size, building key |
| `leases` | `dim_lease` | Tenancy spine, lease duration, active flag |
| `tenant_assignments` | `fact_assignment`, `dim_tenant` | Assignment grain, rent, tenure, current contact and emergency details |
| `bills` | `fact_billing` | Bill components, total, payment balance, status, aging |
| `payments` | `fact_payment` | Cash receipt grain, allocation order, correction rollup |
| `payment_corrections` | `fact_payment_correction` | Audit record per correction |
| `inquiries` | `fact_inquiry` | Prospective tenant inquiry grain, age and open flag |
| `tenant_requests` | `fact_tenant_request` | Request grain, priority, status, resolution time |
| `tenant_occupancy_events` | `fact_occupancy_event` | Move-in, move-out and transfer event grain |
| `notifications` | `fact_notification` | Notification audit trail linked to tenant and source record |

**Fields cleaned:** all text fields, emails, phone numbers, statuses, bill types, request kinds, priorities, roles, and all monetary and size numerics.

**Fields standardized:** all `DATE` and `TIMESTAMPTZ` values, all money and area values, all categorical code fields, booleans, and `jsonb` audit payloads.

**Fields derived:** surrogate keys, date attributes, tenancy durations, room occupancy flags, building occupancy rollups, bill totals, payment balances, bill status, aging buckets, payment allocation rank, correction counts, tenant outstanding-bill counts, inquiry age, request resolution days, occupancy event types, and lineage.

**Fields aggregated:** building-level room counts, occupancy rate and vacancy gap; tenant-level outstanding bill count and flagged-tenant flag. All other aggregates remain a Load/reporting concern.

### 1.4 Idempotency and Failed-Batch Recovery

Idempotency is achieved by three properties:

1. **Deterministic surrogate keys.** Every key is `MD5('<entity>:<source_id>')` truncated to 16 lowercase hexadecimal characters. Source UUIDs are stable, so the same source row always receives the same key on every rerun.
2. **Atomic output write.** Each output file is written to `<name>.tmp` and moved into place with `os.replace()` only after the table is complete. A crash mid-write leaves the previous complete file intact.
3. **Run-date-scoped paths.** Output lives under `transformed/<table>/<run_date>/`, so a rerun overwrites exactly one directory instead of appending.

**Failed-batch rerun:** fix the cause, then rerun the same `run_date` with the same `parent_run_id`. Because of the properties above, the rerun reproduces the prior output content. If the cause is a Stage 1 defect, the extraction run must be re-triggered first, because Stage 2 must not repair source data.

### 1.5 Dependencies and Assumptions

**Dependencies:**

| Dependency | Type | Behavior if unavailable |
| --- | --- | --- |
| Stage 1 handover and metadata records | Hard | Batch fails immediately; no output is written |
| All 12 staged CSV files for the selected run date | Hard | Batch fails; missing table is a critical schema/input failure |
| Stage 1 column manifest | Hard | Batch fails if a required column cannot be verified |
| Parent acceptance decision | Hard | `REJECTED` is refused; `MANUAL REVIEW` requires explicit override |
| Prior stage output within the same batch | Hard | Batch fails at that step; upstream steps are safe to overwrite on rerun |

**Assumptions:**

- Stage 1 performs full extraction. One Stage 1 run is a complete snapshot of the 12 tables.
- Stage 1 preserves source column names and values; only CSV-level adaptations occur (`NULL` to empty field, boolean to `true`/`false`, `jsonb` to compact JSON text).
- `auth.users` is not reachable. `tenant_assignments.id` and `payment_corrections.corrected_by` are not referentially validated against it.
- The tenant identity namespace is shared: `profiles.id` identifies a person, and `tenant_assignments.id` is the tenant reference used by the assignment and lease workflows. When an assignment has no matching profile, the assignment supplies the tenant identity and is flagged `identity_source = ASSIGNMENT`.
- `bills` has no stored total, paid amount, or balance. Those are derived from the three bill components and the payment ledger.
- `bills.billing_period` is the period the bill covers; `bills.due_date` is the payment deadline. `billing_period` is treated as a date even though it is displayed monthly.
- `rooms.size_sqm` is constrained to 8 to 14 square meters. A value outside that range is flagged, not silently corrected.
- `inquiries.room_id` is optional and may be absent from some deployments. It is nullable in the model.
- `tenant_requests.room_id` is null when the request kind is `other`.
- `tenant_occupancy_events.new_lease_id` is null on move-out.
- `notifications.source_id` is polymorphic and has no enforced foreign key. It is carried as a UUID but is not used for referential validation.
- All business dates are interpreted in **Asia/Manila**. Timestamps are rendered as ISO 8601 with an explicit `+08:00` offset.
- Late-arriving records are possible. A configurable lookback or reprocess window is a Load/operations concern; this stage reprocesses the selected parent batch idempotently.

---

## 2. Data Profiling and Issue Identification

The issues below are **representative estimates** derived from the Stage 1 contract and the observed database constraints. Percentages are **estimates to be confirmed against the first real staged batch** and must be replaced with actual profiling counts at that time.

| # | Table | Column | Issue Type | Example | Affected (est.) | Planned Treatment |
| --- | --- | --- | --- | --- | --- | --- |
| P-01 | all | text columns | Leading/trailing or repeated whitespace | `" Dela Cruz "` | ~4% | Trim and collapse — C-01 |
| P-02 | `profiles` | `phone` | Missing value | `NULL` | ~20% | Keep NULL — C-03 |
| P-03 | `tenant_assignments`, `inquiries` | `email` | Mixed case | `Juan.DelaCruz@Mail.COM` | ~3% | Lowercase — C-02 |
| P-04 | `tenant_assignments` | `phone` | Missing or non-numeric text | `NULL`, `"n/a"` | ~15% | Keep as text, NULL preserved — C-03 |
| P-05 | `rooms` | `size_sqm` | Outside the 8–14 constraint | `6.50`, `18.00` | ~0.5% | Flag `size_out_of_bounds` — B-20 |
| P-06 | `rooms` | `status` | New or mixed-case variant | `"Available"`, `"maintenance"` | ~2% | Map to code set — C-11, S-03 |
| P-07 | `profiles` | `role` | New or mixed-case variant | `"Tenant"`, `"ADMIN"` | ~1% | Map to code set — C-11, S-03 |
| P-08 | `leases` | `status` | New or mixed-case variant | `"Active"` | ~1% | Map to code set — C-11, S-03 |
| P-09 | `tenant_assignments` | `status` | New or mixed-case variant | `"Ended"` | ~1% | Map to code set — C-11, S-03 |
| P-10 | `bills` | `status` | Free-text status variants | `"pending"`, `"PARTIALLY PAID"` | ~5% | Map to code set — C-11, S-03 |
| P-11 | `bills` | `bill_type` | Free-text type variants | `"Rent"`, `"utility"` | ~3% | Map to code set — C-11, S-04 |
| P-12 | `bills` | `rent_amount`, `electricity_amount`, `water_amount` | Negative or unparseable amount | `-50.00`, `"n/a"` | ~0.1% | Reject — C-05, S-05 |
| P-13 | `bills` | `due_date` | Due date before billing period | `period=2026-03-01, due=2026-02-20` | ~1% | Repair to period + 15 days — C-07 |
| P-14 | `leases`, `tenant_assignments` | `lease_end_date`, `lease_end` | End date earlier than move-in | `move_in=2026-03-01, end=2026-02-20` | ~0.5% | Set end to NULL and flag — C-08 |
| P-15 | `payments` | `amount` | Negative or unparseable amount | `-100.00` | ~0.1% | Reject — C-05, S-05 |
| P-16 | `payments` | `(bill_id, payment_date, amount)` | Duplicate from client or staff retry | two identical receipts | ~0.5% | Keep earliest `created_at` — C-06 |
| P-17 | `payments` | `payment_date` | Future-dated payment | `2026-12-31` | ~0.1% | Reject — X-04 |
| P-18 | all relationships | FK columns | Orphan child with no parent | `payments.bill_id` not in `bills` | ~0.3% | Reject child — C-10 |
| P-19 | `tenant_assignments`, `tenant_requests`, `inquiries`, `tenant_occupancy_events` | nullable references | Legitimate nulls treated as errors by a naive rule | `room_id=NULL` for ended assignment | varies | Preserve NULL — C-03 |
| P-20 | `payment_corrections`, `tenant_occupancy_events` | `previous_values`, `corrected_values`, `details` | Invalid or non-canonical JSON | truncated payload | ~0.5% | Canonicalize when valid, flag when not — C-12 |
| P-21 | `inquiries`, `tenant_requests` | `status`, `priority`, `kind` | New code variants after an application release | `"In progress"`, `"Urgent"` | ~2% | Map known values, flag unmapped — C-11 |
| P-22 | `rooms` | `building_id` | Orphan room with no building | building deleted or never staged | ~0.2% | Reject child — C-10 |
| P-23 | `bills` | `lease_id` | Orphan bill with no lease | lease outside the staged set | ~0.2% | Reject child — C-10 |

**Profiling requirements:**

- Profiling runs on the staged CSV **before** any cleaning rule is applied.
- Counts must be reproducible for the same batch.
- Counts are recorded in the run log as `profile_<table>_<issue>_count`.
- Every cleaning rule in Section 3 must trace to at least one profiled issue. A rule with no issue behind it must be removed or justified.
- The percentages above are estimates. They must be replaced by measured counts on the first production run before this document is treated as a measured baseline.

---

## 3. Data Cleaning Rules

Rules are applied in ascending Rule ID order. Every rule increments a `rules_applied` counter for the records it modifies or rejects; counters appear in the run log (Section 8).

### 3.1 Cleaning Rule Specification

| Rule ID and name | Target | Condition | Action | Justification |
| --- | --- | --- | --- | --- |
| **C-01** Trim text | All `TEXT`/`VARCHAR` columns: names, emails, phones, room numbers, addresses, subjects, notes, reasons, descriptions | Leading/trailing whitespace, repeated internal spaces, non-printable control characters | `strip()`; collapse internal whitespace to one space; remove control characters; an empty result becomes NULL | Prevents false mismatches on joins and code mapping, and prevents `"Cash "`-style values from failing a lookup |
| **C-02** Lowercase identifiers | `tenant_assignments.email`, `inquiries.email` | Uppercase characters present | `lower()` | Email is an equality identifier in inquiry and tenant workflows; mixed case breaks joins and duplicate detection |
| **C-03** Preserve legitimate nulls | `profiles.phone`, `tenant_assignments.room_id` when `ended`, `tenant_assignments.lease_end`, `tenant_requests.room_id` when kind is `other`, `tenant_occupancy_events.new_lease_id` on move-out, `inquiries.phone`, `inquiries.room_id`, `payments.reference_note`, `payment_corrections.corrected_by`, `notifications.source_id`, `rooms.description` | Value is NULL or empty after C-01 | Retain as NULL. Never impute a placeholder. | These nulls are legal in the source model. Inventing values would create false tenants, rooms, dates, or payment references |
| **C-04** Normalize unknown codes | Categorical fields that cannot be mapped after C-11 | Value is missing, unrecognized, or outside the observed domain | Set the standard `UNKNOWN` value where the column is a code, and set `code_unmapped = TRUE`. For nullable references, keep NULL rather than inventing a key. | A new code must be visible as a data-quality signal, not silently absorbed; a missing optional reference must not remove a valid fact |
| **C-05** Reject impossible values | `rooms.monthly_rate`, `tenant_assignments.monthly_rent`, `bills.rent_amount`, `bills.electricity_amount`, `bills.water_amount`, `payments.amount` | Value is negative; a monetary value is unparseable | Reject the row to quarantine with the failing rule; exclude it from output | A negative rent, utility charge, or receipt is a corrupt record, not a cleaning problem. Publishing it would corrupt every downstream total |
| **C-06** Deduplicate | `payments` on `(bill_id, payment_date, amount)`; `tenant_assignments` on `id` defensively | Two or more rows share the deduplication key | Payments: keep the earliest `created_at`, tie-break on lowest `id`. Assignments: keep the latest `created_at`, tie-break on highest `id`. Log discarded rows with `rule_id = C-06`. | Payments are immutable financial events, so the first recorded receipt wins. Assignments are corrected in place, so the latest assignment state wins. Stage 1 already enforces primary-key uniqueness; this is a defence-in-depth check |
| **C-07** Repair due date | `bills.due_date` | `due_date < billing_period` | Set `due_date = billing_period + 15 days`; set `due_date_corrected = TRUE` | A due date before the billing period makes aging and `days_to_due` meaningless |
| **C-08** Repair inverted tenancy dates | `leases.lease_end_date`, `tenant_assignments.lease_end` | End date is earlier than the corresponding move-in date | Set the end date to NULL and set `date_corrected = TRUE` | A NULL end date is the model's representation of an open-ended tenancy; inventing an end date would be worse than recording the gap |
| **C-09** Enforce assignment-room invariant | `tenant_assignments.status` and `tenant_assignments.room_id` | `active` has no room, or `ended` still has a room | Reject the row to quarantine with `rule_id = C-09` | The source constraint requires this invariant. A violation makes occupancy and assignment facts contradictory |
| **C-10** Reject orphans | `rooms.building_id`, `leases.tenant_id`, `leases.room_id`, `tenant_assignments.room_id` (when not null), `bills.lease_id`, `payments.bill_id`, `payment_corrections.payment_id`, `inquiries.room_id` (when not null), `tenant_requests.tenant_id`, `tenant_requests.room_id` (when not null), `tenant_occupancy_events.tenant_id`, `previous_lease_id`, `new_lease_id` (when not null), `performed_by`, `notifications.tenant_id` | Non-null reference has no matching parent in the staged batch | Reject the **child** row to quarantine with `rule_id = C-10`; keep the parent | An orphan fact cannot be attributed to a tenant, room, lease, bill, or payment and is unusable for every downstream measure. Rejecting the parent would cascade away valid history |
| **C-11** Map code domains | `profiles.role`, `rooms.status`, `leases.status`, `tenant_assignments.status`, `bills.status`, `bills.bill_type`, `inquiries.status`, `tenant_requests.kind`, `tenant_requests.priority`, `tenant_requests.status`, `notifications.kind` | Value is not in the mapped code set after C-01 | Map known synonyms (Section 4.3). If still unmapped, set `UNKNOWN` and set `code_unmapped = TRUE`. | Keeps code sets stable for reporting while surfacing application releases that introduce new labels |
| **C-12** Parse JSON safely | `payment_corrections.previous_values`, `payment_corrections.corrected_values`, `tenant_occupancy_events.details` | Value is empty, malformed, or not a JSON object | Empty becomes NULL. Valid JSON is canonicalized (S-07) and `json_valid = TRUE`. Malformed text is preserved verbatim and `json_valid = FALSE`. | The audit payload must remain inspectable even when the application wrote an invalid value; silently dropping it would destroy correction evidence |
| **C-13** Enforce required output attributes | Every output column declared `NOT NULL` in Section 6.1 | Value is still NULL after C-01 to C-12 | Reject the row to quarantine with `rule_id = C-13`. A rejected dimension member that is referenced by an output fact causes V-06 to fail, rejecting the batch rather than publishing orphans. | Stage 1 guarantees only its required identifiers. A NULL in a key, measure, or code that this stage's contract declares mandatory would break joins, totals, or validation, so it must be quarantined rather than published |

### 3.2 Required Considerations

| Consideration | How it is handled |
| --- | --- |
| **Missing / null values** | Optional and legitimately nullable fields are retained as NULL (C-03). A NULL in a column that Section 6.1 declares `NOT NULL` is rejected by C-13, because Stage 1 guarantees only its required identifiers. Categorical fields use the `UNKNOWN` code with `code_unmapped = TRUE` (C-04, C-11). |
| **Duplicate detection and which record is kept** | C-06. Payments keep the earliest `created_at` because a receipt is an immutable financial event. Assignments keep the latest `created_at` because assignment state is corrected in place. Discarded rows are logged with source values and the surviving key. |
| **Whitespace and invalid characters** | C-01 trims, collapses, and strips non-printable characters. A value that becomes empty becomes NULL, not an empty string, so it follows the NULL path consistently. |
| **Invalid or out-of-range values** | C-05 rejects impossible money. C-07 repairs impossible due dates. C-08 repairs inverted tenancy dates. C-09 rejects invariant violations. A room size outside 8–14 is retained with `size_out_of_bounds = TRUE` because it does not invalidate the financial or occupancy fact. |
| **Orphan records** | C-10 rejects only the child. Nullable references are not treated as orphans. Two `auth.users` edges are out of scope and are reported as unverifiable, not as orphans. |
| **JSON audit payloads** | C-12 preserves malformed payloads and flags them. It never invents corrected values and never removes a correction record. |

> **Requirement:** no record is dropped silently. Every rejected record is written to quarantine (Section 9) with its `run_id`, `rule_id`, rejection reason, and source values, and is counted in `records_rejected`.

---

## 4. Standardization and Data Type Rules

### 4.1 Type Conversion Rules by Column Family

| Column family | Source representation | Target type | Conversion |
| --- | --- | --- | --- |
| Surrogate keys | — | `CHAR(16)` | Deterministic `MD5('<entity>:<source_id>')`, lowercase hex, first 16 characters — S-08 |
| Natural keys | `uuid` text | `UUID` | Lowercase, validated as UUID text; invalid identifier rejects the row |
| Dates | `date` text | `DATE` | ISO 8601 `YYYY-MM-DD` — S-01 |
| Timestamps | `timestamptz` text | `TIMESTAMPTZ` | ISO 8601 with explicit `+08:00` — S-01 |
| Money | `numeric` text | `NUMERIC(12,2)` | Parse, reject on failure, round half-up once — S-02, S-05 |
| Area | `numeric` text | `NUMERIC(6,2)` | Parse, round half-up once — S-02 |
| Counts and durations | derived | `INTEGER` | Integer arithmetic; NULL when the source dates do not support the measure |
| Rates | derived | `NUMERIC(9,4)` | Rounded half-up to 4 decimals |
| Booleans | `true`/`false` text | `BOOLEAN` | Parse to `TRUE`/`FALSE` — S-06 |
| Codes | free text | `VARCHAR` | Uppercase standard code; `UNKNOWN` plus `code_unmapped = TRUE` when unmapped — S-03, S-04 |
| Free text | text | `TEXT` | Trimmed and preserved; no truncation |
| JSON payloads | `jsonb` text | `TEXT` | Canonical compact JSON when valid; raw text when invalid; `json_valid` flag — S-07 |
| Lineage | — | `VARCHAR(64)` | `transformation_run_id` on every output row — Section 8 |

Exact target columns and types are listed in Section 6.1.

### 4.2 Formats and Precision

| Aspect | Rule | Rationale |
| --- | --- | --- |
| Date format | `YYYY-MM-DD` (ISO 8601) | Matches Stage 1 output and sorts lexicographically |
| Timestamp format | ISO 8601 with explicit offset, for example `2026-10-18T01:00:00+08:00` | Keeps Asia/Manila unambiguous without a separate timezone column |
| Time zone | Asia/Manila for all business dates and all `reference_date`-relative derivations | Spartment operates in PHT |
| Numeric precision | Money `NUMERIC(12,2)`; area `NUMERIC(6,2)`; rates `NUMERIC(9,4)` | Widened from source money so cumulative measures cannot overflow; area precision matches the source |
| Rounding | Round half-up to 2 decimals once at conversion; never re-round downstream | Prevents cumulative drift and banker's-rounding bias |
| Currency | Philippine pesos (`PHP`), single currency, no conversion | The source and all reports are PHP |
| Unit of measure | Square meters (`sqm`) for area; days for duration | Matches the source and the operational questions |
| Text case | Emails lowercase; codes uppercase; names and free text preserve source case after trimming | Reliable matching for identifiers; self-evident code sets |
| Booleans | `TRUE` / `FALSE` | Unambiguous in CSV |
| Empty values | Empty CSV field | CSV has no native NULL; an empty field is the NULL representation |

### 4.3 Reference Values and Codes

| Field | Observed source variants | Standard code set | Unmapped handling |
| --- | --- | --- | --- |
| `profiles.role` | `admin`, `tenant`, mixed case | `ADMIN`, `TENANT` | `UNKNOWN` + `code_unmapped = TRUE` |
| `rooms.status` | `available`, `occupied`, `reserved`, `maintenance`, mixed case | `AVAILABLE`, `OCCUPIED`, `RESERVED`, `MAINTENANCE` | `UNKNOWN` + `code_unmapped = TRUE` |
| `leases.status` | `active`, `ended`, mixed case | `ACTIVE`, `ENDED` | `UNKNOWN` + `code_unmapped = TRUE` |
| `tenant_assignments.status` | `active`, `ended`, mixed case | `ACTIVE`, `ENDED` | `UNKNOWN` + `code_unmapped = TRUE` |
| `bills.status` | `pending`, `partially_paid`, `paid`, mixed case and spacing variants | `PENDING`, `PARTIAL`, `PAID` | `UNKNOWN` + `code_unmapped = TRUE`; status is also recomputed from amounts (B-11) |
| `bills.bill_type` | `rent`, `utility`, mixed case | `RENT`, `UTILITY` | `UNKNOWN` + `code_unmapped = TRUE` |
| `inquiries.status` | `New`, `Contacted`, `Resolved`, mixed case | `NEW`, `CONTACTED`, `RESOLVED` | `UNKNOWN` + `code_unmapped = TRUE` |
| `tenant_requests.kind` | `maintenance`, `other`, mixed case | `MAINTENANCE`, `OTHER` | `UNKNOWN` + `code_unmapped = TRUE` |
| `tenant_requests.priority` | `Normal`, `High`, `Urgent`, mixed case | `NORMAL`, `HIGH`, `URGENT` | `UNKNOWN` + `code_unmapped = TRUE` |
| `tenant_requests.status` | `New`, `Scheduled`, `In progress`, `Resolved`, mixed case | `NEW`, `SCHEDULED`, `IN_PROGRESS`, `RESOLVED` | `UNKNOWN` + `code_unmapped = TRUE` |
| `notifications.kind` | `bill`, `payment`, `request`, `tenancy`, mixed case | `BILL`, `PAYMENT`, `REQUEST`, `TENANCY` | `UNKNOWN` + `code_unmapped = TRUE` |
| Currency | — | `PHP` | n/a |
| Unit | — | `sqm` | n/a |
| Region | — | `PH` / `Asia/Manila` | n/a |

### 4.4 Standardization Rule Specification

| Rule ID | Column | Source format | Standard format | On failure |
| --- | --- | --- | --- | --- |
| **S-01** | all `DATE` / `TIMESTAMPTZ` | `YYYY-MM-DD`, ISO 8601 | `YYYY-MM-DD`, ISO 8601 `+08:00` | Reject row, quarantine with `rule_id = S-01` |
| **S-02** | all money and area columns | Numeric text from CSV | `NUMERIC(12,2)` money, `NUMERIC(6,2)` area | Reject row, quarantine with `rule_id = S-02` |
| **S-03** | role and status columns | Mixed-case free text | Section 4.3 code set | Set `UNKNOWN`, flag `code_unmapped` |
| **S-04** | `bills.bill_type` | Mixed-case free text | `RENT` / `UTILITY` | Set `UNKNOWN`, flag `code_unmapped` |
| **S-05** | all money columns | Numeric text from CSV | `NUMERIC(12,2)` | Reject row, quarantine with `rule_id = S-05` |
| **S-06** | booleans | `true`, `false`, `t`, `f`, `1`, `0` | `TRUE` / `FALSE` | Reject row, quarantine with `rule_id = S-06` |
| **S-07** | JSON payload columns | `jsonb` text | Canonical compact JSON, sorted keys | Preserve raw text, set `json_valid = FALSE` |
| **S-08** | surrogate keys | — | `CHAR(16)` lowercase hex | n/a (deterministic; cannot fail) |

**Conversion-failure principle:** a conversion failure means the source value cannot be interpreted at all, such as unparseable money or an invalid UUID. Such a row is rejected to quarantine. A value that is parseable but outside a business range is handled by a cleaning or business rule, never by a conversion rule, so the log distinguishes "unreadable" from "unusable".

---

## 5. Business Transformation Rules

### 5.1 Business Rule Specification

| Rule ID and name | Input columns | Logic / formula | Output column(s) | Business reason |
| --- | --- | --- | --- | --- |
| **B-01** Surrogate keys | source `id` values | `MD5('<entity>:<source_id>')` truncated to 16 hex characters; `dim_date.date_key` is the calendar date as `YYYYMMDD` | `*_key` on every dimension and fact | Stable keys make reruns idempotent and decouple the warehouse from source UUIDs |
| **B-02** Date dimension | all source dates | One row per calendar date across the minimum and maximum source dates; derive year, quarter, month, ISO week, day, weekend flag, and `YYYY-MM` period | `dim_date` | Precomputed calendar attributes keep monthly and weekday reporting consistent across all facts |
| **B-03** Tenant identity resolution | `profiles.id`, `tenant_assignments.id`, `full_name`, `email`, `phone`, `monthly_rent`, `move_in`, `lease_end`, `status`, emergency fields | Union both identity sources on `id`. Profile is authoritative for `full_name`, `phone`, and `role`; assignment supplies assignment attributes when present. Set `identity_source` to `PROFILE`, `ASSIGNMENT`, or `BOTH`. | `dim_tenant` | `profiles` and `tenant_assignments` describe the same people from different workflows. A union prevents a tenant from disappearing when only one side is staged. |
| **B-04** Tenant role and active flags | `profiles.role`, `identity_source`, assignment status, lease status | `is_tenant = (role = 'TENANT' OR identity_source = 'ASSIGNMENT')`; `is_staff = (role = 'ADMIN' AND identity_source <> 'ASSIGNMENT')`; `active_flag = TRUE` when the tenant has an active assignment or active lease | `dim_tenant` | Staff profiles must remain referenceable as actors without inflating tenant counts; an assignment with no matching profile is still a tenant |
| **B-05** Lease duration and status | `move_in_date`, `lease_end_date`, `status` | `lease_days = lease_end_date - move_in_date` when the end date exists; `is_open_ended = lease_end_date IS NULL`; `is_active = (status = 'ACTIVE')` | `dim_lease` | Tenancy length and open-ended status drive occupancy and turnover analysis |
| **B-06** Assignment tenure | `move_in`, `lease_end`, `status` | `tenure_days = lease_end - move_in` when the end date exists; `is_active = (status = 'ACTIVE')` | `fact_assignment` | Assignment tenure is the operational record used by tenant and room workflows |
| **B-07** Room occupancy flags | `rooms.status` | `is_available = (status = 'AVAILABLE')`; `is_occupied = (status = 'OCCUPIED')`; `is_under_maintenance = (status = 'MAINTENANCE')` | `dim_room` | Dashboard filters and occupancy measures should not re-implement status logic |
| **B-08** Building rollups | `dim_room`, `dim_building` | `room_count`, `occupied_rooms`, `available_rooms`, `occupancy_rate = occupied_rooms / room_count`, `vacancy_gap = SUM(monthly_rate)` for available rooms | `dim_building` | Building-level occupancy and vacancy are the primary operational indicators |
| **B-09** Bill total | `rent_amount`, `electricity_amount`, `water_amount` | `total_amount = rent_amount + electricity_amount + water_amount` | `fact_billing.total_amount` | The source stores components but no total; every revenue measure needs the sum |
| **B-10** Billing period attributes | `billing_period` | `billing_year`, `billing_month`, `billing_period = 'YYYY-MM'` | `fact_billing` | Monthly reporting and bill filtering must use one consistent period definition |
| **B-11** Bill payment aggregation and status | `total_amount`, `payments.amount`, `bills.status` | `amount_paid = SUM(payments.amount)` per bill; `balance = MAX(total_amount - amount_paid, 0)`; `collection_rate = amount_paid / total_amount` when total > 0. Recompute status: `PAID` when balance = 0, `PARTIAL` when 0 < balance < total, `PENDING` when amount_paid = 0. Set `status_recomputed = TRUE` when the recomputed status differs from the source status. | `fact_billing.amount_paid`, `balance`, `collection_rate`, `status`, `status_recomputed` | The payment ledger is the source of truth for collections. Source status can lag behind the amounts |
| **B-12** Bill aging | `due_date`, `reference_date` | `days_to_due = due_date - billing_period_date`; `days_overdue = MAX(reference_date - due_date, 0)`; `is_overdue = days_overdue > 0`; bucket `CURRENT`, `D1_30`, `D31_60`, `D61_90`, `D90_PLUS` | `fact_billing` | Collections staff prioritize by age; a raw date difference answers "who is late and by how much" badly for bills not yet due |
| **B-13** Payment allocation order | `bill_key`, `payment_date`, `created_at`, `payment_id` | `payment_allocation_rank = ROW_NUMBER() OVER (PARTITION BY bill_key ORDER BY payment_date, created_at, payment_id)`; `is_oldest_payment = (rank = 1)` | `fact_payment` | Makes the oldest-first receipt order explicit and reproducible |
| **B-14** Payment correction rollup | `payment_corrections` | `correction_count` per payment; `is_corrected = correction_count > 0` | `fact_payment` | A corrected payment is invisible in the payment row alone; the correction ledger must be reflected |
| **B-15** Tenant outstanding bills | `fact_billing`, `dim_tenant` | Per tenant, count bills with `status IN ('PENDING','PARTIAL')` and `due_date < reference_date`; `is_flagged_tenant = outstanding_bill_count >= 3` | `dim_tenant` | Precomputes the administrative follow-up threshold without a per-request window function |
| **B-16** Inquiry age and open flag | `inquiries.status`, `created_at` | `age_days = reference_date - created_at`; `is_open = (status IN ('NEW','CONTACTED'))` | `fact_inquiry` | Prospective tenant follow-up depends on how long an inquiry has been open |
| **B-17** Request resolution time | `tenant_requests.status`, `created_at`, `updated_at` | `resolution_days = updated_at - created_at` when `status = 'RESOLVED'`; `is_open = (status <> 'RESOLVED')` | `fact_tenant_request` | Maintenance and service performance is measured by time to resolution |
| **B-18** Occupancy event type | `previous_lease_id`, `new_lease_id` | `MOVE_OUT` when `new_lease_id` is NULL; `TRANSFER` when both are present and different; otherwise `UNKNOWN` | `fact_occupancy_event.event_type` | The source writes an occupancy event only for a move-out or a transfer, where `previous_lease_id` is always populated and `new_lease_id` distinguishes the two |
| **B-19** Notification linkage | `tenant_id`, `kind`, `source_id` | Resolve tenant key; map kind; retain `source_id` as a polymorphic pointer | `fact_notification` | The notification trail explains which bills, payments, requests, or tenancy changes were communicated |
| **B-20** Room size bounds | `rooms.size_sqm` | `size_out_of_bounds = (size_sqm < 8 OR size_sqm > 14)` | `dim_room.size_out_of_bounds` | The source constraint is 8–14 sqm; an out-of-range value is a data-quality signal, not a reason to drop the room |
| **B-21** Revenue recognition flag | `fact_billing.status`, `balance` | `is_revenue_recognized = (status = 'PAID')` | `fact_billing` | Outstanding and partially paid bills are receivables, not recognized revenue. The flag removes no rows. |

### 5.2 Joins and Lookups

| Join | Left | Right | Key | Type | Unmatched handling |
| --- | --- | --- | --- | --- | --- |
| J-01 | `rooms` | `buildings` | `building_id = id` | INNER (many:1) | Child rejected by C-10 before the join |
| J-02 | `leases` | `profiles` | `tenant_id = id` | INNER (many:1) | Child rejected by C-10 |
| J-03 | `leases` | `rooms` | `room_id = id` | INNER (many:1) | Child rejected by C-10 |
| J-04 | `tenant_assignments` | `profiles` | `id = id` | LEFT (1:0..1) | Assignment supplies tenant identity with `identity_source = ASSIGNMENT` |
| J-05 | `tenant_assignments` | `rooms` | `room_id = id` | LEFT (many:0..1) | Legitimate NULL for ended assignments; no sentinel room |
| J-06 | `bills` | `leases` | `lease_id = id` | INNER (many:1) | Child rejected by C-10 |
| J-07 | `payments` | `bills` | `bill_id = id` | INNER (many:1) | Child rejected by C-10 |
| J-08 | `payment_corrections` | `payments` | `payment_id = id` | INNER (many:1) | Child rejected by C-10 |
| J-09 | `inquiries` | `rooms` | `room_id = id` | LEFT (many:0..1) | Legitimate NULL; inquiry retained |
| J-10 | `tenant_requests` | `profiles` | `tenant_id = id` | INNER (many:1) | Child rejected by C-10 |
| J-11 | `tenant_requests` | `rooms` | `room_id = id` | LEFT (many:0..1) | Legitimate NULL for kind `other` |
| J-12 | `tenant_occupancy_events` | `leases`, `profiles` | `previous_lease_id`, `tenant_id`, `performed_by` | INNER (many:1 each) | Required references are rejected by C-10 when missing |
| J-13 | `tenant_occupancy_events` | `leases` | `new_lease_id` | LEFT (many:0..1) | NULL on a move-out is retained; a non-NULL value must resolve |

**Join cardinality guarantee:** every right-hand key is unique after C-06 and Stage 1 primary-key validation. Therefore no join can multiply rows. Validation V-07 compares pre-join and post-join row counts; any increase is a critical failure.

### 5.3 Filtering and Business Exclusions

| Exclusion ID | Table | Condition | Reason | Recorded as |
| --- | --- | --- | --- | --- |
| **X-01** | `dim_tenant`, `dim_lease`, `fact_assignment` | Tenant, lease, or assignment is inactive or ended | Inactive tenancy must remain visible for history but must not count as current occupancy | `active_flag`, `is_active`, `is_open_ended`; **no rows removed** |
| **X-02** | `fact_billing` | `status <> 'PAID'` or `balance > 0` | Outstanding balances are receivables, not recognized revenue | `is_revenue_recognized`; **no rows removed** |
| **X-03** | `fact_payment_correction` | Always | Corrections are audit evidence. They explain a payment but are not additional cash receipts. | Retained in `fact_payment_correction`; `payments.amount` is not adjusted by corrections |
| **X-04** | `fact_payment` | `payment_date > reference_date` | A future-dated receipt is a data-entry error; including it inflates collections | Reject to quarantine, `rule_id = X-04` |
| **X-05** | `fact_inquiry`, `fact_tenant_request` | Status is `RESOLVED` | Resolved work remains reportable but must not count as open work | `is_open = FALSE`; **no rows removed** |
| **X-06** | `dim_tenant` | `role = 'ADMIN'` | Staff profiles are needed to resolve `performed_by` and correction actors, but are not tenants | `is_tenant = FALSE`, `is_staff = TRUE`; **no rows removed** |

> **Requirement:** extraction may apply only technical filters such as the batch date. Business exclusions belong to this stage. Records are extracted first, then flagged or rejected here with the count and reason recorded, so excluded records remain reportable. Only X-04 removes rows, and every removed row is counted in `records_rejected`.

### 5.4 Dimensional Modeling Steps

| Step | Action | Detail |
| --- | --- | --- |
| **D-01** | Generate surrogate keys | `building_key`, `room_key`, `tenant_key`, `lease_key`, `assignment_key`, `bill_key`, `payment_key`, `correction_key`, `inquiry_key`, `request_key`, `event_key`, `notification_key`, `date_key` |
| **D-02** | Retain natural keys | Source UUIDs are kept beside every surrogate key so a fact can be traced to the exact source record |
| **D-03** | Build `dim_date` | Grain: one row per calendar date across the staged date range |
| **D-04** | Build `dim_building` | Grain: one row per building; attributes plus room and occupancy rollups |
| **D-05** | Build `dim_room` | Grain: one row per room; building key, status, capacity, size, rate, availability flags |
| **D-06** | Build `dim_tenant` | Grain: one row per tenant or staff identity; merged profile and assignment attributes; active and flagged flags |
| **D-07** | Build `dim_lease` | Grain: one row per lease; tenant and room keys; duration and open-ended flags |
| **D-08** | Build facts | One row per assignment, bill, payment, correction, inquiry, request, occupancy event, and notification. Degenerate source keys are retained; `tenant_key` is the conformed dimension |
| **D-09** | SCD handling | Type 1 on `dim_room.status`, `dim_room.monthly_rate`, `dim_tenant` contact and assignment attributes, and `dim_lease.status`. **Justification:** the source stores current state only for these attributes; there is no history table from which a Type 2 row could be sourced. If history becomes a requirement, it must first be added to the source application. |

---

## 6. Transformed Data Contract and Naming Convention

> This is **not** a repeat of the extraction contract. The extraction contract (`docs/extraction.md` Section 6) describes what Stage 2 receives. This contract describes what the Load Stage receives and introduces columns that do not exist in the source: surrogate keys, derived measures, and dimensional structures.

### 6.1 Required Output Schema

All timestamps are rendered as ISO 8601 with an explicit `+08:00` offset. Empty CSV fields represent NULL. Boolean values are `TRUE` or `FALSE`.

#### `dim_date`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `date_key` | `CHAR(8)` | NOT NULL | PK | `YYYYMMDD` |
| `date` | `DATE` | NOT NULL | UNIQUE | Calendar date |
| `year` | `SMALLINT` | NOT NULL | — | Calendar year |
| `quarter` | `SMALLINT` | NOT NULL | — | 1–4 |
| `month` | `SMALLINT` | NOT NULL | — | 1–12 |
| `month_name` | `VARCHAR(9)` | NOT NULL | — | English month name |
| `week` | `SMALLINT` | NOT NULL | — | ISO week |
| `day_of_month` | `SMALLINT` | NOT NULL | — | 1–31 |
| `day_of_week` | `SMALLINT` | NOT NULL | — | 1 = Monday |
| `day_name` | `VARCHAR(9)` | NOT NULL | — | English day name |
| `is_weekend` | `BOOLEAN` | NOT NULL | — | Saturday or Sunday |
| `billing_period` | `CHAR(7)` | NOT NULL | — | `YYYY-MM` |

**Grain:** one row per calendar date.

#### `dim_building`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `building_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `building_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `name` | `VARCHAR(200)` | NOT NULL | — | Trimmed |
| `address` | `TEXT` | NULL | — | Trimmed |
| `room_count` | `INTEGER` | NOT NULL | — | Rooms in the building |
| `occupied_rooms` | `INTEGER` | NOT NULL | — | Rooms with status `OCCUPIED` |
| `available_rooms` | `INTEGER` | NOT NULL | — | Rooms with status `AVAILABLE` |
| `occupancy_rate` | `NUMERIC(9,4)` | NULL | — | `occupied_rooms / room_count`; NULL when no rooms |
| `vacancy_gap` | `NUMERIC(12,2)` | NOT NULL | — | Sum of monthly rates for available rooms |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | — | Source timestamp |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per building.

#### `dim_room`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `room_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `room_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `building_key` | `CHAR(16)` | NOT NULL | FK → `dim_building.building_key` | Conformed dimension |
| `building_id` | `UUID` | NOT NULL | — | Natural building key |
| `room_number` | `VARCHAR(50)` | NOT NULL | — | Trimmed |
| `floor` | `SMALLINT` | NULL | — | Source floor |
| `status` | `VARCHAR(20)` | NOT NULL | — | `AVAILABLE` / `OCCUPIED` / `RESERVED` / `MAINTENANCE` / `UNKNOWN` |
| `status_raw` | `VARCHAR(50)` | NULL | — | Source value before mapping |
| `code_unmapped` | `BOOLEAN` | NOT NULL | — | `TRUE` if the status was unmapped |
| `size_sqm` | `NUMERIC(6,2)` | NULL | — | Square meters |
| `size_out_of_bounds` | `BOOLEAN` | NOT NULL | — | `TRUE` when outside 8–14 |
| `capacity` | `VARCHAR(50)` | NULL | — | Source text; preserved |
| `monthly_rate` | `NUMERIC(12,2)` | NOT NULL | — | PHP per month |
| `description` | `TEXT` | NULL | — | Optional |
| `is_available` | `BOOLEAN` | NOT NULL | — | Derived |
| `is_occupied` | `BOOLEAN` | NOT NULL | — | Derived |
| `is_under_maintenance` | `BOOLEAN` | NOT NULL | — | Derived |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | — | Source timestamp |
| `updated_at` | `TIMESTAMPTZ` | NULL | — | Source timestamp; not trigger verified |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per room.

#### `dim_tenant`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `tenant_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `tenant_id` | `UUID` | NOT NULL | UNIQUE | `profiles.id` or `tenant_assignments.id` |
| `full_name` | `VARCHAR(200)` | NOT NULL | — | Profile name, assignment fallback |
| `phone` | `VARCHAR(50)` | NULL | — | Text, never cast to integer |
| `role` | `VARCHAR(20)` | NOT NULL | — | `ADMIN` / `TENANT` / `UNKNOWN` |
| `is_tenant` | `BOOLEAN` | NOT NULL | — | `role = 'TENANT'` |
| `is_staff` | `BOOLEAN` | NOT NULL | — | `role = 'ADMIN'` |
| `email` | `VARCHAR(255)` | NULL | — | Lowercase, from assignment |
| `current_assignment_id` | `UUID` | NULL | — | Latest active or most recent assignment |
| `monthly_rent` | `NUMERIC(12,2)` | NULL | — | From assignment |
| `move_in` | `DATE` | NULL | — | From assignment |
| `lease_end` | `DATE` | NULL | — | From assignment; NULL for open-ended |
| `assignment_status` | `VARCHAR(20)` | NULL | — | `ACTIVE` / `ENDED` / `UNKNOWN` |
| `emergency_name` | `VARCHAR(200)` | NULL | — | From assignment |
| `emergency_phone` | `VARCHAR(50)` | NULL | — | From assignment |
| `relationship` | `VARCHAR(100)` | NULL | — | From assignment |
| `identity_source` | `VARCHAR(20)` | NOT NULL | — | `PROFILE` / `ASSIGNMENT` / `BOTH` |
| `active_flag` | `BOOLEAN` | NOT NULL | — | Active assignment or active lease |
| `outstanding_bill_count` | `INTEGER` | NOT NULL | — | Past-due pending or partial bills |
| `is_flagged_tenant` | `BOOLEAN` | NOT NULL | — | `outstanding_bill_count >= 3` |
| `code_unmapped` | `BOOLEAN` | NOT NULL | — | `TRUE` if role or assignment status was unmapped |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | — | Earliest available identity timestamp |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per tenant or staff identity.

#### `dim_lease`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `lease_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `lease_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `tenant_key` | `CHAR(16)` | NOT NULL | FK → `dim_tenant.tenant_key` | Conformed dimension |
| `tenant_id` | `UUID` | NOT NULL | — | Natural tenant key |
| `room_key` | `CHAR(16)` | NOT NULL | FK → `dim_room.room_key` | Leased room |
| `room_id` | `UUID` | NOT NULL | — | Natural room key |
| `move_in_date` | `DATE` | NOT NULL | — | Lease start |
| `lease_end_date` | `DATE` | NULL | — | NULL when open-ended |
| `status` | `VARCHAR(20)` | NOT NULL | — | `ACTIVE` / `ENDED` / `UNKNOWN` |
| `is_active` | `BOOLEAN` | NOT NULL | — | `status = 'ACTIVE'` |
| `is_open_ended` | `BOOLEAN` | NOT NULL | — | No end date |
| `lease_days` | `INTEGER` | NULL | — | `lease_end_date - move_in_date` when closed |
| `code_unmapped` | `BOOLEAN` | NOT NULL | — | `TRUE` if status was unmapped |
| `date_corrected` | `BOOLEAN` | NOT NULL | — | `TRUE` if C-08 repaired the dates |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per lease.

#### `fact_assignment`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `assignment_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `assignment_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `tenant_key` | `CHAR(16)` | NOT NULL | FK → `dim_tenant.tenant_key` | Conformed dimension |
| `room_key` | `CHAR(16)` | NULL | FK → `dim_room.room_key` | NULL when ended |
| `monthly_rent` | `NUMERIC(12,2)` | NOT NULL | — | PHP per month |
| `move_in` | `DATE` | NOT NULL | — | Assignment start |
| `lease_end` | `DATE` | NULL | — | NULL when open-ended |
| `status` | `VARCHAR(20)` | NOT NULL | — | `ACTIVE` / `ENDED` / `UNKNOWN` |
| `is_active` | `BOOLEAN` | NOT NULL | — | Derived |
| `tenure_days` | `INTEGER` | NULL | — | `lease_end - move_in` when closed |
| `code_unmapped` | `BOOLEAN` | NOT NULL | — | `TRUE` if status was unmapped |
| `date_corrected` | `BOOLEAN` | NOT NULL | — | `TRUE` if C-08 repaired the dates |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per tenant assignment.

#### `fact_billing`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `bill_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `bill_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `lease_key` | `CHAR(16)` | NOT NULL | FK → `dim_lease.lease_key` | Billed lease |
| `tenant_key` | `CHAR(16)` | NOT NULL | FK → `dim_tenant.tenant_key` | Conformed dimension |
| `room_key` | `CHAR(16)` | NOT NULL | FK → `dim_room.room_key` | Room at billing time |
| `bill_type` | `VARCHAR(20)` | NOT NULL | — | `RENT` / `UTILITY` / `UNKNOWN` |
| `billing_period_date` | `DATE` | NOT NULL | — | Source `billing_period` |
| `billing_year` | `SMALLINT` | NOT NULL | — | Derived |
| `billing_month` | `SMALLINT` | NOT NULL | — | Derived |
| `billing_period` | `CHAR(7)` | NOT NULL | — | `YYYY-MM` |
| `due_date` | `DATE` | NOT NULL | — | Repaired by C-07 |
| `rent_amount` | `NUMERIC(12,2)` | NOT NULL | — | **CHECK ≥ 0** |
| `electricity_amount` | `NUMERIC(12,2)` | NOT NULL | — | **CHECK ≥ 0** |
| `water_amount` | `NUMERIC(12,2)` | NOT NULL | — | **CHECK ≥ 0** |
| `total_amount` | `NUMERIC(12,2)` | NOT NULL | — | Sum of components |
| `amount_paid` | `NUMERIC(12,2)` | NOT NULL | — | Sum of payments |
| `balance` | `NUMERIC(12,2)` | NOT NULL | — | `MAX(total - paid, 0)` |
| `status` | `VARCHAR(20)` | NOT NULL | — | `PAID` / `PARTIAL` / `PENDING` / `UNKNOWN` |
| `status_recomputed` | `BOOLEAN` | NOT NULL | — | `TRUE` if amounts overrode source status |
| `code_unmapped` | `BOOLEAN` | NOT NULL | — | `TRUE` if type or status was unmapped |
| `is_rent` | `BOOLEAN` | NOT NULL | — | `bill_type = 'RENT'` |
| `is_utility` | `BOOLEAN` | NOT NULL | — | `bill_type = 'UTILITY'` |
| `days_to_due` | `INTEGER` | NOT NULL | — | `due_date - billing_period_date` |
| `days_overdue` | `INTEGER` | NOT NULL | — | `MAX(reference_date - due_date, 0)` |
| `is_overdue` | `BOOLEAN` | NOT NULL | — | `days_overdue > 0` |
| `overdue_bucket` | `VARCHAR(10)` | NOT NULL | — | `CURRENT` / `D1_30` / `D31_60` / `D61_90` / `D90_PLUS` |
| `is_revenue_recognized` | `BOOLEAN` | NOT NULL | — | `status = 'PAID'` |
| `due_date_corrected` | `BOOLEAN` | NOT NULL | — | `TRUE` if C-07 repaired the due date |
| `collection_rate` | `NUMERIC(9,4)` | NULL | — | `amount_paid / total_amount`; NULL when total = 0 |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per bill.

#### `fact_payment`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `payment_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `payment_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `bill_key` | `CHAR(16)` | NOT NULL | FK → `fact_billing.bill_key` | Settled bill |
| `lease_key` | `CHAR(16)` | NOT NULL | FK → `dim_lease.lease_key` | Billed lease |
| `tenant_key` | `CHAR(16)` | NOT NULL | FK → `dim_tenant.tenant_key` | Conformed dimension |
| `amount` | `NUMERIC(12,2)` | NOT NULL | — | **CHECK ≥ 0** |
| `payment_date` | `DATE` | NOT NULL | — | `YYYY-MM-DD` |
| `payment_year` | `SMALLINT` | NOT NULL | — | Derived |
| `payment_month` | `SMALLINT` | NOT NULL | — | Derived |
| `reference_note` | `TEXT` | NULL | — | Optional |
| `payment_allocation_rank` | `INTEGER` | NOT NULL | — | Oldest-first rank per bill |
| `is_oldest_payment` | `BOOLEAN` | NOT NULL | — | Rank 1 |
| `correction_count` | `INTEGER` | NOT NULL | — | Corrections for this payment |
| `is_corrected` | `BOOLEAN` | NOT NULL | — | `correction_count > 0` |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per payment receipt.

#### `fact_payment_correction`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `correction_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `correction_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `payment_key` | `CHAR(16)` | NOT NULL | FK → `fact_payment.payment_key` | Corrected payment |
| `payment_id` | `UUID` | NOT NULL | — | Natural payment key |
| `corrected_by` | `UUID` | NULL | — | `auth.users` actor; unverifiable |
| `reason` | `TEXT` | NULL | — | Correction reason |
| `previous_values` | `TEXT` | NULL | — | Canonical JSON when valid |
| `corrected_values` | `TEXT` | NULL | — | Canonical JSON when valid |
| `json_valid` | `BOOLEAN` | NOT NULL | — | FALSE when either payload was malformed |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | — | Correction timestamp |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per payment correction.

#### `fact_inquiry`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `inquiry_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `inquiry_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `room_key` | `CHAR(16)` | NULL | FK → `dim_room.room_key` | NULL when no room is associated |
| `full_name` | `VARCHAR(200)` | NOT NULL | — | Trimmed |
| `email` | `VARCHAR(255)` | NOT NULL | — | Lowercase |
| `phone` | `VARCHAR(50)` | NULL | — | Optional |
| `preferred_room` | `VARCHAR(100)` | NULL | — | Source text |
| `move_in_date` | `DATE` | NULL | — | Requested move-in |
| `status` | `VARCHAR(20)` | NOT NULL | — | `NEW` / `CONTACTED` / `RESOLVED` / `UNKNOWN` |
| `code_unmapped` | `BOOLEAN` | NOT NULL | — | `TRUE` if status was unmapped |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | — | Inquiry timestamp |
| `age_days` | `INTEGER` | NOT NULL | — | `reference_date - created_at` |
| `is_open` | `BOOLEAN` | NOT NULL | — | `status IN ('NEW','CONTACTED')` |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per inquiry.

#### `fact_tenant_request`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `request_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `request_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `tenant_key` | `CHAR(16)` | NOT NULL | FK → `dim_tenant.tenant_key` | Submitting tenant |
| `room_key` | `CHAR(16)` | NULL | FK → `dim_room.room_key` | NULL when kind is `OTHER` |
| `kind` | `VARCHAR(30)` | NOT NULL | — | `MAINTENANCE` / `OTHER` / `UNKNOWN` |
| `subject` | `VARCHAR(300)` | NOT NULL | — | Trimmed |
| `priority` | `VARCHAR(20)` | NOT NULL | — | `NORMAL` / `HIGH` / `URGENT` / `UNKNOWN` |
| `status` | `VARCHAR(20)` | NOT NULL | — | `NEW` / `SCHEDULED` / `IN_PROGRESS` / `RESOLVED` / `UNKNOWN` |
| `code_unmapped` | `BOOLEAN` | NOT NULL | — | `TRUE` if any code was unmapped |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | — | Request creation |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL | — | Last update |
| `resolution_days` | `INTEGER` | NULL | — | `updated_at - created_at` when resolved |
| `is_open` | `BOOLEAN` | NOT NULL | — | `status <> 'RESOLVED'` |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per tenant request.

#### `fact_occupancy_event`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `event_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `event_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `tenant_key` | `CHAR(16)` | NOT NULL | FK → `dim_tenant.tenant_key` | Tenant involved |
| `previous_lease_key` | `CHAR(16)` | NOT NULL | FK → `dim_lease.lease_key` | Lease held before the event |
| `new_lease_key` | `CHAR(16)` | NULL | FK → `dim_lease.lease_key` | NULL for a move-out |
| `performed_by_tenant_key` | `CHAR(16)` | NOT NULL | FK → `dim_tenant.tenant_key` | Actor profile |
| `event_type` | `VARCHAR(20)` | NOT NULL | — | `MOVE_OUT` / `TRANSFER` / `UNKNOWN` |
| `details` | `TEXT` | NULL | — | Canonical JSON when valid |
| `json_valid` | `BOOLEAN` | NOT NULL | — | FALSE when the payload was malformed |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | — | Event timestamp |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per occupancy event.

#### `fact_notification`

| Column | Type | Null | Key | Notes |
| --- | --- | --- | --- | --- |
| `notification_key` | `CHAR(16)` | NOT NULL | PK | Deterministic surrogate |
| `notification_id` | `UUID` | NOT NULL | UNIQUE | Natural key |
| `tenant_key` | `CHAR(16)` | NOT NULL | FK → `dim_tenant.tenant_key` | Recipient |
| `kind` | `VARCHAR(30)` | NOT NULL | — | `BILL` / `PAYMENT` / `REQUEST` / `TENANCY` / `UNKNOWN` |
| `code_unmapped` | `BOOLEAN` | NOT NULL | — | `TRUE` if kind was unmapped |
| `title` | `VARCHAR(300)` | NOT NULL | — | Trimmed |
| `source_id` | `UUID` | NULL | — | Polymorphic pointer; no FK |
| `is_read` | `BOOLEAN` | NOT NULL | — | Read state |
| `created_at` | `TIMESTAMPTZ` | NOT NULL | — | Notification timestamp |
| `transformation_run_id` | `VARCHAR(64)` | NOT NULL | — | Lineage |

**Grain:** one row per notification.

### 6.2 Naming Convention

| Element | Rule | Example |
| --- | --- | --- |
| Table names | `snake_case` with a type prefix | `dim_room`, `fact_billing` |
| Column names | `snake_case` | `billing_period`, `amount_paid` |
| Surrogate keys | `<entity>_key`, type `CHAR(16)` | `room_key`, `bill_key` |
| Natural keys | `<entity>_id` | `room_id`, `bill_id` |
| Foreign keys | Reference the target's surrogate key | `fact_billing.tenant_key` → `dim_tenant.tenant_key` |
| Measures | Noun form ending in the unit or metric | `total_amount`, `monthly_rent`, `occupancy_rate` |
| Booleans | Predicate form, `is_` / `has_` prefix | `is_overdue`, `is_flagged_tenant` |
| Code columns | Uppercase value set; `snake_case` column | `status`, `priority` |
| Timestamps | `*_at` | `created_at`, `updated_at` |
| Lineage | `transformation_run_id` | Present on every output row |
| Abbreviations | Only `sqm` (square meters) and `pct` where displayed; both defined in Section 4.2 | — |

### 6.3 Source-to-Target Mapping

All source columns not listed as derived are carried through after C-01 trimming and the relevant code or type standardization.

| Source | Target | Transformation |
| --- | --- | --- |
| `buildings.id` | `dim_building.building_id`, `building_key` | Direct natural key; surrogate B-01 |
| `buildings.name`, `address` | `dim_building.name`, `address` | Trim C-01 |
| `buildings.created_at` | `dim_building.created_at` | S-01 |
| `rooms.*` | `dim_room.*` | Direct after C-01, C-11, S-03, S-02 |
| `rooms.id` | `dim_room.room_id`, `room_key` | Direct; surrogate B-01 |
| `rooms.building_id` | `dim_room.building_id`, `building_key` | Join J-01; surrogate B-01 |
| `rooms.status` | `dim_room.status`, `status_raw` | Map S-03; raw retained |
| `rooms.size_sqm` | `dim_room.size_sqm`, `size_out_of_bounds` | S-02; B-20 |
| `rooms.monthly_rate` | `dim_room.monthly_rate` | S-02 |
| `profiles.id` | `dim_tenant.tenant_id`, `tenant_key` | Direct; surrogate B-01 |
| `profiles.full_name`, `phone`, `role` | `dim_tenant.full_name`, `phone`, `role` | Trim C-01; map role S-03 |
| `tenant_assignments.id` | `dim_tenant.tenant_id` (fallback), `fact_assignment.assignment_id` | B-03; B-01; C-06 |
| `tenant_assignments.full_name`, `email`, `phone`, emergency fields | `dim_tenant.*` | Trim C-01; email lowercase C-02; B-03 |
| `tenant_assignments.monthly_rent`, `move_in`, `lease_end`, `status` | `dim_tenant.*`, `fact_assignment.*` | S-02; S-01; map S-03; C-08 |
| `tenant_assignments.room_id` | `fact_assignment.room_key` | Join J-05; NULL when ended |
| `leases.*` | `dim_lease.*` | Direct after S-01, S-03 |
| `leases.tenant_id` | `dim_lease.tenant_key`, `tenant_id` | Join J-02; surrogate B-01 |
| `leases.room_id` | `dim_lease.room_key`, `room_id` | Join J-03; surrogate B-01 |
| `leases.move_in_date`, `lease_end_date`, `status` | `dim_lease.*` | S-01; S-03; C-08; B-05 |
| `bills.id` | `fact_billing.bill_id`, `bill_key` | Direct; surrogate B-01 |
| `bills.lease_id` | `fact_billing.lease_key`; `tenant_key`, `room_key` via lease | Join J-06; B-01 |
| `bills.bill_type` | `fact_billing.bill_type`, `is_rent`, `is_utility` | Map S-04; B-09 |
| `bills.billing_period` | `fact_billing.billing_period_date`, `billing_year`, `billing_month`, `billing_period` | S-01; B-10 |
| `bills.due_date` | `fact_billing.due_date`, `due_date_corrected` | S-01; C-07 |
| `bills.rent_amount`, `electricity_amount`, `water_amount` | `fact_billing.*`, `total_amount` | S-02; B-09 |
| `bills.status` | `fact_billing.status`, `status_recomputed` | Map S-03; recompute B-11 |
| `payments.id` | `fact_payment.payment_id`, `payment_key` | Direct; surrogate B-01 |
| `payments.bill_id` | `fact_payment.bill_key`; `lease_key`, `tenant_key` via bill | Join J-07 |
| `payments.amount` | `fact_payment.amount`; `fact_billing.amount_paid`, `balance` | S-02; B-11 |
| `payments.payment_date`, `reference_note`, `created_at` | `fact_payment.*` | S-01; C-01 |
| `payment_corrections.id` | `fact_payment_correction.correction_id`, `correction_key` | Direct; surrogate B-01 |
| `payment_corrections.payment_id` | `fact_payment_correction.payment_key`, `payment_id` | Join J-08 |
| `payment_corrections.corrected_by`, `reason` | `fact_payment_correction.*` | Direct; trim |
| `payment_corrections.previous_values`, `corrected_values` | `fact_payment_correction.*`, `json_valid` | C-12; S-07 |
| `inquiries.id` | `fact_inquiry.inquiry_id`, `inquiry_key` | Direct; surrogate B-01 |
| `inquiries.room_id` | `fact_inquiry.room_key` | Join J-09; NULL allowed |
| `inquiries.full_name`, `email`, `phone`, `preferred_room`, `move_in_date` | `fact_inquiry.*` | C-01; C-02; S-01 |
| `inquiries.status`, `created_at` | `fact_inquiry.status`, `created_at`, `age_days`, `is_open` | S-03; B-16 |
| `tenant_requests.id` | `fact_tenant_request.request_id`, `request_key` | Direct; surrogate B-01 |
| `tenant_requests.tenant_id` | `fact_tenant_request.tenant_key` | Join J-10 |
| `tenant_requests.room_id` | `fact_tenant_request.room_key` | Join J-11; NULL for `other` |
| `tenant_requests.kind`, `subject`, `priority`, `status` | `fact_tenant_request.*` | S-03; trim |
| `tenant_requests.created_at`, `updated_at` | `fact_tenant_request.*`, `resolution_days`, `is_open` | S-01; B-17 |
| `tenant_occupancy_events.id` | `fact_occupancy_event.event_id`, `event_key` | Direct; surrogate B-01 |
| `tenant_occupancy_events.tenant_id` | `fact_occupancy_event.tenant_key` | Join J-12 |
| `tenant_occupancy_events.previous_lease_id`, `new_lease_id` | `fact_occupancy_event.*_lease_key`; `event_type` | S-01; B-18 |
| `tenant_occupancy_events.performed_by` | `fact_occupancy_event.performed_by_tenant_key` | Join J-12; NULL allowed |
| `tenant_occupancy_events.details`, `created_at` | `fact_occupancy_event.details`, `json_valid`, `created_at` | C-12; S-07; S-01 |
| `notifications.id` | `fact_notification.notification_id`, `notification_key` | Direct; surrogate B-01 |
| `notifications.tenant_id` | `fact_notification.tenant_key` | Join J-12 |
| `notifications.kind`, `title`, `source_id`, `is_read`, `created_at` | `fact_notification.*` | S-03; trim; S-06; S-01 |

**Not carried forward:** `auth.users` payloads (never extracted), and no source password, token, or credential field exists in the Stage 1 contract. `payment_corrections.corrected_by` is carried as an opaque UUID and is not joined to a user table because `auth.users` is out of scope.

### 6.4 Schema Consistency

Unexpected changes in the **staged input** are handled as follows:

1. **Input schema check.** Before any cleaning rule runs, each staged CSV header is compared against the Stage 1 contract in `src/spartment/stage1_extract/contract.py` and the `data_contract.column_manifest` in the parent handover. A missing required column is a **critical** failure.
2. **No sidecar dependency.** Stage 1 does not emit per-table `.schema.json` files. Stage 2 must not assume they exist. The handover manifest is the authoritative type source.
3. **Fail fast, no silent coercion.** The batch stops before writing output. Missing columns are never defaulted to NULL, and extra columns are never silently dropped; both are logged.
4. **Added columns tolerated.** A new optional column in the staged input is logged as `input_column_unmapped` and ignored. It does not fail the batch, because Stage 1 may extend its own contract independently.
5. **Versioned contract.** This document is versioned by `rules_version` in Section 8. A change to the source-to-target mapping requires a new contract version and a documented change ticket.
6. **Recovery.** After the contract or the Stage 1 extractor is fixed, reprocess the same `run_date` with the same `parent_run_id`. Reruns are idempotent.

**Contract change log:**

| Contract version | Date | Change | Author |
| --- | --- | --- | --- |
| 1.0 | 2026-10-23 | Initial Stage 2 contract: `dim_date`, `dim_building`, `dim_room`, `dim_tenant`, `dim_lease`, `fact_assignment`, `fact_billing`, `fact_payment`, `fact_payment_correction`, `fact_inquiry`, `fact_tenant_request`, `fact_occupancy_event`, `fact_notification` | data engineering |

### 6.5 Nullable Foreign Keys and Sentinel Policy

Spartment has legitimate nullable references. A missing room, lease, or actor is not a real-world category; inventing a sentinel row would create a joinable record that does not exist.

| Column | Null meaning | Referential validation |
| --- | --- | --- |
| `fact_assignment.room_key` | Assignment is ended and has no room | NULL rows are excluded from V-06; non-NULL must resolve |
| `fact_tenant_request.room_key` | Request kind is `other` | NULL rows are excluded; non-NULL must resolve |
| `fact_inquiry.room_key` | No room was associated with the inquiry | NULL rows are excluded; non-NULL must resolve |
| `fact_occupancy_event.new_lease_key` | Event is a move-out | NULL rows are excluded; non-NULL must resolve |
| `fact_payment_correction.corrected_by` | Actor is unavailable or out of scope (`auth.users`) | Never validated; opaque UUID |
| `fact_notification.source_id` | No source record is linked | Never validated; polymorphic pointer |

Categorical columns that cannot be mapped still use an `UNKNOWN` code with `code_unmapped = TRUE`, because a code is an attribute of a valid row, whereas a missing reference means the relationship does not exist.

---

## 7. Transformation Validation and Data Quality Checks

These checks verify the **transformed output**. Stage 1 validation is not repeated here. Stage 1's `records_extracted` is used only as a reference value for reconciliation.

| Check Name | Target | Purpose | Validation Criteria (Pass / Fail) | Severity |
| --- | --- | --- | --- | --- |
| **V-01** Record reconciliation | All 12 source tables | Detect lost records; prove nothing vanished silently | PASS: `records_read = records_output + records_rejected + duplicates_discarded` per table. FAIL: any unexplained difference | **Critical** |
| **V-02** Duplicate accounting | `payments`, `tenant_assignments` | Prove removed duplicates are accounted for | PASS: `duplicates_discarded` equals the C-06 count and is non-zero only for these two tables. FAIL: unexplained duplicates or a residual in V-01 | **Critical** |
| **V-03** Surrogate key uniqueness | Every `*_key` | Prevent row multiplication | PASS: 0 duplicate values per key column. FAIL: >0 | **Critical** |
| **V-04** Natural key uniqueness | Every `*_id` | Preserve source grain | PASS: 0 duplicates per natural key. FAIL: >0 | **Critical** |
| **V-05** Required columns NOT NULL | Every NOT NULL column in Section 6.1 | Catch rows that escaped C-13 with a missing key or measure | PASS: 0 NULLs. FAIL: >0 | **Critical** |
| **V-06** Referential integrity | Every non-null FK in Section 6.1 | Prevent orphans in the transformed model | PASS: 100% of non-null FKs resolve. FAIL: any non-null orphan | **Critical** |
| **V-07** Join cardinality | Joins J-01…J-13 | Catch a non-unique lookup key inflating totals | PASS: post-join row count equals pre-join row count for every required join. FAIL: any increase | **Critical** |
| **V-08** Non-negative measures | `monthly_rate`, `monthly_rent`, `rent_amount`, `electricity_amount`, `water_amount`, `total_amount`, `amount_paid`, `balance`, `amount` | Enforce commercial invariants | PASS: 0 negative values. FAIL: >0 | **Critical** |
| **V-09** Bill total identity | `total_amount`, components | Prove the total is the sum of its parts | PASS: `total_amount = rent + electricity + water` within 0.01. FAIL: otherwise | **Critical** |
| **V-10** Bill balance identity | `total_amount`, `amount_paid`, `balance` | Prove the payment ledger reconciles | PASS: `balance = MAX(total_amount - amount_paid, 0)` within 0.01. FAIL: otherwise | **Critical** |
| **V-11** Date ordering | `due_date`, `billing_period_date`, lease and assignment dates | Catch impossible date states after C-07/C-08 | PASS: `due_date >= billing_period_date`; end date is NULL or `>= move_in`. FAIL: >0 violations | **Critical** |
| **V-12** Assignment-room invariant | `fact_assignment.status`, `room_key` | Enforce the source invariant | PASS: every `ACTIVE` has a room; every `ENDED` has none. FAIL: >0 violations | **Critical** |
| **V-13** Payment reconciliation | `SUM(fact_payment.amount)` vs staged `payments.amount`, net of C-06/X-04 | Prove collections were not silently altered | PASS: sums agree within 0.01. FAIL: otherwise | **Critical** |
| **V-14** Bill component reconciliation | `SUM` of each bill component vs staged `bills`, net of C-05/C-06 | Prove cleaning did not alter billed amounts | PASS: sums agree within 0.01. FAIL: otherwise | **Critical** |
| **V-15** Exclusion accounting | X-01…X-06 counters | Prove business exclusions are counted and reported | PASS: every exclusion has a counter and the count equals the rows affected. FAIL: missing counter | **Non-critical** |
| **V-16** Type / format conformance | Every output column vs Section 6.1 | Confirm CSV values parse as the contracted type | PASS: 0 values fail parse or format. FAIL: >0 | **Non-critical** |
| **V-17** Code domain conformance | `status`, `role`, `priority`, `kind`, `bill_type`, `overdue_bucket`, `event_type`, `billing_period` | Confirm code sets are respected | PASS: 100% within Section 4.3 and Section 6.1. FAIL: any out-of-domain value | **Non-critical** |
| **V-18** Null rate on optional columns | `phone`, `email`, `lease_end`, `room_key` on nullable facts, `reference_note` | Track degradation of optional data | PASS: within baseline. WARNING: increased vs previous run. FAIL: >90% | **Non-critical** |
| **V-19** Unmapped code rate | `code_unmapped` flags | Detect new code variants | PASS: ≤1% of rows. WARNING: 1–5%. FAIL: >5% | **Non-critical** |
| **V-20** Rejection rate | Per table and overall | Decide acceptance against the pre-declared threshold | PASS: ≤1.0%. WARNING: >1.0% and ≤5.0%. FAIL: >5.0% | **Critical** for FAIL; **Non-critical** for WARNING |
| **V-21** Idempotency assertion | Output of a repeat run | Prove Section 1.4's claim | PASS: same `run_date` and `parent_run_id` produce the same content checksum. FAIL: any difference not explained by a rules-version change | **Non-critical** |
| **V-22** Lineage completeness | `transformation_run_id` on every row | Make every output row traceable | PASS: 0 missing or mismatched values. FAIL: >0 | **Critical** |
| **V-23** Sensitive data absence | All output files | Ensure no credential or `auth.users` payload leaks | PASS: 0 credential fields or DSN fragments. FAIL: >0 | **Critical** |
| **V-24** Room size bounds | `dim_room.size_out_of_bounds` | Confirm the flag matches the source value | PASS: flag equals `size < 8 OR size > 14`. FAIL: any disagreement | **Non-critical** |
| **V-25** Occupancy consistency | `dim_room`, `dim_lease`, `fact_assignment` | Detect rooms whose status contradicts the tenancy records | PASS: 0 `OCCUPIED` rooms without an active lease/assignment and 0 `AVAILABLE` rooms with one. WARNING: any contradiction | **Non-critical** |

**Severity semantics:**

- **Critical** — stops the pipeline. The batch is rejected and quarantined; it does not proceed to Load.
- **Non-critical** — produces a WARNING. The batch is held for manual review but is not auto-rejected.

---

## 8. Transformation Metadata and Log Specification

Each transformation batch records the following fields for monitoring, troubleshooting, auditing, lineage, and recovery.

| Field | Purpose | Example value / format |
| --- | --- | --- |
| `run_id` | Unique ID of the transformation run | `spartment-transform-2026-10-18-0200` |
| `parent_run_id` | Stage 1 `pipeline_run_id` this batch came from | `spartment-extract-2026-10-18-0100` |
| `batch_id` | Unique ID of the batch within the run | `spartment-transform-2026-10-18-0200-b1` |
| `batch_window_start` | Start of the processed batch | `2026-10-18T01:00:00+08:00` |
| `batch_window_end` | End of the processed batch | `2026-10-18T02:00:00+08:00` |
| `reference_date` | "Today" used by date-relative derivations (B-12, B-15, B-16) | `2026-10-18` |
| `table_name` | Output table produced | `fact_billing` |
| `transformation_started_at` | Stage start timestamp | `2026-10-18T02:00:00+08:00` |
| `transformation_ended_at` | Stage end timestamp | `2026-10-18T02:03:41+08:00` |
| `transformation_status` | Final table status | `SUCCESS` / `WARNING` / `FAILED` |
| `records_read` | Rows received from the staged CSV | `1250` |
| `records_output` | Rows written to the output table | `1239` |
| `records_rejected` | Rows written to quarantine | `8` |
| `records_modified` | Rows changed but retained (informational; not part of V-01) | `417` |
| `duplicates_discarded` | Rows removed by C-06 | `3` |
| `rules_applied` | Records affected per rule | `C-01: 54; C-02: 38; C-06: 3; S-03: 51; B-13: 1250` |
| `exclusions_applied` | Records affected per business exclusion | `X-04: 1` |
| `validation_results` | Summary of Section 7 checks | `PASS` / `WARNING: V-18 null rate up on phone` / `FAIL: V-01 reconciliation 1250 != 1239+8` |
| `output_file` | Path to the produced file | `transformed/fact_billing/2026-10-18/fact_billing_2026-10-18.csv` |
| `output_checksum` | SHA-256 of the output file; the idempotency assertion (V-21) | `9f2c...` |
| `quarantine_file` | Path to the rejected-record file, if any | `quarantine/transformation/2026-10-18/fact_billing_rejected.csv` |
| `error_message` / `error_code` | Error detail if applicable | `None` / `S01_FAIL: staged bills missing column` |
| `transformation_duration_ms` | Wall-clock duration | `221000` |
| `rules_version` | Version of the rules applied | `1.0` |

**Example log record (JSON):**

```json
{
  "run_id": "spartment-transform-2026-10-18-0200",
  "parent_run_id": "spartment-extract-2026-10-18-0100",
  "batch_id": "spartment-transform-2026-10-18-0200-b1",
  "batch_window_start": "2026-10-18T01:00:00+08:00",
  "batch_window_end": "2026-10-18T02:00:00+08:00",
  "reference_date": "2026-10-18",
  "table_name": "fact_billing",
  "transformation_started_at": "2026-10-18T02:01:12+08:00",
  "transformation_ended_at": "2026-10-18T02:01:39+08:00",
  "transformation_status": "WARNING",
  "records_read": 1250,
  "records_output": 1239,
  "records_rejected": 8,
  "records_modified": 417,
  "duplicates_discarded": 3,
  "rules_applied": {
    "C-01": 54,
    "C-07": 22,
    "C-08": 6,
    "S-03": 47,
    "B-11": 1239
  },
  "exclusions_applied": {
    "X-04": 1
  },
  "validation_results": "WARNING: V-18 null rate on dim_tenant.phone increased vs baseline; V-01 and V-14 PASS",
  "output_file": "transformed/fact_billing/2026-10-18/fact_billing_2026-10-18.csv",
  "output_checksum": "9f2c4a1d8b7e6f5a3c2d1b0e9f8a7b6c",
  "quarantine_file": "quarantine/transformation/2026-10-18/fact_billing_rejected.csv",
  "error_message": null,
  "transformation_duration_ms": 27000,
  "rules_version": "1.0"
}
```

**Usage:**

- **Monitoring:** Airflow dashboard plus per-table status; V-20 rejection rate is the headline metric.
- **Troubleshooting:** `rules_applied` identifies which rule caused a volume shift; `validation_results` names the failing check; `quarantine_file` locates the offending rows.
- **Auditing:** `run_id` + `parent_run_id` + `rules_version` prove exactly which code transformed which extraction output, and when.
- **Data lineage:** `parent_run_id` links back to the Stage 1 handover; `transformation_run_id` on every output row links forward to the Load Stage.
- **Recovery:** `batch_window_*` + `output_checksum` determine the safe reprocess range. Reprocessing the same `run_date` is always safe because reruns are idempotent; a checksum mismatch against a previous run of the same `run_date` signals a rules-version change and requires review before promotion.

**Usage of Stage 1 metadata:** this stage reads `records_extracted` and `validation_result` from the Stage 1 metadata/handover for reconciliation (V-01) and for the extraction validation summary in the handover (Section 10). It never modifies or re-derives Stage 1 metadata.

---

## 9. Log Retention, Rejected Data, and Access

Stage 2 records, ownership, and retention are defined **separately** from Stage 1, even though both stages use the same log tree.

### 9.1 Transformation Log Retention

| Item | Policy |
| --- | --- |
| **Retention period** | Operational transformation logs: minimum **90 days** (`retention.operational_days` in `config/extraction.toml`). Audit-relevant run summaries (`run_id`, `parent_run_id`, window, counts, `rules_applied`, `validation_results`, `rules_version`): minimum **1 year** (`retention.audit_days`). Justification: the bill-aging bucket (B-12) and the flagged-tenant rule (B-15) mean a single tenancy decision may require reading back up to a year of transformation history to explain an `outstanding_bill_count`. |
| **Storage location** | `logs/spartment-transformation/<run_date>/<run_id>_<table>.json`, alongside the Airflow task log for the same `run_id`. Output data lives in `transformed/<table>/<run_date>/`. |
| **Access permissions** | Data engineers and Super Admin (read). Write restricted to the transformation service account / Airflow worker. No tenant, landlord, or leasing-clerk access. Credentials are never written to logs. |
| **Archiving** | Logs older than 90 days are compressed and moved to cold storage. Audit summaries stay online for 1 year. The archive preserves `run_id` and `parent_run_id` indexing so any lineage lookup still resolves. |
| **Deletion rules** | Logs must not be deleted before the retention period, or while needed for troubleshooting, auditing, or recovery. Deletion after retention requires data-engineer approval and is itself logged. Operational logs may be purged after 90 days; audit records only after 1 year. |

### 9.2 Quarantine (Rejected Records)

| Item | Policy |
| --- | --- |
| **Location** | `quarantine/transformation/<run_date>/<table>_rejected.csv` |
| **Retention** | Minimum **1 year**, matching the audit-summary period, because a rejected bill, payment, or correction is a financial record whose rejection a data engineer may need to explain long after the fact. Retained separately from operational logs. |
| **Content** | One row per rejected record: `run_id`, `parent_run_id`, `table_name`, `rule_id` (the C-/S-/X- rule that rejected it), `rejection_reason` (free text), and the source column values needed to diagnose the issue. |
| **Data minimization** | Only columns necessary to diagnose the rejection are stored. `profiles.phone`, `tenant_assignments.phone`, and `inquiries.phone` are **masked** (e.g. `09***1234`) in quarantine, since a rejected row has no analytical need for readable PII. No password, token, or `auth.users` field is ever written anywhere in this stage — Stage 1 does not extract them and V-23 asserts their absence. |
| **Access permissions** | Data engineers and Super Admin only (read). Write restricted to the transformation service account. |
| **Deletion rules** | Never deleted before 1 year. Deletion requires data-engineer approval and is logged. Rejected financial records (bills, payments, corrections) additionally require Super Admin approval. |
| **Reprocessing** | A quarantined record may be re-admitted by fixing the source and re-running Stage 1 for that window, then reprocessing the transformation batch. Records are **not** manually promoted from quarantine, because that would bypass the reconciliation count in V-01. |

### 9.3 Ownership Summary

| Concern | Stage 1 (Extraction) | Stage 2 (Transformation) |
| --- | --- | --- |
| Logs record | Retrieval errors, extraction counts, source-to-staging reconciliation, range-sanity flags | Cleaning actions, conversion errors, validation failures, rule application counts |
| Log retention | 90 days operational / 1 year audit | 90 days operational / 1 year audit |
| Quarantine content | Rows with NULL identifiers, rejected at source-of-record level | Rows that could not be safely transformed, with `rule_id` + reason + batch ID |
| Quarantine retention | Per `docs/extraction.md` §5 | 1 year, with PII masked |
| Owner | Data engineer | Data engineer (Super Admin approves deletions) |

---

## 10. Transformation Acceptance and Handover Rules

### 10.1 Pre-Declared Rejection-Rate Threshold

> **Threshold: 1.0% per table and overall.**

**Justification (defined before any results are observed):** Spartment billing is operationally complete per lease-period — a bill that is silently missing is a collections and an accountability problem, not a rounding artifact. A rejection rate above 1.0% cannot be explained by isolated corrupt records; it indicates either a systematic source defect (for example a code-set change in the billing application) or a cleaning rule that is too aggressive (for example C-05/C-06 removing more rows than intended). Either cause warrants human review before the batch is allowed to influence collections reporting or flagged-tenant recommendations. Below 1.0%, the rejected records are individually traceable in quarantine and the retained population is materially complete.

The 5.0% escalation to automatic **FAIL** (V-20) exists because above that level the output is no longer a faithful representation of the input and must not be promoted under any review outcome.

### 10.2 Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
| --- | --- | --- |
| All tables PASS | **APPROVED** | Proceed to Load |
| One or more non-critical WARNING | **MANUAL REVIEW** | Hold batch |
| One or more critical FAIL | **REJECTED** | Stop, quarantine, or retry |

Stage 2 mirrors Stage 1's safety posture: **MANUAL REVIEW is blocked by default** and requires an explicit `--allow-manual-review` flag to proceed, and **REJECTED is never overridable**.

Target exit codes:

| Code | Meaning |
| --- | --- |
| 0 | `APPROVED` |
| 1 | Rejected or transformation failure |
| 2 | Configuration or input error |
| 3 | Unusable parent run, or schema drift |
| 5 | `MANUAL REVIEW` |

### 10.3 Required Decisions

- **Automatic Approval:** All critical checks in Section 7 pass, no non-critical WARNING is raised, and the rejection rate is ≤ 1.0%. The transformed output is packaged for the Load Stage automatically, including metadata and validation results.
- **Manual Review:** Any non-critical WARNING is raised (for example V-18 optional-field null rate degradation, V-19 unmapped-code rate above 1%, or V-21 idempotency checksum drift after a rules-version change), **or** the rejection rate is between 1.0% and 5.0%. The batch is held and not promoted. **Responsible: data engineer / Super Admin.** The reviewer inspects `quarantine_file` and `rules_applied`, then explicitly releases or rejects the batch. Release requires `--allow-manual-review`; the hold state and the release decision are logged.
- **Rejection or Failure:** Any critical check fails, or the rejection rate exceeds 5.0%. The batch is **stopped**: no output is promoted to Load, all rejected records are quarantined with their `rule_id`, and the run is logged as `REJECTED`. **Responsible: data engineer** investigates via the Section 8 logs. If the cause is a transient read/parse error, the batch is **retried** up to 3 times with backoff; if the cause is data or contract related, the batch is **not** retried automatically — it is fixed and then reprocessed idempotently for the same `run_date`. If Stage 1 was the cause, Stage 1 is re-triggered for that window first. This decision cannot be overridden.

### 10.4 Handover

The following is passed to the **Load Stage** (loading itself is not performed in this stage):

| Handover artifact | Contents |
| --- | --- |
| Transformed datasets | `transformed/<table>/<run_date>/<table>_<run_date>.csv` for `dim_date`, `dim_building`, `dim_room`, `dim_tenant`, `dim_lease`, `fact_assignment`, `fact_billing`, `fact_payment`, `fact_payment_correction`, `fact_inquiry`, `fact_tenant_request`, `fact_occupancy_event`, `fact_notification`. Column contracts follow Section 6.1 of this document; no `.schema.json` sidecar is produced (see Section 1.2). |
| Metadata | Per-table JSON run log with every Section 8 field, including `parent_run_id` for lineage and `rules_version` for reproducibility |
| Validation results | Per-table PASS / WARNING / FAIL from Section 7, plus the overall decision and the acceptance basis |
| Quarantine | `quarantine/transformation/<run_date>/<table>_rejected.csv` with `rule_id` and reason, available to the Load Stage so it can reconcile rejected counts |
| Aggregation config | `reference_date` and `batch_window_*` used for all date-relative derivations, so the Load Stage can reproduce the same `days_overdue` and `outstanding_bill_count` values |
| Grouping config | `billing_period` (`YYYY-MM`) and building/room keys are pre-computed and standardized, so downstream consumers group monthly and per-building without re-deriving either |

**Example handover summary:**

| Validation Summary | Result |
| --- | --- |
| `dim_date` transformation | PASS |
| `dim_building` transformation | PASS |
| `dim_room` transformation | PASS |
| `dim_tenant` transformation | PASS |
| `dim_lease` transformation | PASS |
| `fact_assignment` transformation | PASS |
| `fact_billing` transformation | WARNING |
| `fact_payment` transformation | PASS |
| `fact_payment_correction` transformation | PASS |
| `fact_inquiry` transformation | PASS |
| `fact_tenant_request` transformation | PASS |
| `fact_occupancy_event` transformation | PASS |
| `fact_notification` transformation | WARNING |
| Extraction Validation Summary (from `parent_run_id`) | APPROVED |
| Rejection rate (overall) | 0.64% (≤ 1.0% threshold) |
| **Overall Transformation** | **MANUAL REVIEW** |

> **Requirement:** the handover contains transformed data only. Loading into the target database or warehouse must not be performed in this stage unless explicitly required. The transformed output is documented, validated, traceable, and packaged; the Load Stage owns persistence.

---

## 11. Common Transformation Problems and Mitigation

| # | Problem description | Possible cause | Impact on the transformation process | Proposed solution / mitigation |
| --- | --- | --- | --- | --- |
| 1 | Duplicate rows after a batch rerun | Batch rerun without idempotent output handling | Every count, sum, and derived attribute is inflated; `outstanding_bill_count` and `collection_rate` are wrong, and flagged-tenant recommendations become unreliable | Deterministic surrogate keys (D-01) plus atomic `os.replace()` writes into run-date-scoped `transformed/<table>/<run_date>/` directories (Section 1.4); V-21 asserts checksum stability across reruns |
| 2 | Late-arriving records missing from the batch | The source application syncs an offline outbox after the batch window closed; `docs/extraction.md` §8 Problem 4 | A lease-period is absent from `fact_billing`, so the tenant's bill and any downstream collections measure show a gap that looks like "no bill" rather than missing data | Reprocess a configurable lookback window (default 3 billing months) rather than only the current window; record `batch_window_start` so the widened scope is auditable; `fact_billing.status_recomputed` flags bills whose status was derived from an incomplete payment set |
| 3 | Unexpected schema change in the staged input | Stage 1 contract extended or a source migration renamed a column | Rule application fails mid-batch; naive handling either crashes or silently writes NULLs into NOT NULL output columns | Input schema check against the Stage 1 contract and the parent handover manifest before any rule runs (Section 6.4 step 1); fail fast without writing output; versioned contract and a schema-change ticket; no silent coercion |
| 4 | Row multiplication after a join | A lookup key is not unique — for example two `dim_tenant` rows share a `tenant_id` because the profile/assignment union in B-03 was implemented inconsistently | Output row counts exceed input row counts; `SUM(total_amount)` and `SUM(amount_paid)` inflate, and the reconciliation checks (V-13/V-14) fail | Uniqueness guaranteed by C-06 and by natural-key checks V-04; V-07 compares pre-join and post-join row counts on every join and treats any increase as **critical**; add a critical uniqueness check whenever a new join is introduced |
| 5 | Tenant identity duplicated or split | `profiles.id` and `tenant_assignments.id` treated as separate people instead of one identity namespace, or an assignment exists without a profile | Tenant counts inflate, or a tenant's bills and requests do not join, so `outstanding_bill_count` is wrong and collections follow-up misses them | B-03 unions both sources on `id` with `identity_source` recording the origin; when only an assignment exists it still produces a tenant row with `identity_source = ASSIGNMENT`; V-04 checks `tenant_id` uniqueness across the union |
| 6 | A new `status`, `kind`, or `priority` value appears after an application release | The leasing or billing app adds a code the pipeline has never seen | Affected rows map to `UNKNOWN`, so aging buckets and request-queue breakdowns silently lose detail | C-11 maps known synonyms and flags unmapped values; V-19 monitors the unmapped rate and raises a **warning** above 1%; `code_unmapped` flags the affected rows so a new code is noticed rather than absorbed |
| 7 | Cash reconciliation mismatch on payments | Bill components, `amount_paid`, and `balance` disagree — typically because the application stored a rounded or partially posted value | `collection_rate` and any cash reconciliation report disagree with the physical cash drawer, undermining trust in the collections analytics | B-09/B-11 recompute `total_amount`, `amount_paid`, and `balance` from components and payments; V-09/V-10 assert the identities within `0.01`; residual mismatches are logged for manual review |
| 8 | Bill aging computed for bills not yet due | `days_overdue` computed as a naive date difference without clamping | Bills still in their grace period appear overdue, inflating the `D1_30` bucket and triggering false tenant flags | B-12 clamps `days_overdue` with `MAX(reference_date - due_date, 0)` and sets `CURRENT` when not yet due; `days_to_due` preserves the pre-due interval; V-11 checks `due_date >= billing_period_date` |
| 9 | Batch exceeds its time window | Row-count growth in `bills`/`payments` history, or the per-tenant window functions in B-15 becoming expensive at scale | The transformation finishes after the Load Stage's slot, delaying or overlapping downstream loads | Process tables in dependency order so the largest tables are not blocked behind the others; partition by `billing_period` in the Load Stage; index `fact_billing(tenant_key, status, due_date)` and `fact_payment(bill_key, payment_date)`; monitor `transformation_duration_ms` against a threshold and alert on growth before it breaches the window |
| 10 | Rounding drift between source sums and transformed sums | Independent rounding of each value versus rounding only at the aggregate | V-13/V-14 fail on a difference of a few centavos, producing a false critical rejection | All money columns are rounded half-up to 2dp exactly once at conversion (Section 4.2) and never re-rounded downstream; aggregate checks compare with a `0.01` tolerance; a larger drift indicates a genuine cleaning-rule defect and is investigated rather than absorbed |
| 11 | Orphaned child records arriving late | A payment or bill was staged in a batch whose parent was rejected in an earlier batch, or a parent row (room, lease, tenant) is missing from the staged set for that window | Orphan facts would either break the output foreign keys or be dropped, and dropping them silently breaks the V-01 reconciliation | C-10 rejects orphans to quarantine with `rule_id=C-10` and counts them in `records_rejected`, so V-01 still balances; a spike indicates a source integrity problem worth investigating |
| 12 | Legitimate NULLs mistaken for orphans | The transformer rejects ended assignments with no room, `other` requests with no room, or move-out events with no new lease | Real history is quarantined as corrupt, inflating the rejection rate and understating turnover | Section 6.5 documents each legitimate nullable reference; C-10 and V-06 exclude NULL FKs from referential validation while still validating non-NULL values |

---

## Expected Output

The documentation must be consistent with the implemented transformation process and provide sufficient information for a team member to understand, validate, monitor, and maintain the transformation stage.

The transformed data must be validated and packaged for handover to the next stage: **Load**. Loading into the target database or data warehouse must not be performed in this stage unless explicitly required.

The transformed output must cover the full star schema defined in Section 6.1: dimensions `dim_date`, `dim_building`, `dim_room`, `dim_tenant`, `dim_lease`; facts `fact_assignment`, `fact_billing`, `fact_payment`, `fact_payment_correction`, `fact_inquiry`, `fact_tenant_request`, `fact_occupancy_event`, `fact_notification`.

> **Note on Section 2:** the profiling percentages in Section 2 are representative estimates based on the Spartment source schema, the Stage 1 contract in `src/spartment/stage1_extract/contract.py`, and the failure modes documented in `docs/extraction.md` §8. They are not measurements. On the first production run of this stage, they must be replaced with the actual counts produced by the profiling step, and `rules_version` must be incremented if any cleaning rule changes as a result.
