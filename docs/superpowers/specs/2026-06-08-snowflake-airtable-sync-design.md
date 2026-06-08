# Snowflake → Airtable File-Stats Sync — Design

**Issue:** [nf-osi/nf-airtable#8](https://github.com/nf-osi/nf-airtable/issues/8)
**Date:** 2026-06-08
**Status:** Approved (pending implementation plan)

## Summary

Add a one-way automation pipeline that computes per-study file and download
statistics from the Synapse Data Warehouse (Snowflake) and upserts them into a
dedicated Airtable table, "Snowflake - File Stats". This surfaces warehouse
metrics in Airtable's dashboard interface for non-technical stakeholders,
consistent with the existing Synapse and Jira syncs.

The pipeline is **fully self-contained**: it derives the NF project list and
project metadata directly from Snowflake (it does not depend on the Synapse or
Jira sync having run first). It ports validated SQL from the existing
`snowflake-streamlit` dashboards so the Airtable numbers match those dashboards.

## Goals

- Surface per-study file stats (counts, bytes) and download activity in Airtable.
- Match the metrics already used in the `snowflake-streamlit` dashboards.
- Follow the repo's established sync pattern and reuse its helpers.
- Run unattended on a daily schedule via GitHub Actions.

## Non-Goals

- Bidirectional sync (Airtable → Snowflake). This is read-only from Snowflake.
- A generic, config-defined raw-SQL runner (Approach C, rejected).
- Augmenting or linking to the existing Synapse studies table (separate table only).
- Time-series / monthly-breakdown tables (only current cumulative snapshot per study).

## Decisions (resolved during brainstorming)

| Question | Decision |
|----------|----------|
| What data to surface | Per-study file stats + download activity |
| Target Airtable table | A new, separate "Snowflake - File Stats" table |
| Project scoping | **Snowflake-native** — derive list + metadata from `node_latest` scope_ids of view `52677631` (Approach A) |
| Snowflake auth | Programmatic Access Token (PAT) |
| Columns | Core file metrics + human-readable size + study name/metadata + last-synced + download/access counts |
| Download window | **All-time cumulative**; `start` configurable (default `2019-01-01`), `end` = today; staff excluded |
| Table creation | Folded inline into the sync script (no separate `setup_*` script) |

## Architecture

A single new script, `sync_snowflake_to_airtable.py`, mirroring
`sync_jira_to_airtable.py`:

```
load_config()
  → ensure_snowflake_auth()
  → query_project_meta()                      # NF project list + metadata
  → extract project_ids, batch (100)
  → query_project_sizes(batch)                # per batch
  → query_project_downloads(batch, start, end)# per batch
  → transform_records()                       # join on project_id (Python)
  → ensure Airtable table exists (auto-create if missing)
  → sync_to_airtable()                        # upsert by project_id
```

Data flows one way (Snowflake → Airtable). Table creation is inline, following
the newer `create_jira_table()` pattern rather than a separate setup script.

## Components

All components live in `sync_snowflake_to_airtable.py` (~400 lines target; split
if it grows past 800).

### Config & auth
- `load_config()` — reads `config.yml` + `creds.yaml`, env vars override, keys
  normalized to lowercase. Copied from the Jira sync. New keys below.
- `ensure_snowflake_auth()` — local: use the pre-configured `snow` connection
  as-is (no action). CI: when `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_USER` /
  `SNOWFLAKE_PAT` are present in the environment, generate a temporary
  `~/.snowflake/config.toml` with a default connection using the
  `PROGRAMMATIC_ACCESS_TOKEN` authenticator.

### Snowflake query execution
- `run_snowflake_query(query)` — runs `snow sql --format JSON -q <query>` via
  `subprocess`, parses JSON, retries transient failures with exponential
  backoff, 5-minute timeout. Ported from `snowflake_cli/fetch_snowflake_data.py`
  with retry added.

### Query builders (ported from `snowflake-streamlit/toolkit/queries.py`)
- `STAFF_USERIDS` — staff user-id exclusion list (ported verbatim).
- `query_project_meta()` → `project_id, project_name, funder, study_leads,
  study_status, data_status`. Uses the `project_scope` CTE flattening
  `node_latest.scope_ids` where `id = 52677631`.
- `query_project_sizes(project_ids)` → `project_id, file_count,
  unique_file_handles, total_bytes`. Combines the file-count logic from
  `fetch_snowflake_data.py` with the content-size sum from `query_project_sizes`.
- `query_project_downloads(project_ids, start_date, end_date)` → `project_id,
  download_bytes, download_unique_files`. From `synapse_event.objectdownload_event`
  joined to `file_latest`, excluding `STAFF_USERIDS`.

### Transform
- `transform_records(meta, sizes, downloads)` — joins the three result sets on
  `project_id` in Python (left-join on the meta list so every NF project appears
  even with zero files/downloads), computes `total_size_readable` (e.g.
  "1.23 TB"), and stamps `last_synced` (UTC ISO). Builds new dicts; no mutation.

### Airtable I/O (reused patterns)
- `_request_with_retry(...)` — copied from the Jira sync.
- `create_snowflake_table(base_id, table_name, pat)` — creates the table via the
  Airtable Metadata API with the schema below.
- `get_airtable_valid_fields(...)` — copied from the Jira sync; drives field
  filtering and table-existence detection.
- `sync_to_airtable(...)` — upsert keyed on `project_id` with change detection
  (skip unchanged), rate-limit sleeps every 10 records. Copied/adapted from the
  Jira sync.
- `main()` — orchestrates, validates required credentials, sets exit codes.

## Airtable Table Schema — "Snowflake - File Stats"

Key field: `project_id`.

| Field | Type | Notes |
|-------|------|-------|
| `project_id` | singleLineText | Synapse project id (numeric string); upsert key |
| `project_name` | singleLineText | studyName annotation |
| `funder` | singleLineText | fundingAgency annotation |
| `study_status` | singleLineText | studyStatus annotation |
| `data_status` | singleLineText | dataStatus annotation |
| `study_leads` | singleLineText | studyLeads annotation (comma-joined) |
| `file_count` | number | distinct file nodes |
| `unique_file_handles` | number | distinct file handle ids |
| `total_bytes` | number | sum of content_size |
| `total_size_readable` | singleLineText | human-readable (GB/TB) |
| `download_bytes` | number | sum of downloaded content_size, staff excluded |
| `download_unique_files` | number | distinct downloaded file handle ids |
| `last_synced` | dateTime | UTC, stamped each run |

## Configuration

### `config.yml` (non-sensitive — committed)
```yaml
# Snowflake Sync Configuration
SNOWFLAKE_TABLE_NAME: "Snowflake - File Stats"
SNOWFLAKE_PROJECT_VIEW_ID: "52677631"
SNOWFLAKE_DATABASE: "SYNAPSE_DATA_WAREHOUSE"
SNOWFLAKE_SCHEMA: "SYNAPSE"
SNOWFLAKE_WAREHOUSE: "COMPUTE_XSMALL"
SNOWFLAKE_ROLE: "DATA_ANALYTICS"
SNOWFLAKE_DOWNLOAD_START_DATE: "2019-01-01"
```

### `creds.yaml` / GitHub secrets (sensitive)
- `AIRTABLE_PAT`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PAT` (programmatic access token)

`example_creds.yaml` gains placeholder Snowflake entries.

## Authentication (PAT)

- **Local:** the developer's existing `snow` CLI connection (configured in
  `~/.snowflake/config.toml`, `PROGRAMMATIC_ACCESS_TOKEN`) is used as-is.
- **CI:** the workflow generates `~/.snowflake/config.toml` from the
  `SNOWFLAKE_*` secrets before running the script.
- PATs expire and must be rotated; document this in `CLAUDE.md` alongside the
  existing Jira PAT-expiry note. A Snowflake auth error in the workflow likely
  means the token expired.

## Error Handling

- Missing required credentials → log the missing list, exit 1.
- Snowflake query failure → retry with backoff; if still failing, exit 1.
- Empty project list → warn, exit 0.
- Airtable table missing → auto-create, then proceed.
- Per-record Airtable errors → counted and logged; the run continues and reports
  created / updated / skipped / error counts.

## GitHub Actions Workflow

`.github/workflows/sync_snowflake_to_airtable.yml`:
- Triggers: `schedule` (daily 05:00 UTC) + `workflow_dispatch`.
- Steps: checkout → setup-python → `pip install -r requirements.txt` → install
  Snowflake CLI (`pip install snowflake-cli-labs` or the official action) →
  write `~/.snowflake/config.toml` from secrets → run
  `python sync_snowflake_to_airtable.py`.
- Secrets: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PAT`, `AIRTABLE_PAT`.

## Testing (pytest, target 80% coverage)

- **Unit:** query builders emit correct SQL (project ids interpolated, staff
  exclusion present, scope view id correct); `transform_records` join logic
  including zero-file/zero-download projects; `total_size_readable` formatting;
  `load_config` precedence (file vs env).
- **Integration:** mock the `snow` subprocess to return sample JSON and mock the
  Airtable API; assert created/updated/skipped counts and field filtering.
- **Manual E2E:** run locally against the real `snow` connection and a test
  Airtable base; verify the table is created and populated.
- Introduces a `tests/` directory (none exists yet).

## Files

**New:**
- `sync_snowflake_to_airtable.py`
- `.github/workflows/sync_snowflake_to_airtable.yml`
- `tests/test_sync_snowflake_to_airtable.py`

**Edit:**
- `config.yml` — Snowflake section
- `example_creds.yaml` — Snowflake placeholders
- `CLAUDE.md` — Snowflake sync docs + PAT-rotation note
- `README.md` — Snowflake sync usage

## Open Risks

- Exact `snow` CLI env-var / `config.toml` shape for PAT auth in CI must be
  confirmed during implementation (the design assumes a generated `config.toml`).
- `project_id` format: Snowflake returns a numeric id; stored as a string key.
  This is independent of the existing studies table's `id` format since the
  tables are separate (no linking required).
