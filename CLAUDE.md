# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository provides bidirectional data synchronization between Airtable and the NF Data Portal's data sources (Synapse and Jira). The primary purpose is to enable easier viewing and management of scientific data and project tracking information in Airtable while maintaining synchronization with source systems.

## Development Commands

### Environment Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create credentials file
cp example_creds.yaml creds.yaml
# Edit creds.yaml with your PATs
```

### Running Sync Scripts

All sync scripts use credentials from `creds.yaml` (for local development) or environment variables (for GitHub Actions). Required environment variables vary by script but typically include:
- `AIRTABLE_PAT`, `SYNAPSE_PAT` (or `JIRA_PAT`, `JIRA_EMAIL`, `JIRA_SERVER`)
- `AIRTABLE_BASE_ID`, `AIRTABLE_TABLE_NAME`
- `SYNAPSE_KEY_FIELD` (required for Synapse syncs to prevent duplicates)
- `SYNAPSE_TABLE_ID` (optional, defaults to syn52677631)

```bash
# Initial setup - create Airtable table from Synapse schema
export AIRTABLE_BASE_ID="your_base_id"
export AIRTABLE_TABLE_NAME="Your Table Name"
python setup_airtable_table.py

# Sync Synapse → Airtable
export SYNAPSE_KEY_FIELD="id"  # Required
python sync_synapse_to_airtable.py

# Sync Airtable → Synapse
export SYNAPSE_KEY_FIELD="id"  # Required
python sync_airtable_to_synapse.py

# Sync Jira → Airtable
python sync_jira_to_airtable.py
```

## Architecture

### Core Sync Pattern

All sync scripts follow a consistent architecture:

1. **Credential Loading** (`load_credentials()`)
   - Reads from `creds.yaml` (YAML format) or environment variables
   - Environment variables override file credentials
   - Keys are normalized to lowercase internally

2. **Schema Introspection** (`get_synapse_schema_info()`)
   - Queries source system schema to identify field types
   - Categorizes fields: DATE, TEXT (USERID/ENTITYID/STRING), LIST (STRING_LIST, etc.)
   - This drives data transformation logic

3. **Data Fetching**
   - `get_synapse_table_data()`: Queries Synapse table using SQL-like syntax
   - `get_airtable_table_data()`: Fetches all records from Airtable table
   - `get_jira_issues()`: Fetches issues with pagination support
   - Returns list of dictionaries

4. **Data Transformation**
   - **Synapse → Airtable**: Convert epoch milliseconds to ISO date strings, arrays to comma-separated strings (or keep as arrays for multipleSelects fields)
   - **Airtable → Synapse**: Convert ISO dates back to epoch milliseconds, handle JSON arrays for LIST fields
   - Handles pandas/numpy NaN values, skips None values

5. **Upsert Logic** (`sync_to_airtable()`, `sync_to_synapse()`)
   - Fetches existing records indexed by `key_field`
   - For each record: check if key exists → update if found, create if new
   - **Important**: The `key_field` parameter is required to prevent duplicates
   - For Airtable → Synapse sync: Uses DataFrame-based approach with change detection (skips updates if no changes)

### Key Differences Between Scripts

**Synapse Syncs:**
- `sync_synapse_to_airtable.py`: Reads from a Synapse view/table (SOURCE_VIEW_ID), writes to Airtable
- `sync_airtable_to_synapse.py`: Reads from Airtable, writes to Synapse table (TARGET_TABLE_ID)
  - Must write to a table, not a view
  - Currently disabled in GitHub Actions (`if: false` in workflow)
  - Uses DataFrame-based approach for batch updates
  - Includes change detection to avoid unnecessary updates

**Jira Sync:**
- `sync_jira_to_airtable.py`: One-way sync from Jira to Airtable
- Includes connection testing functions
- Handles pagination with nextPageToken
- Supports JQL queries for filtering

### Data Type Handling

**DATE fields (Synapse columnType: DATE):**
- Synapse stores as epoch milliseconds (int64)
- Airtable expects ISO 8601 strings: `YYYY-MM-DDTHH:MM:SS.000Z`
- Conversion functions: `convert_epoch_to_date()`, `convert_date_to_epoch()`

**TEXT fields (USERID, ENTITYID, STRING, LARGETEXT):**
- Converted to strings to ensure proper formatting

**LIST fields (STRING_LIST, ENTITYID_LIST, etc.):**
- Synapse stores as JSON arrays: `["item1", "item2"]`
- Airtable multipleSelects fields accept arrays directly
- When syncing back to Synapse, arrays are converted to JSON strings
- Non-list arrays in non-list columns become comma-separated strings

**Change Detection (Airtable → Synapse only):**
- Excludes auto-updated fields: `etag`, `modifiedOn`, `modifiedBy`, `ROW_ID`, `ROW_VERSION`
- For LIST fields: Compares as sorted lists (order-independent)
- For DATE fields: Compares with second precision (Airtable limitation)
- Skips updates when no actual changes detected

## GitHub Actions

Three workflows are configured:
- `sync_synapse_to_airtable.yml`: Runs daily at 2 AM UTC, or manually
- `sync_airtable_to_synapse.yml`: Scheduled for 3 AM UTC but currently disabled (`if: false`)
- `sync_jira_to_airtable.yml`: Runs daily at 4 AM UTC, or manually

Synapse workflows require GitHub secrets: `AIRTABLE_PAT`, `SYNAPSE_PAT`. Jira workflow requires: `AIRTABLE_PAT`, `JIRA_EMAIL`, `JIRA_PAT`. Non-sensitive config (base ID, table names, Jira server/project) is in `config.yml`.

### Re-enabling Auto-Disabled Workflows

GitHub automatically disables scheduled workflows after 60 days of repository inactivity. To re-enable:

1. Go to the repository's **Actions** tab
2. Select the disabled workflow from the left sidebar
3. Click **"Enable workflow"** banner at the top
4. Optionally click **"Run workflow"** to trigger an immediate run

To prevent auto-disablement, ensure the repo has at least one commit or manual workflow run within every 60-day window.

## Important Notes

- **Credentials**: Never commit `creds.yaml` (it's gitignored). Always use `example_creds.yaml` as template.
- **Key Field**: The `SYNAPSE_KEY_FIELD` is mandatory for sync scripts to prevent duplicate records. It should be a unique identifier (typically "id").
- **Views vs Tables**: When syncing back to Synapse (Airtable → Synapse), you must specify a writable table ID, not a view. The environment variable naming distinguishes this: `SYNAPSE_SOURCE_VIEW_ID` for reading, `SYNAPSE_TARGET_TABLE_ID` for writing.
- **Default Synapse Table**: If not specified, syncs default to table `syn52677631`.
- **Jira API Token Renewal**: Jira API tokens expire after 1 year. Regenerate at https://id.atlassian.com/manage-profile/security/api-tokens and update the `JIRA_PAT` secret in GitHub repo settings. A 401 error in the Jira sync workflow indicates the token has likely expired.
