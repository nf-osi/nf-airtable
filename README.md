# NF Data Portal - Airtable Sync

This repository enables bidirectional syncing between Airtable and resources related to the NF Data Portal. Specifically, this syncs Synapse projects and associated metadata from the NF Data Portal to an Airtable base for easier viewing and management.

## Features

- **Bidirectional Syncing**: 
  - **Synapse → Airtable**: Syncs data from Synapse table `syn52677631` to an Airtable table
  - **Airtable → Synapse**: Syncs data from Airtable back to Synapse table
- **Automated Workflows**: GitHub Actions workflows for scheduled and manual syncing in both directions
- **Incremental Updates**: Supports updating existing records based on a key field (prevents duplicates)
- **Data Type Conversion**: Automatically handles date conversions, text fields, and array/list data

## Setup

### Prerequisites

- Python 3.10 or higher
- Airtable Personal Access Token (PAT)
- Synapse Personal Access Token (PAT)
- Airtable Base ID and Table Name

### Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd nf-airtable
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create credentials file:
```bash
cp example_creds.yaml creds.yaml
```

5. Edit `creds.yaml` with your credentials:
```yaml
AIRTABLE_PAT: "patYourTokenHere"
SYNAPSE_PAT: "yourSynapseTokenHere"
```

## Usage

### Initial Setup

Before running regular syncs, you need to create the Airtable table. Use the setup script to automatically create the table structure from your Synapse table:

```bash
export AIRTABLE_BASE_ID="your_base_id"
export AIRTABLE_TABLE_NAME="Your Table Name"  # Optional, defaults to Synapse table name

python setup_airtable_table.py
```

The setup script will:
1. Check if the table already exists (if so, skips creation)
2. Fetch the schema from your Synapse table
3. Attempt to create the table in Airtable automatically using the Metadata API
4. If automatic creation isn't possible, it will print detailed instructions for manual creation
5. Perform an initial data sync to populate the table with all existing data from Synapse

**Setup Script Options:**
- `--skip-table-creation`: Skip table creation (assumes table already exists)
- `--skip-data-sync`: Skip initial data sync (only create table structure)
- `--table-name`: Specify a custom table name

**Example:**
```bash
# Create table structure only (no data sync)
python setup_airtable_table.py --skip-data-sync

# Sync data only (table already exists)
python setup_airtable_table.py --skip-table-creation
```

### Manual Sync

#### Synapse to Airtable

Run the sync script to sync from Synapse to Airtable:

```bash
export AIRTABLE_BASE_ID="your_base_id"
export AIRTABLE_TABLE_NAME="Your Table Name"
export SYNAPSE_KEY_FIELD="id"  # Required: field to match existing records (prevents duplicates)

python sync_synapse_to_airtable.py
```

#### Airtable to Synapse

Run the sync script to sync from Airtable back to Synapse:

```bash
export AIRTABLE_BASE_ID="your_base_id"
export AIRTABLE_TABLE_NAME="Your Table Name"
export SYNAPSE_KEY_FIELD="id"  # Required: field to match existing records (prevents duplicates)

python sync_airtable_to_synapse.py
```

#### Snowflake to Airtable

Run the sync script to surface per-study file counts, total bytes, and download activity from the Synapse Data Warehouse (Snowflake) into Airtable:

**Prerequisites:** Install the Snowflake CLI:
```bash
pip install snowflake-cli-labs
```

For local runs, configure your Snowflake connection in `~/.snowflake/config.toml`. In CI, the script generates this file automatically from the environment variables below.

```bash
export AIRTABLE_BASE_ID="your_base_id"
export SNOWFLAKE_ACCOUNT="your_account"   # only needed in CI; local uses ~/.snowflake/config.toml
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PAT="your_token"

python sync_snowflake_to_airtable.py
```

### Environment Variables

- `AIRTABLE_BASE_ID` (required): Your Airtable base ID
- `AIRTABLE_TABLE_NAME` (required): Name of the Airtable table to sync to
- `SYNAPSE_KEY_FIELD` (required): Field name to use for matching existing records for updates (prevents duplicate records)
- `SYNAPSE_TABLE_ID` (optional): Synapse table ID (defaults to `syn52677631`)

### GitHub Actions

The repository includes GitHub Actions workflows for syncing:

1. **Synapse → Airtable** (`sync_synapse_to_airtable.yml`):
   - Scheduled: Runs daily at 2 AM UTC
   - Manual Trigger: Can be triggered manually from the Actions tab

2. **Airtable → Synapse** (`sync_airtable_to_synapse.yml`):
   - Scheduled: Runs daily at 3 AM UTC (after Synapse → Airtable sync)
   - Manual Trigger: Can be triggered manually from the Actions tab

3. **Jira → Airtable** (`sync_jira_to_airtable.yml`):
   - Scheduled: Runs daily at 4 AM UTC
   - Manual Trigger: Can be triggered manually from the Actions tab

4. **Snowflake → Airtable** (`sync_snowflake_to_airtable.yml`):
   - Scheduled: Runs daily at 5 AM UTC (after all other syncs)
   - Manual Trigger: Can be triggered manually from the Actions tab
   - Syncs per-study file counts, total bytes, and download stats from the Synapse Data Warehouse

#### Setting up GitHub Secrets

Add the following secrets to your GitHub repository:

1. Go to Settings → Secrets and variables → Actions
2. Add the following secrets:
   - `AIRTABLE_PAT`: Your Airtable Personal Access Token
   - `SYNAPSE_PAT`: Your Synapse Personal Access Token (Synapse syncs)
   - `AIRTABLE_BASE_ID`: Your Airtable base ID
   - `AIRTABLE_TABLE_NAME`: Name of your Airtable table
   - `SYNAPSE_KEY_FIELD`: (Required) Field name for matching records (prevents duplicates)
   - `SYNAPSE_TABLE_ID`: (Optional) Synapse table ID (defaults to syn52677631)
   - `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier (Snowflake sync)
   - `SNOWFLAKE_USER`: Your Snowflake username (Snowflake sync)
   - `SNOWFLAKE_PAT`: Your Snowflake programmatic access token (Snowflake sync)

## How It Works

### Synapse → Airtable Sync

1. **Fetch from Synapse**: The script queries the specified Synapse table and retrieves all records
2. **Transform Data**: 
   - Converts epoch milliseconds to ISO date strings for date fields
   - Converts USERID/ENTITYID fields to strings
   - Handles lists/arrays by converting to comma-separated strings
3. **Sync to Airtable**: 
   - Uses `SYNAPSE_KEY_FIELD` to match existing records by their key field value
   - Updates existing records if found
   - Creates new records for entries that don't exist
   - Prevents duplicate records by requiring a unique key field

### Airtable → Synapse Sync

1. **Fetch from Airtable**: The script retrieves all records from the specified Airtable table
2. **Transform Data**: 
   - Converts ISO date strings back to epoch milliseconds for date fields
   - Ensures text fields (USERID/ENTITYID) are properly formatted
   - Handles comma-separated strings and arrays
3. **Sync to Synapse**: 
   - Uses `SYNAPSE_KEY_FIELD` to match existing records by their key field value
   - Updates existing records if found
   - Creates new records for entries that don't exist
   - Prevents duplicate records by requiring a unique key field

## Airtable Base Setup

Before running the sync, ensure your Airtable base has:

1. A table with the same column names as your Synapse table (or configure field mapping)
2. Appropriate field types for the data being synced
3. If using incremental updates, ensure the key field is unique

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Verify your PATs are correct and have appropriate permissions
2. **Table Not Found**: Ensure the Airtable table name matches exactly (case-sensitive)
3. **Field Mismatches**: Ensure Airtable table has columns matching Synapse table column names

### Logging

The script provides detailed logging. Check the output for:
- Connection status
- Number of records fetched
- Sync statistics (created/updated/errors)

## Future Enhancements

- Field mapping configuration
- Conflict resolution strategies
- Webhook support for real-time syncing
- Selective field syncing (sync only specific fields)
- Sync direction configuration (one-way or bidirectional)

## License

See LICENSE file for details.

