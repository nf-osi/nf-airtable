#!/usr/bin/env python3
"""
Setup script to create an Airtable table from a Synapse table schema.

This script:
1. Checks if the table already exists - if yes, exits
2. If not, creates the table structure in Airtable
3. Performs an initial data sync with all records
"""

import os
import sys
import yaml
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from pathlib import Path

try:
    import synapseclient
    from synapseclient import Synapse
    from pyairtable import Api
    import pandas as pd
    import numpy as np
    import requests
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Please install requirements: pip install -r requirements.txt")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_credentials(creds_path: str = "creds.yaml") -> Dict[str, str]:
    """Load credentials from YAML file."""
    creds_file = Path(creds_path)
    if not creds_file.exists():
        raise FileNotFoundError(
            f"Credentials file not found: {creds_path}. "
            "Please create it based on example_creds.yaml"
        )
    
    with open(creds_file, 'r') as f:
        content = f.read()
    
    airtable_pat = None
    synapse_pat = None
    try:
        creds = yaml.safe_load(content)
        if isinstance(creds, dict) and creds:
            airtable_pat = creds.get('AIRTABLE_PAT') or creds.get('airtable_pat')
            synapse_pat = creds.get('SYNAPSE_PAT') or creds.get('synapse_pat')
    except yaml.YAMLError:
        pass
    
    if not airtable_pat or not synapse_pat:
        for line in content.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line.startswith('AIRTABLE_PAT='):
                airtable_pat = line.split('=', 1)[1].strip('"\'')
            elif line.startswith('SYNAPSE_PAT='):
                synapse_pat = line.split('=', 1)[1].strip('"\'')
    
    airtable_pat = airtable_pat or os.getenv('AIRTABLE_PAT')
    synapse_pat = synapse_pat or os.getenv('SYNAPSE_PAT')
    
    if not airtable_pat:
        raise ValueError("AIRTABLE_PAT not found in credentials file or environment")
    if not synapse_pat:
        raise ValueError("SYNAPSE_PAT not found in credentials file or environment")
    
    return {'airtable_pat': airtable_pat, 'synapse_pat': synapse_pat}


def get_synapse_table_schema(syn: Synapse, table_id: str) -> Dict[str, Any]:
    """Get the schema/column information from a Synapse table."""
    logger.info(f"Fetching schema from Synapse table: {table_id}")
    
    table_entity = syn.get(table_id, downloadFile=False)
    synapse_columns = list(syn.getTableColumns(table_id))
    
    query = f"SELECT * FROM {table_id} LIMIT 1"
    results = syn.tableQuery(query)
    df = results.asDataFrame()
    
    synapse_col_types = {col.get('name'): col.get('columnType', '') for col in synapse_columns}
    
    columns = []
    for col in df.columns:
        synapse_type = synapse_col_types.get(col, '')
        
        # Use Synapse schema type first (most reliable)
        if synapse_type == 'DATE':
            col_type = 'dateTime'
        elif synapse_type in ['STRING_LIST', 'ENTITYID_LIST', 'USERID_LIST', 'INTEGER_LIST']:
            # Use multipleSelects for list columns
            col_type = 'multipleSelects'
        elif synapse_type in ['ENTITYID', 'USERID', 'STRING', 'LARGETEXT']:
            col_type = 'singleLineText'
        # Only use pandas dtype inference if Synapse type is not specified
        elif not synapse_type:
            if pd.api.types.is_integer_dtype(df[col]):
                col_type = 'number'
            elif pd.api.types.is_float_dtype(df[col]):
                col_type = 'number'
            elif pd.api.types.is_bool_dtype(df[col]):
                col_type = 'checkbox'
            else:
                col_type = 'singleLineText'
        else:
            # For other Synapse types, default to text
            col_type = 'singleLineText'
        
        columns.append({
            'name': col,
            'type': col_type,
            'synapse_type': synapse_type,
            'is_date': synapse_type == 'DATE',
            'is_list': synapse_type in ['STRING_LIST', 'ENTITYID_LIST', 'USERID_LIST', 'INTEGER_LIST']
        })
    
    date_fields = [col['name'] for col in columns if col.get('is_date')]
    if date_fields:
        logger.info(f"Date fields detected: {', '.join(date_fields)}")
    
    return {
        'columns': columns,
        'table_name': table_entity.get('name', table_id)
    }


def check_table_exists(api: Api, base_id: str, table_name: str) -> bool:
    """Check if a table exists in the Airtable base."""
    try:
        table = api.table(base_id, table_name)
        table.all(limit=1)
        return True
    except Exception:
        return False


def create_airtable_table(api: Api, base_id: str, table_name: str, schema: Dict[str, Any], airtable_pat: str, data_records: List[Dict] = None) -> bool:
    """Create Airtable table using Metadata API with pre-populated choices for multipleSelects."""
    logger.info(f"Creating table '{table_name}' in Airtable...")
    
    # Collect all unique values for multipleSelects fields
    list_field_choices = {}
    if data_records:
        list_fields = [col['name'] for col in schema['columns'] if col.get('is_list')]
        for field_name in list_fields:
            unique_values = set()
            for record in data_records:
                value = record.get(field_name)
                if value is None:
                    continue
                if isinstance(value, (list, tuple)):
                    for item in value:
                        if item is not None:
                            unique_values.add(str(item))
                elif isinstance(value, np.ndarray):
                    for item in value.flatten():
                        if item is not None:
                            unique_values.add(str(item))
            list_field_choices[field_name] = sorted(list(unique_values))
            logger.info(f"Found {len(unique_values)} unique choices for '{field_name}'")
    
    metadata_url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {
        'Authorization': f'Bearer {airtable_pat}',
        'Content-Type': 'application/json'
    }
    
    fields = []
    for col in schema['columns']:
        field_def = {'name': col['name'], 'type': col['type']}
        
        if col['type'] == 'number':
            field_def['options'] = {'precision': 0}
        elif col['type'] == 'dateTime':
            field_def['options'] = {
                'timeZone': 'utc',
                'dateFormat': {'name': 'local'},
                'timeFormat': {'name': '24hour'}
            }
        elif col['type'] == 'multipleSelects':
            # Pre-populate choices from data
            choices = list_field_choices.get(col['name'], [])
            field_def['options'] = {
                'choices': [{'name': choice} for choice in choices]
            }
        
        fields.append(field_def)
    
    payload = {
        'name': table_name,
        'description': 'Table synced from Synapse',
        'fields': fields
    }
    
    response = requests.post(metadata_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        logger.info(f"Successfully created table '{table_name}'")
        return True
    else:
        logger.error(f"Failed to create table: {response.status_code} - {response.text}")
        return False


def convert_epoch_to_date(value: Any) -> Optional[str]:
    """Convert epoch milliseconds to ISO date string for Airtable."""
    if isinstance(value, (int, float)) and value > 1000000000000:
        try:
            dt = datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
            return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        except (ValueError, OSError, OverflowError):
            pass
    return None


def sync_all_data(api: Api, base_id: str, table_name: str, syn: Synapse, synapse_table_id: str, schema: Dict[str, Any], records: List[Dict] = None) -> None:
    """Sync all data from Synapse to Airtable."""
    if records is None:
        logger.info("Fetching all data from Synapse...")
        query = f"SELECT * FROM {synapse_table_id}"
        results = syn.tableQuery(query)
        df = results.asDataFrame()
        records = df.to_dict('records')
        logger.info(f"Retrieved {len(records)} records from Synapse")
    else:
        logger.info(f"Using {len(records)} pre-fetched records from Synapse")
    
    table = api.table(base_id, table_name)
    date_fields = {col['name'] for col in schema['columns'] if col.get('is_date')}
    text_fields = {col['name'] for col in schema['columns'] if col.get('synapse_type') in ['USERID', 'ENTITYID', 'STRING', 'LARGETEXT']}
    list_fields = {col['name'] for col in schema['columns'] if col.get('is_list')}
    
    batch_size = 10
    created_count = 0
    error_count = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batch_fields = []
        
        for record in batch:
            fields = {}
            for key, value in record.items():
                if value is None:
                    continue
                
                # Skip NaN values
                try:
                    if pd.isna(value):
                        continue
                except (ValueError, TypeError):
                    try:
                        if isinstance(value, (float, int)) and np.isnan(value):
                            continue
                    except (TypeError, ValueError):
                        pass
                
                # Convert date fields
                if key in date_fields:
                    converted = convert_epoch_to_date(value)
                    if converted:
                        fields[key] = converted
                    continue
                
                # Convert USERID/ENTITYID fields to strings
                if key in text_fields and not isinstance(value, str):
                    fields[key] = str(value)
                    continue
                
                # Handle list fields - keep as arrays for multipleSelects
                if key in list_fields:
                    if isinstance(value, (list, tuple)):
                        # Keep as list for multipleSelects field
                        clean_values = [str(v) for v in value if v is not None]
                        if clean_values:
                            fields[key] = clean_values
                    elif isinstance(value, np.ndarray):
                        if value.size > 0:
                            clean_values = [str(v) for v in value.flatten() if v is not None]
                            if clean_values:
                                fields[key] = clean_values
                    elif isinstance(value, str):
                        # Single string value - wrap in array
                        fields[key] = [value]
                    continue
                
                # Convert non-list arrays to strings
                if isinstance(value, (list, tuple)):
                    if value:
                        clean_values = [str(v) for v in value if v is not None]
                        if clean_values:
                            fields[key] = ', '.join(clean_values)
                    continue
                
                if isinstance(value, np.ndarray):
                    if value.size > 0:
                        clean_values = [str(v) for v in value.flatten() if v is not None]
                        if clean_values:
                            fields[key] = ', '.join(clean_values)
                    continue
                
                fields[key] = value
            
            if fields:
                batch_fields.append(fields)
        
        if batch_fields:
            try:
                if hasattr(table, 'batch_create'):
                    table.batch_create(batch_fields)
                else:
                    for record_fields in batch_fields:
                        table.create(record_fields)
                created_count += len(batch_fields)
                logger.info(f"Created {created_count}/{len(records)} records...")
            except Exception as e:
                logger.error(f"Error creating batch: {e}")
                # Try individual records
                for record_fields in batch_fields:
                    try:
                        table.create(record_fields)
                        created_count += 1
                    except Exception as e2:
                        logger.warning(f"Error creating record (skipping): {e2}")
                        error_count += 1
    
    logger.info(f"Sync complete: {created_count} created, {error_count} errors")
    
    if error_count > 0:
        logger.warning(f"Some records failed to sync. Please review the errors above.")
    if created_count < len(records):
        logger.warning(f"Expected {len(records)} records but only {created_count} were created.")


def main():
    """Main setup function."""
    SYNAPSE_TABLE_ID = os.getenv('SYNAPSE_TABLE_ID', 'syn52677631')
    AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
    
    if not AIRTABLE_BASE_ID:
        raise ValueError("AIRTABLE_BASE_ID environment variable is required")
    
    # Load credentials
    creds = load_credentials()
    
    # Initialize Synapse client
    logger.info("Connecting to Synapse...")
    syn = Synapse()
    syn.login(authToken=creds['synapse_pat'], silent=True)
    logger.info("Successfully connected to Synapse")
    
    # Get schema from Synapse
    schema = get_synapse_table_schema(syn, SYNAPSE_TABLE_ID)
    table_name = schema.get('table_name', 'Synapse Sync')
    
    # Initialize Airtable API
    logger.info("Connecting to Airtable...")
    api = Api(creds['airtable_pat'])
    logger.info("Successfully connected to Airtable")
    
    # Check if table exists
    if check_table_exists(api, AIRTABLE_BASE_ID, table_name):
        logger.info(f"Table '{table_name}' already exists. Exiting.")
        sys.exit(0)
    
    # Fetch data from Synapse first (needed to populate multipleSelects choices)
    logger.info("Fetching data from Synapse to populate field choices...")
    query = f"SELECT * FROM {SYNAPSE_TABLE_ID}"
    results = syn.tableQuery(query)
    df = results.asDataFrame()
    records = df.to_dict('records')
    logger.info(f"Retrieved {len(records)} records from Synapse")
    
    # Create table with pre-populated choices
    if not create_airtable_table(api, AIRTABLE_BASE_ID, table_name, schema, creds['airtable_pat'], records):
        logger.error("Failed to create table. Exiting.")
        sys.exit(1)
    
    # Sync all data (use the same records we already fetched)
    sync_all_data(api, AIRTABLE_BASE_ID, table_name, syn, SYNAPSE_TABLE_ID, schema, records)
    
    logger.info("Setup completed successfully!")


if __name__ == "__main__":
    main()
