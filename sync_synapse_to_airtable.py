#!/usr/bin/env python3
"""
Sync data from Synapse table (syn52677631) to Airtable base.

This script fetches data from a Synapse table and syncs it to an Airtable table,
creating or updating records as needed.
"""

import os
import sys
import yaml
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

try:
    import requests
    import synapseclient
    from synapseclient import Synapse
    from pyairtable import Api
    import pandas as pd
    import numpy as np
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


def convert_epoch_to_date(value: Any) -> Optional[str]:
    """Convert epoch milliseconds to ISO date string for Airtable."""
    if isinstance(value, (int, float)) and value > 1000000000000:
        try:
            dt = datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
            return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        except (ValueError, OSError, OverflowError):
            pass
    return None


def load_config() -> Dict[str, str]:
    """Load configuration and credentials from config.yml and creds.yaml."""
    config = {}

    # Load from config.yml (non-sensitive settings)
    if os.path.exists('config.yml'):
        try:
            with open('config.yml', 'r') as f:
                yaml_config = yaml.safe_load(f)
                if yaml_config and isinstance(yaml_config, dict):
                    # Normalize keys to lowercase
                    config.update({k.lower(): v for k, v in yaml_config.items()})
        except Exception as e:
            logger.warning(f"Could not read config.yml: {e}")

    # Load from creds.yaml (sensitive credentials)
    if os.path.exists('creds.yaml'):
        try:
            with open('creds.yaml', 'r') as f:
                yaml_creds = yaml.safe_load(f)
                if yaml_creds and isinstance(yaml_creds, dict):
                    # Normalize keys to lowercase
                    config.update({k.lower(): v for k, v in yaml_creds.items()})
        except Exception as e:
            logger.warning(f"Could not read creds.yaml: {e}")

    # Environment variables override file (normalize to lowercase)
    env_keys = ['AIRTABLE_PAT', 'SYNAPSE_PAT', 'SYNAPSE_TABLE_NAME',
                'SYNAPSE_TABLE_ID', 'SYNAPSE_KEY_FIELD', 'AIRTABLE_BASE_ID']
    for key in env_keys:
        if key in os.environ:
            config[key.lower()] = os.environ[key]

    # Validate required credentials
    if not config.get('airtable_pat'):
        raise ValueError("AIRTABLE_PAT not found in credentials file or environment")
    if not config.get('synapse_pat'):
        raise ValueError("SYNAPSE_PAT not found in credentials file or environment")

    return config


def get_synapse_schema_info(syn: Synapse, table_id: str) -> Dict[str, List[str]]:
    """Get date, text, and list field names from Synapse table schema."""
    try:
        synapse_columns = list(syn.getTableColumns(table_id))
        date_fields = [
            col.get('name') 
            for col in synapse_columns 
            if col.get('columnType') == 'DATE'
        ]
        text_fields = [
            col.get('name')
            for col in synapse_columns
            if col.get('columnType') in ['USERID', 'ENTITYID', 'STRING', 'LARGETEXT']
        ]
        list_fields = [
            col.get('name')
            for col in synapse_columns
            if col.get('columnType') in ['STRING_LIST', 'ENTITYID_LIST', 'USERID_LIST', 'INTEGER_LIST']
        ]
        if date_fields:
            logger.info(f"Date fields from schema: {', '.join(date_fields)}")
        if list_fields:
            logger.info(f"List fields from schema: {', '.join(list_fields)}")
        return {
            'date_fields': date_fields,
            'text_fields': text_fields,
            'list_fields': list_fields
        }
    except Exception as e:
        logger.warning(f"Could not get schema: {e}")
        return {'date_fields': [], 'text_fields': [], 'list_fields': []}


def get_airtable_table_metadata(
    base_id: str, table_name: str, airtable_pat: str
) -> Tuple[set, Dict[str, set]]:
    """Fetch Airtable table metadata: valid field names and select options.

    Returns (valid_field_names, select_options_map).
    Raises on network/auth failure; callers should handle exceptions.
    """
    url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    headers = {'Authorization': f'Bearer {airtable_pat}'}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    tables = response.json().get('tables', [])
    target_table = None
    for t in tables:
        if t.get('name') == table_name:
            target_table = t
            break

    if not target_table:
        logger.warning(f"Table '{table_name}' not found in Airtable metadata")
        return set(), {}

    valid_fields: set = set()
    select_options: Dict[str, set] = {}
    for field in target_table.get('fields', []):
        valid_fields.add(field['name'])
        if field.get('type') in ('singleSelect', 'multipleSelects'):
            choices = field.get('options', {}).get('choices', [])
            select_options[field['name']] = {c['name'] for c in choices}

    return valid_fields, select_options


def _strip_quotes(val: str) -> str:
    """Strip surrounding double-quote characters from a string value."""
    if len(val) >= 2 and val.startswith('"') and val.endswith('"'):
        return val[1:-1]
    return val


def clean_select_fields(
    fields: Dict[str, Any],
    select_options: Dict[str, set]
) -> Tuple[Dict[str, Any], List[Tuple[str, str]]]:
    """Clean select field values: strip embedded quotes, log new values.

    Synapse data sometimes stores values with embedded quotes
    (e.g. '"other"') that should be cleaned to 'other'. Values not in
    the allowed set are kept (typecast=True on the API call will create
    new options) but logged as new additions.

    Returns (cleaned_fields, [(field_name, new_value), ...]).
    """
    cleaned = dict(fields)
    new_values: List[Tuple[str, str]] = []

    for field_name, allowed in select_options.items():
        if field_name not in cleaned:
            continue

        value = cleaned[field_name]

        if isinstance(value, list):
            result = []
            for v in value:
                if v is None:
                    continue
                s = _strip_quotes(str(v))
                result.append(s)
                if s not in allowed:
                    new_values.append((field_name, s))
            if result:
                cleaned[field_name] = result
            else:
                del cleaned[field_name]
        elif isinstance(value, str):
            s = _strip_quotes(value)
            cleaned[field_name] = s
            if s not in allowed:
                new_values.append((field_name, s))
        else:
            s = _strip_quotes(str(value))
            cleaned[field_name] = s
            if s not in allowed:
                new_values.append((field_name, s))

    return cleaned, new_values


def get_synapse_table_data(syn: Synapse, table_id: str) -> List[Dict[str, Any]]:
    """Fetch all data from a Synapse table."""
    logger.info(f"Fetching data from Synapse table: {table_id}")
    
    try:
        # Query the table
        query = f"SELECT * FROM {table_id}"
        results = syn.tableQuery(query)
        
        # Convert to list of dictionaries
        df = results.asDataFrame()
        records = df.to_dict('records')
        
        logger.info(f"Retrieved {len(records)} records from Synapse")
        return records
    except Exception as e:
        logger.error(f"Error fetching data from Synapse: {e}")
        raise


def sync_to_airtable(
    api: Api,
    base_id: str,
    table_name: str,
    records: List[Dict[str, Any]],
    key_field: str,
    date_fields: Optional[List[str]] = None,
    text_fields: Optional[List[str]] = None,
    list_fields: Optional[List[str]] = None,
    select_options: Optional[Dict[str, set]] = None,
    valid_airtable_fields: Optional[set] = None
) -> None:
    """
    Sync records to Airtable.

    Args:
        api: PyAirtable API instance
        base_id: Airtable base ID
        table_name: Name of the Airtable table
        records: List of records to sync
        key_field: Required field name to use for matching existing records (prevents duplicates)
        date_fields: List of date field names
        text_fields: List of text field names (USERID/ENTITYID)
        list_fields: List of list field names (STRING_LIST, etc.)
        select_options: Map of select field names to their allowed option sets
        valid_airtable_fields: Set of field names that exist in the Airtable table
    """
    if not key_field:
        raise ValueError("key_field is required to prevent duplicate records")
    
    logger.info(f"Syncing {len(records)} records to Airtable table: {table_name} using key field: {key_field}")
    
    table = api.table(base_id, table_name)
    date_fields_set = set(date_fields) if date_fields else set()
    text_fields_set = set(text_fields) if text_fields else set()
    list_fields_set = set(list_fields) if list_fields else set()
    
    # Get existing records using the key field
    existing_records = {}
    try:
        all_existing = table.all()
        existing_records = {
            str(rec['fields'].get(key_field)): rec['id']
            for rec in all_existing
            if key_field in rec.get('fields', {})
        }
        logger.info(f"Found {len(existing_records)} existing records")
    except Exception as e:
        logger.error(f"Could not fetch existing records: {e}")
        raise
    
    created_count = 0
    updated_count = 0
    error_count = 0
    skipped_values_count = 0
    skipped_fields_count = 0
    skipped_by_field: Dict[str, int] = {}
    
    for record in records:
        try:
            # Prepare fields for Airtable (remove None values, convert types)
            fields = {}
            for key, value in record.items():
                # Skip None values
                if value is None:
                    continue
                
                # Check for pandas/numpy NaN - skip NaN values
                try:
                    if pd.isna(value):
                        continue
                except (ValueError, TypeError):
                    try:
                        if isinstance(value, (float, int)) and np.isnan(value):
                            continue
                    except (TypeError, ValueError):
                        pass  # Not a NaN value, continue
                
                # Convert date fields
                if key in date_fields_set:
                    converted = convert_epoch_to_date(value)
                    if converted:
                        fields[key] = converted
                    continue
                
                # Convert USERID/ENTITYID fields to strings
                if key in text_fields_set and not isinstance(value, str):
                    fields[key] = str(value)
                    continue
                
                # Handle list fields - keep as arrays for multipleSelects
                if key in list_fields_set:
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
            
            # Filter out fields that don't exist in the Airtable table
            if valid_airtable_fields:
                unknown = [k for k in fields if k not in valid_airtable_fields]
                for k in unknown:
                    del fields[k]
                if unknown:
                    skipped_fields_count += len(unknown)
                    logger.debug(f"Stripped unknown fields: {', '.join(unknown)}")

            # Ensure key field is present
            if key_field not in fields:
                logger.warning(f"Record missing key field '{key_field}', skipping: {record}")
                error_count += 1
                continue

            # Clean select field values (strip quotes) and log new options
            if select_options:
                fields, new_vals = clean_select_fields(fields, select_options)
                for field_name, new_val in new_vals:
                    logger.info(f"New select option '{new_val}' for field '{field_name}' (will be auto-created)")
                    skipped_by_field[field_name] = skipped_by_field.get(field_name, 0) + 1
                skipped_values_count += len(new_vals)

            # Update existing record or create new one
            record_key = str(fields[key_field])
            if record_key in existing_records:
                table.update(existing_records[record_key], fields, typecast=True)
                updated_count += 1
            else:
                # Create new record
                table.create(fields, typecast=True)
                created_count += 1

        except Exception as e:
            if 'INVALID_MULTIPLE_CHOICE_OPTIONS' in str(e):
                logger.warning(
                    f"Record '{fields.get(key_field, '?')}': invalid select option(s) - {e}")
            else:
                logger.error(f"Error syncing record: {e}")
            error_count += 1

    summary = (
        f"Sync complete: {created_count} created, {updated_count} updated, "
        f"{error_count} errors"
    )
    if skipped_fields_count > 0:
        summary += f", {skipped_fields_count} unknown fields stripped"
    if skipped_values_count > 0:
        summary += f", {skipped_values_count} new select options auto-created"
    logger.info(summary)

    if skipped_by_field and select_options:
        for field_name, count in skipped_by_field.items():
            if count > 5:
                logger.info(
                    f"Field '{field_name}': {count} new options added "
                    f"(had {len(select_options.get(field_name, set()))} existing options)"
                )


def main():
    """Main sync function."""
    # Load configuration and credentials
    try:
        config = load_config()
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

    # Get configuration settings
    SYNAPSE_SOURCE_VIEW_ID = config.get('synapse_table_id', 'syn52677631')
    AIRTABLE_BASE_ID = config.get('airtable_base_id')
    AIRTABLE_TABLE_NAME = config.get('synapse_table_name')  # Use Synapse-specific table name
    SYNAPSE_KEY_FIELD = config.get('synapse_key_field')

    if not AIRTABLE_BASE_ID:
        raise ValueError("AIRTABLE_BASE_ID is required in config.yml or environment")
    if not AIRTABLE_TABLE_NAME:
        raise ValueError("SYNAPSE_TABLE_NAME is required in config.yml or environment")
    if not SYNAPSE_KEY_FIELD:
        raise ValueError("SYNAPSE_KEY_FIELD is required in config.yml or environment to prevent duplicate records")
    
    # Initialize Synapse client
    logger.info("Connecting to Synapse...")
    try:
        syn = Synapse()
        syn.login(authToken=config['synapse_pat'], silent=True)
        logger.info("Successfully connected to Synapse")
    except Exception as e:
        logger.error(f"Failed to connect to Synapse: {e}")
        sys.exit(1)

    # Initialize Airtable API
    logger.info("Connecting to Airtable...")
    try:
        api = Api(config['airtable_pat'])
        logger.info("Successfully connected to Airtable")
    except Exception as e:
        logger.error(f"Failed to connect to Airtable: {e}")
        sys.exit(1)
    
    # Fetch Airtable table metadata for field validation
    valid_airtable_fields: set = set()
    select_options: Dict[str, set] = {}
    try:
        valid_airtable_fields, select_options = get_airtable_table_metadata(
            AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, config['airtable_pat']
        )
        if valid_airtable_fields:
            logger.info(f"Airtable table has {len(valid_airtable_fields)} fields")
        if select_options:
            logger.info(
                f"Select fields: "
                f"{', '.join(f'{k} ({len(v)} options)' for k, v in select_options.items())}"
            )
    except Exception as e:
        logger.warning(
            f"Could not fetch Airtable table metadata: {e}. "
            "Proceeding without field/select validation."
        )

    # Get schema info (date and text fields) from source view
    try:
        schema_info = get_synapse_schema_info(syn, SYNAPSE_SOURCE_VIEW_ID)
        date_fields = schema_info.get('date_fields', [])
        text_fields = schema_info.get('text_fields', [])
        list_fields = schema_info.get('list_fields', [])
    except Exception as e:
        logger.warning(f"Could not get schema info: {e}")
        date_fields = None
        text_fields = None
        list_fields = None
    
    # Fetch data from Synapse source view
    logger.info(f"Reading from Synapse view/table: {SYNAPSE_SOURCE_VIEW_ID}")
    try:
        records = get_synapse_table_data(syn, SYNAPSE_SOURCE_VIEW_ID)
    except Exception as e:
        logger.error(f"Failed to fetch Synapse data: {e}")
        sys.exit(1)
    
    if not records:
        logger.warning("No records found in Synapse table")
        return
    
    # Sync to Airtable
    try:
        sync_to_airtable(
            api=api,
            base_id=AIRTABLE_BASE_ID,
            table_name=AIRTABLE_TABLE_NAME,
            records=records,
            key_field=SYNAPSE_KEY_FIELD,
            date_fields=date_fields,
            text_fields=text_fields,
            list_fields=list_fields,
            select_options=select_options,
            valid_airtable_fields=valid_airtable_fields
        )
        logger.info("Sync completed successfully")
    except Exception as e:
        logger.error(f"Failed to sync to Airtable: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

