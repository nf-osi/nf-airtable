#!/usr/bin/env python3
"""
Sync data from Airtable base to Synapse table (syn52677631).

This script fetches data from an Airtable table and syncs it to a Synapse table,
creating or updating records as needed.
"""

import os
import sys
import yaml
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

try:
    import synapseclient
    from synapseclient import Synapse, Table, Row, RowSet, Table, as_table_columns
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


def convert_date_to_epoch(value: Any) -> Optional[int]:
    """Convert ISO date string or datetime to epoch milliseconds for Synapse."""
    if value is None:
        return None
    
    # If already a number, assume it's already epoch milliseconds
    if isinstance(value, (int, float)):
        return int(value)
    
    # Try parsing as ISO string
    if isinstance(value, str):
        try:
            # Try parsing ISO format (with or without timezone)
            if 'T' in value:
                # ISO format with time
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            else:
                # Date only
                dt = datetime.strptime(value, '%Y-%m-%d')
            # Convert to epoch milliseconds
            return int(dt.timestamp() * 1000)
        except (ValueError, AttributeError):
            pass
    
    # Try parsing as datetime object
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    
    return None


def load_credentials(creds_path: str = "creds.yaml") -> Dict[str, str]:
    """Load credentials from YAML file."""
    creds_file = Path(creds_path)
    if not creds_file.exists():
        raise FileNotFoundError(
            f"Credentials file not found: {creds_path}. "
            "Please create it based on example_creds.yaml"
        )
    
    # Read file content
    with open(creds_file, 'r') as f:
        content = f.read()
    
    # Try to parse as YAML first
    airtable_pat = None
    synapse_pat = None
    try:
        creds = yaml.safe_load(content)
        if isinstance(creds, dict) and creds:
            airtable_pat = creds.get('AIRTABLE_PAT') or creds.get('airtable_pat')
            synapse_pat = creds.get('SYNAPSE_PAT') or creds.get('synapse_pat')
    except yaml.YAMLError:
        pass  # Will try key=value format below
    
    # If YAML parsing didn't work or returned None, try parsing as key=value format
    if not airtable_pat or not synapse_pat:
        for line in content.split('\n'):
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            if line.startswith('AIRTABLE_PAT='):
                airtable_pat = line.split('=', 1)[1].strip('"\'')
            elif line.startswith('SYNAPSE_PAT='):
                synapse_pat = line.split('=', 1)[1].strip('"\'')
    
    # Fallback to environment variables
    airtable_pat = airtable_pat or os.getenv('AIRTABLE_PAT')
    synapse_pat = synapse_pat or os.getenv('SYNAPSE_PAT')
    
    if not airtable_pat:
        raise ValueError("AIRTABLE_PAT not found in credentials file or environment")
    if not synapse_pat:
        raise ValueError("SYNAPSE_PAT not found in credentials file or environment")
    
    return {
        'airtable_pat': airtable_pat,
        'synapse_pat': synapse_pat
    }


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
            'list_fields': list_fields,
            'columns': synapse_columns
        }
    except Exception as e:
        logger.warning(f"Could not get schema: {e}")
        return {'date_fields': [], 'text_fields': [], 'list_fields': [], 'columns': []}


def get_airtable_table_data(api: Api, base_id: str, table_name: str) -> List[Dict[str, Any]]:
    """Fetch all data from an Airtable table."""
    logger.info(f"Fetching data from Airtable table: {table_name}")
    
    try:
        table = api.table(base_id, table_name)
        records = table.all()
        
        # Convert to list of dictionaries with just the fields
        result = []
        for rec in records:
            fields = rec.get('fields', {})
            # Include the Airtable record ID for reference
            fields['_airtable_id'] = rec.get('id')
            result.append(fields)
        
        logger.info(f"Retrieved {len(result)} records from Airtable")
        return result
    except Exception as e:
        logger.error(f"Error fetching data from Airtable: {e}")
        raise


def get_existing_synapse_rowset(syn: Synapse, table_id: str, key_field: str) -> tuple:
    """Get existing records from Synapse as RowSet and indexed by key field."""
    logger.info(f"Fetching existing records from Synapse table: {table_id}")
    
    try:
        query = f"SELECT * FROM {table_id}"
        results = syn.tableQuery(query)
        
        # Get as RowSet for easier manipulation
        try:
            rowset = results.asRowSet()
        except AttributeError:
            # Fallback: create RowSet from DataFrame
            df = results.asDataFrame()
            cols = as_table_columns(list(syn.getTableColumns(table_id)))
            rows = []
            for _, row in df.iterrows():
                row_values = [row.get(col.name if hasattr(col, 'name') else str(col), None) 
                            for col in cols]
                rows.append(Row(row_values))
            rowset = RowSet(rows=rows, headers=cols)
        
        # Index by key field for quick lookup
        existing = {}
        key_field_idx = None
        for idx, col in enumerate(rowset.headers):
            col_name = col.name if hasattr(col, 'name') else str(col)
            if col_name == key_field:
                key_field_idx = idx
                break
        
        if key_field_idx is not None:
            for row in rowset.rows:
                key_value = str(row.values[key_field_idx]) if key_field_idx < len(row.values) else None
                if key_value and key_value != 'nan':
                    existing[key_value] = row
        
        logger.info(f"Found {len(existing)} existing records in Synapse")
        return rowset, existing, key_field_idx
    except Exception as e:
        logger.error(f"Error fetching existing Synapse records: {e}")
        raise


def sync_to_synapse(
    syn: Synapse,
    table_id: str,
    records: List[Dict[str, Any]],
    key_field: str,
    date_fields: Optional[List[str]] = None,
    text_fields: Optional[List[str]] = None,
    list_fields: Optional[List[str]] = None,
    schema_columns: Optional[List[Dict]] = None
) -> None:
    """
    Sync records to Synapse using DataFrame-based approach.
    
    Args:
        syn: Synapse client instance
        table_id: Synapse table/view ID
        records: List of records to sync
        key_field: Required field name to use for matching existing records
        date_fields: List of date field names
        text_fields: List of text field names (USERID/ENTITYID)
        list_fields: List of list field names (STRING_LIST, etc.)
        schema_columns: List of column definitions from Synapse schema
    """
    if not key_field:
        raise ValueError("key_field is required to prevent duplicate records")
    
    logger.info(f"Syncing {len(records)} records to Synapse table/view: {table_id} using key field: {key_field}")
    
    date_fields_set = set(date_fields) if date_fields else set()
    text_fields_set = set(text_fields) if text_fields else set()
    list_fields_set = set(list_fields) if list_fields else set()
    
    # Get existing data as DataFrame
    logger.info("Fetching existing data from Synapse...")
    query = f"SELECT * FROM {table_id}"
    results = syn.tableQuery(query)
    existing_df = results.asDataFrame()
    
    logger.info(f"Found {len(existing_df)} existing records in Synapse")
    
    created_count = 0
    updated_count = 0
    error_count = 0
    
    # Process all Airtable records and update the DataFrame
    skipped_count = 0
    for record in records:
        try:
            # Remove Airtable-specific fields
            fields = {k: v for k, v in record.items() if not k.startswith('_')}
            
            # Ensure key field is present
            if key_field not in fields:
                logger.warning(f"Record missing key field '{key_field}', skipping")
                error_count += 1
                continue
            
            record_key = str(fields[key_field])
            
            # Prepare row data with proper conversions
            row_data = {}
            for key, value in fields.items():
                if value is None:
                    row_data[key] = None
                    continue
                
                # Skip empty lists
                if isinstance(value, (list, tuple)) and len(value) == 0:
                    row_data[key] = []
                    continue
                
                # Skip NaN values (but only for scalars, not lists)
                if not isinstance(value, (list, tuple, np.ndarray)):
                    try:
                        if pd.isna(value):
                            row_data[key] = None
                            continue
                    except (ValueError, TypeError):
                        try:
                            if isinstance(value, (float, int)) and np.isnan(value):
                                row_data[key] = None
                                continue
                        except (TypeError, ValueError):
                            pass
                
                # Convert date fields back to epoch milliseconds
                if key in date_fields_set:
                    converted = convert_date_to_epoch(value)
                    row_data[key] = converted
                    continue
                
                # Handle text fields - ensure they're strings
                if key in text_fields_set and not isinstance(value, str):
                    row_data[key] = str(value)
                    continue
                
                # Handle list fields - keep as lists for comparison, convert to JSON later
                if key in list_fields_set:
                    if isinstance(value, str):
                        # Treat string as a single value (don't split on commas!)
                        row_data[key] = [value] if value else []
                    elif isinstance(value, (list, tuple, np.ndarray)):
                        if isinstance(value, np.ndarray):
                            value = value.flatten().tolist()
                        # Keep as list
                        clean_list = [str(item) for item in value if item is not None]
                        row_data[key] = clean_list
                    else:
                        # Single value - wrap in list
                        row_data[key] = [str(value)]
                    continue
                
                # Handle other fields
                if isinstance(value, str):
                    row_data[key] = value
                elif isinstance(value, (list, tuple, np.ndarray)):
                    # Non-list fields that have array values - convert to string
                    if isinstance(value, np.ndarray):
                        value = value.flatten().tolist()
                    if value:
                        row_data[key] = ', '.join([str(v) for v in value if v is not None])
                    else:
                        row_data[key] = None
                else:
                    row_data[key] = value
            
            # Check if record exists in Synapse
            mask = existing_df[key_field].astype(str) == record_key
            if mask.any():
                # Check if data has actually changed
                existing_row = existing_df[mask].iloc[0]
                has_changes = False
                
                # Fields to exclude from comparison (auto-updated by Synapse)
                exclude_fields = {'etag', 'modifiedOn', 'modifiedBy', 'ROW_ID', 'ROW_VERSION'}
                
                for col_name, new_value in row_data.items():
                    if col_name not in existing_df.columns or col_name in exclude_fields:
                        continue
                    
                    existing_value = existing_row[col_name]
                    
                    # Normalize both values for comparison
                    # For lists/arrays, never null - treat empty list as value
                    if isinstance(existing_value, (list, tuple, np.ndarray)):
                        existing_is_null = False
                    elif existing_value is None:
                        existing_is_null = True
                    else:
                        try:
                            existing_is_null = pd.isna(existing_value)
                        except (ValueError, TypeError):
                            existing_is_null = False
                    
                    if isinstance(new_value, (list, tuple, np.ndarray)):
                        new_is_null = False
                    elif new_value is None:
                        new_is_null = True
                    else:
                        try:
                            new_is_null = pd.isna(new_value)
                        except (ValueError, TypeError):
                            new_is_null = False
                    
                    if existing_is_null:
                        existing_value = None
                    if new_is_null:
                        new_value = None
                    
                    # Convert to comparable types
                    if existing_value is not None and new_value is not None:
                        # For list fields, compare as sorted lists
                        if col_name in list_fields_set:
                            # Ensure both are lists
                            existing_list = existing_value if isinstance(existing_value, list) else (json.loads(existing_value) if isinstance(existing_value, str) else [existing_value])
                            new_list = new_value if isinstance(new_value, list) else (json.loads(new_value) if isinstance(new_value, str) else [new_value])
                            
                            if sorted(existing_list) != sorted(new_list):
                                has_changes = True
                                break
                        elif col_name in date_fields_set:
                            # For date fields, compare with second precision (Airtable only stores seconds)
                            # Convert both to seconds, compare, then check if difference is significant
                            try:
                                existing_sec = int(existing_value) // 1000
                                new_sec = int(new_value) // 1000
                                if existing_sec != new_sec:
                                    has_changes = True
                                    break
                            except (ValueError, TypeError):
                                # If conversion fails, compare as strings
                                if str(existing_value) != str(new_value):
                                    has_changes = True
                                    break
                        else:
                            # For other fields, compare as strings
                            if str(existing_value) != str(new_value):
                                has_changes = True
                                break
                    elif (existing_value is None) != (new_value is None):
                        # One is None and the other isn't
                        has_changes = True
                        break
                
                if has_changes:
                    # Update existing row in DataFrame
                    for col_name, col_value in row_data.items():
                        if col_name in existing_df.columns:
                            # For date fields, ensure we store as Int64 to avoid scientific notation
                            if col_name in date_fields_set and col_value is not None:
                                existing_df.loc[mask, col_name] = pd.array([col_value], dtype='Int64')[0]
                            elif col_name in list_fields_set:
                                # For list fields, store as object to preserve list structure
                                existing_df.at[existing_df[mask].index[0], col_name] = col_value
                            else:
                                existing_df.loc[mask, col_name] = col_value
                    updated_count += 1
                else:
                    # No changes detected, skip update
                    skipped_count += 1
            else:
                # Create new row - add to DataFrame
                # Store list fields properly by using object dtype
                new_row_data = {}
                for k, v in row_data.items():
                    if k in list_fields_set:
                        new_row_data[k] = [v]  # Wrap in list to prevent pandas from unpacking
                    else:
                        new_row_data[k] = v
                new_row_df = pd.DataFrame(new_row_data)
                # Convert date columns to Int64 to avoid scientific notation
                for col in date_fields_set:
                    if col in new_row_df.columns:
                        new_row_df[col] = new_row_df[col].astype('Int64')
                # Unwrap list fields
                for col in list_fields_set:
                    if col in new_row_df.columns:
                        new_row_df[col] = new_row_df[col].apply(lambda x: x[0] if isinstance(x, list) and len(x) == 1 else x)
                existing_df = pd.concat([existing_df, new_row_df], ignore_index=True)
                created_count += 1
                
        except Exception as e:
            logger.error(f"Error processing record {record.get('id', 'unknown')}: {e}")
            error_count += 1
    
    # Store updated DataFrame back to Synapse
    try:
        # Only store if there were actual changes
        if updated_count == 0 and created_count == 0:
            logger.info("No changes detected, skipping Synapse update")
        else:
            logger.info(f"Storing {updated_count + created_count} changed records to Synapse...")
            
            # Ensure date columns are Int64 to avoid scientific notation
            for col in date_fields_set:
                if col in existing_df.columns:
                    existing_df[col] = existing_df[col].astype('Int64')
            
            # Convert list fields to JSON strings for Synapse
            for col in list_fields_set:
                if col in existing_df.columns:
                    def convert_to_json(x):
                        # Check if it's a list first (to avoid pd.isna errors on arrays)
                        if isinstance(x, list):
                            return json.dumps(x, ensure_ascii=False)
                        
                        # Check for null/NaN (only for scalars)
                        if x is None:
                            return None
                        
                        try:
                            if pd.isna(x):
                                return None
                        except (ValueError, TypeError):
                            pass  # Not a scalar, continue
                        
                        # Handle strings
                        if isinstance(x, str):
                            # Already a JSON string, or needs to be wrapped
                            try:
                                json.loads(x)  # Test if it's valid JSON
                                return x
                            except:
                                # Not valid JSON, wrap as single-item list
                                return json.dumps([x], ensure_ascii=False)
                        else:
                            return json.dumps([str(x)], ensure_ascii=False)
                    
                    existing_df[col] = existing_df[col].apply(convert_to_json)
            
            # Use store with the table/view ID
            table_entity = Table(table_id, existing_df)
            syn.store(table_entity)
            logger.info(f"Successfully stored {len(existing_df)} records to Synapse ({updated_count} updated, {created_count} created)")
    except Exception as e:
        logger.error(f"Error storing records to Synapse: {e}")
        raise
    
    logger.info(
        f"Sync complete: {created_count} created, {updated_count} updated, "
        f"{skipped_count} skipped (unchanged), {error_count} errors"
    )


def main():
    """Main sync function."""
    # Configuration - these should be set via environment variables or config file
    # SYNAPSE_TARGET_TABLE_ID is the writable table (not a view)
    # Support both old SYNAPSE_TABLE_ID and new SYNAPSE_TARGET_TABLE_ID for backward compatibility
    SYNAPSE_TARGET_TABLE_ID = os.getenv('SYNAPSE_TARGET_TABLE_ID') or os.getenv('SYNAPSE_TABLE_ID')
    AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
    AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME')
    SYNAPSE_KEY_FIELD = os.getenv('SYNAPSE_KEY_FIELD')  # Required: field to match records
    
    if not AIRTABLE_BASE_ID:
        raise ValueError("AIRTABLE_BASE_ID environment variable is required")
    if not AIRTABLE_TABLE_NAME:
        raise ValueError("AIRTABLE_TABLE_NAME environment variable is required")
    if not SYNAPSE_TARGET_TABLE_ID:
        raise ValueError("SYNAPSE_TARGET_TABLE_ID environment variable is required (must be a table, not a view)")
    if not SYNAPSE_KEY_FIELD:
        raise ValueError("SYNAPSE_KEY_FIELD environment variable is required to prevent duplicate records")
    
    # Load credentials
    try:
        creds = load_credentials()
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        sys.exit(1)
    
    # Initialize Synapse client
    logger.info("Connecting to Synapse...")
    try:
        syn = Synapse()
        syn.login(authToken=creds['synapse_pat'], silent=True)
        logger.info("Successfully connected to Synapse")
    except Exception as e:
        logger.error(f"Failed to connect to Synapse: {e}")
        sys.exit(1)
    
    # Initialize Airtable API
    logger.info("Connecting to Airtable...")
    try:
        api = Api(creds['airtable_pat'])
        logger.info("Successfully connected to Airtable")
    except Exception as e:
        logger.error(f"Failed to connect to Airtable: {e}")
        sys.exit(1)
    
    # Get schema info (date, text, and list fields) from target table
    logger.info(f"Writing to Synapse table: {SYNAPSE_TARGET_TABLE_ID}")
    try:
        schema_info = get_synapse_schema_info(syn, SYNAPSE_TARGET_TABLE_ID)
        date_fields = schema_info.get('date_fields', [])
        text_fields = schema_info.get('text_fields', [])
        list_fields = schema_info.get('list_fields', [])
        schema_columns = schema_info.get('columns', [])
    except Exception as e:
        logger.warning(f"Could not get schema info: {e}")
        date_fields = None
        text_fields = None
        list_fields = None
        schema_columns = None
    
    # Fetch data from Airtable
    try:
        records = get_airtable_table_data(api, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
    except Exception as e:
        logger.error(f"Failed to fetch Airtable data: {e}")
        sys.exit(1)
    
    if not records:
        logger.warning("No records found in Airtable table")
        return
    
    # Sync to Synapse target table
    try:
        sync_to_synapse(
            syn=syn,
            table_id=SYNAPSE_TARGET_TABLE_ID,
            records=records,
            key_field=SYNAPSE_KEY_FIELD,
            date_fields=date_fields,
            text_fields=text_fields,
            list_fields=list_fields,
            schema_columns=schema_columns
        )
        logger.info("Sync completed successfully")
    except Exception as e:
        logger.error(f"Failed to sync to Synapse: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

