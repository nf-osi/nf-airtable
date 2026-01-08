#!/usr/bin/env python3
"""
Create a Synapse table from an existing view/table structure.

This script duplicates the structure and data from a Synapse view/table
to a new table in a specified project.
"""

import os
import sys
import yaml
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path

try:
    import synapseclient
    from synapseclient import Synapse, Table, Row, RowSet, as_table_columns
    from synapseclient.table import Column, Schema
    import pandas as pd
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
    
    if not synapse_pat:
        raise ValueError("SYNAPSE_PAT not found in credentials file or environment")
    
    return {
        'airtable_pat': airtable_pat,
        'synapse_pat': synapse_pat
    }


def create_table_from_view(
    syn: Synapse,
    source_view_id: str,
    target_project_id: str,
    table_name: Optional[str] = None
) -> str:
    """
    Create a new table in the target project with the same data as the source view.
    
    Args:
        syn: Synapse client
        source_view_id: ID of the source view/table
        target_project_id: ID of the target project
        table_name: Name for the new table (defaults to source name + " (Copy)")
    
    Returns:
        ID of the created table
    """
    logger.info(f"Fetching data from source view/table: {source_view_id}")
    
    # Get source entity info
    source_entity = syn.get(source_view_id, downloadFile=False)
    source_name = source_entity.get('name', 'Synapse Table')
    
    if not table_name:
        table_name = f"{source_name} (Test Copy)"
    
    logger.info(f"Creating table '{table_name}' in project {target_project_id}")
    
    # Copy data - let Synapse infer schema from the data
    # This approach is more reliable than manually creating columns
    copy_data = os.getenv('COPY_DATA', 'true').lower() == 'true'
    
    if copy_data:
        logger.info("Fetching all data from source (this may take a moment)...")
        query = f"SELECT * FROM {source_view_id}"
        results = syn.tableQuery(query)
        df = results.asDataFrame()
        logger.info(f"Retrieved {len(df)} rows with {len(df.columns)} columns")
        
        # Store DataFrame as table - Synapse will infer schema from data
        logger.info("Creating table from data...")
        cols = as_table_columns(df)
        table_schema = Schema(name=table_name, columns=cols, parent=target_project_id)
        table = Table(schema=table_schema, values=df)
        table_entity = syn.store(table)
        table_id = table_entity.tableId if hasattr(table_entity, 'tableId') else table_entity.id
        
        logger.info(f"Successfully created and populated table with ID: {table_id}")
    else:
        logger.info("COPY_DATA=false, creating empty table...")
        # Create empty table - query with LIMIT 0 to get structure only
        query = f"SELECT * FROM {source_view_id} LIMIT 1"
        results = syn.tableQuery(query)
        df = results.asDataFrame()
        # Clear the data but keep structure
        empty_df = pd.DataFrame(columns=df.columns)
        
        logger.info("Creating empty table from structure...")
        cols = as_table_columns(empty_df)
        table_schema = Schema(name=table_name, columns=cols, parent=target_project_id)
        table = Table(schema=table_schema, values=empty_df)
        table_entity = syn.store(table)
        table_id = table_entity.tableId if hasattr(table_entity, 'tableId') else table_entity.id
        
        logger.info(f"Successfully created empty table with ID: {table_id}")
    
    return table_id
    
    return table_id


def main():
    """Main function."""
    SOURCE_VIEW_ID = os.getenv('SOURCE_VIEW_ID', 'syn52677631')
    TARGET_PROJECT_ID = os.getenv('TARGET_PROJECT_ID', 'syn26451327')
    TABLE_NAME = os.getenv('TABLE_NAME')  # Optional
    
    logger.info(f"Source view/table: {SOURCE_VIEW_ID}")
    logger.info(f"Target project: {TARGET_PROJECT_ID}")
    
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
    
    # Create table from view
    try:
        table_id = create_table_from_view(
            syn=syn,
            source_view_id=SOURCE_VIEW_ID,
            target_project_id=TARGET_PROJECT_ID,
            table_name=TABLE_NAME
        )
        logger.info(f"Table creation completed successfully!")
        logger.info(f"New table ID: {table_id}")
        logger.info(f"Use this ID as SYNAPSE_TARGET_TABLE_ID in your sync scripts")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

