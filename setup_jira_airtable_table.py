#!/usr/bin/env python3
"""
Setup script to create the Jira Issues table in Airtable.

This script creates an Airtable table with the appropriate schema for Jira issues.
"""

import os
import sys
import logging
import yaml
import requests
from pyairtable import Api
from typing import Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_credentials() -> Dict[str, str]:
    """Load credentials from creds.yaml or environment variables."""
    creds = {}
    
    # Try to load from creds.yaml
    if os.path.exists('creds.yaml'):
        try:
            with open('creds.yaml', 'r') as f:
                content = f.read()
                
            # Try YAML format first
            if ':' in content and not content.strip().startswith('#'):
                try:
                    yaml_creds = yaml.safe_load(content)
                    if yaml_creds:
                        creds.update(yaml_creds)
                except yaml.YAMLError:
                    pass
            
            # Try key=value format
            if not creds:
                for line in content.splitlines():
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        creds[key.strip()] = value.strip().strip('"').strip("'")
        except Exception as e:
            logger.warning(f"Could not read creds.yaml: {e}")
    
    # Environment variables override file
    env_keys = ['AIRTABLE_PAT', 'AIRTABLE_BASE_ID', 'AIRTABLE_TABLE_NAME']
    for key in env_keys:
        if key in os.environ:
            creds[key.lower()] = os.environ[key]
    
    return creds


def check_table_exists(api: Api, base_id: str, table_name: str) -> bool:
    """Check if a table exists in the Airtable base using Metadata API."""
    try:
        url = f'https://api.airtable.com/v0/meta/bases/{base_id}/tables'
        headers = {'Authorization': f'Bearer {api.api_key}'}
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            tables = response.json().get('tables', [])
            # Check for exact name match
            for table in tables:
                if table['name'] == table_name:
                    return True
            return False
        else:
            logger.error(f"Error checking tables: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Error checking if table exists: {e}")
        return False


def create_jira_table(base_id: str, table_name: str, airtable_pat: str) -> bool:
    """
    Create Jira Issues table in Airtable using Metadata API.
    
    Args:
        base_id: Airtable base ID
        table_name: Name for the new table
        airtable_pat: Airtable Personal Access Token
    
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Creating table '{table_name}' in Airtable...")
    
    url = f'https://api.airtable.com/v0/meta/bases/{base_id}/tables'
    headers = {
        'Authorization': f'Bearer {airtable_pat}',
        'Content-Type': 'application/json'
    }
    
    # Define table schema
    table_schema = {
        'name': table_name,
        'description': 'Jira issues synced from Jira',
        'fields': [
            {
                'name': 'key',
                'type': 'singleLineText',
                'description': 'Jira issue key (e.g., PROJ-123)'
            },
            {
                'name': 'summary',
                'type': 'singleLineText',
                'description': 'Issue summary/title'
            },
            {
                'name': 'url',
                'type': 'url',
                'description': 'Link to the Jira issue'
            },
            {
                'name': 'type',
                'type': 'singleLineText',
                'description': 'Issue type (e.g., Bug, Story, Task)'
            },
            {
                'name': 'status',
                'type': 'singleLineText',
                'description': 'Current status (e.g., To Do, In Progress, Done)'
            },
            {
                'name': 'priority',
                'type': 'singleLineText',
                'description': 'Issue priority'
            },
            {
                'name': 'assignee',
                'type': 'singleLineText',
                'description': 'Assigned user'
            },
            {
                'name': 'created',
                'type': 'dateTime',
                'description': 'Creation date',
                'options': {
                    'timeZone': 'utc',
                    'dateFormat': {
                        'name': 'iso'
                    },
                    'timeFormat': {
                        'name': '24hour'
                    }
                }
            },
            {
                'name': 'updated',
                'type': 'dateTime',
                'description': 'Last updated date',
                'options': {
                    'timeZone': 'utc',
                    'dateFormat': {
                        'name': 'iso'
                    },
                    'timeFormat': {
                        'name': '24hour'
                    }
                }
            }
        ]
    }
    
    try:
        response = requests.post(url, headers=headers, json=table_schema)
        
        if response.status_code == 200:
            logger.info(f"Successfully created table '{table_name}'")
            return True
        else:
            logger.error(f"Could not create table via API (status {response.status_code}): {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False


def main():
    """Main function."""
    # Load credentials
    try:
        creds = load_credentials()
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        sys.exit(1)
    
    # Get required environment variables (try both cases)
    AIRTABLE_BASE_ID = os.environ.get('AIRTABLE_BASE_ID', creds.get('airtable_base_id') or creds.get('AIRTABLE_BASE_ID'))
    AIRTABLE_TABLE_NAME = os.environ.get('AIRTABLE_TABLE_NAME', creds.get('airtable_table_name') or creds.get('AIRTABLE_TABLE_NAME') or 'Jira Issues')
    AIRTABLE_PAT = creds.get('airtable_pat') or creds.get('AIRTABLE_PAT')
    
    # Validate required variables
    if not AIRTABLE_BASE_ID:
        logger.error("AIRTABLE_BASE_ID environment variable is required")
        sys.exit(1)
    if not AIRTABLE_TABLE_NAME:
        logger.error("AIRTABLE_TABLE_NAME environment variable is required")
        sys.exit(1)
    if not AIRTABLE_PAT:
        logger.error("AIRTABLE_PAT not found in credentials file or environment")
        sys.exit(1)
    
    # Connect to Airtable
    try:
        api = Api(AIRTABLE_PAT)
        logger.info("Successfully connected to Airtable")
    except Exception as e:
        logger.error(f"Failed to connect to Airtable: {e}")
        sys.exit(1)
    
    # Check if table already exists
    if check_table_exists(api, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME):
        logger.info(f"Table '{AIRTABLE_TABLE_NAME}' already exists. Exiting.")
        sys.exit(0)
    
    # Create the table
    if create_jira_table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, AIRTABLE_PAT):
        logger.info("Setup completed successfully!")
    else:
        logger.error("Failed to create table")
        sys.exit(1)


if __name__ == "__main__":
    main()

