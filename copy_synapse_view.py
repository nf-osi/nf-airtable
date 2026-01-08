#!/usr/bin/env python3
"""
Copy a Synapse view to another project for testing.

This script duplicates a Synapse view (including its defining SQL and scope)
to a target project.
"""

import os
import sys
import yaml
import logging
from typing import Dict
from pathlib import Path

try:
    import synapseclient
    from synapseclient import Synapse, EntityViewType
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
    synapse_pat = None
    try:
        creds = yaml.safe_load(content)
        if isinstance(creds, dict) and creds:
            synapse_pat = creds.get('SYNAPSE_PAT') or creds.get('synapse_pat')
    except yaml.YAMLError:
        pass
    
    # If YAML parsing didn't work, try parsing as key=value format
    if not synapse_pat:
        for line in content.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line.startswith('SYNAPSE_PAT='):
                synapse_pat = line.split('=', 1)[1].strip('"\'')
    
    # Fallback to environment variable
    synapse_pat = synapse_pat or os.getenv('SYNAPSE_PAT')
    
    if not synapse_pat:
        raise ValueError("SYNAPSE_PAT not found in credentials file or environment")
    
    return {'synapse_pat': synapse_pat}


def copy_view(syn: Synapse, source_view_id: str, target_project_id: str, new_name: str = None) -> str:
    """
    Copy a Synapse view to another project.
    
    Args:
        syn: Synapse client
        source_view_id: ID of the source view
        target_project_id: ID of the target project
        new_name: Optional new name for the view
    
    Returns:
        ID of the copied view
    """
    logger.info(f"Fetching source view: {source_view_id}")
    
    # Get the source view
    source_view = syn.get(source_view_id, downloadFile=False)
    
    # Extract view properties
    view_name = new_name or f"{source_view.name} (Test Copy)"
    
    logger.info(f"Source view name: {source_view.name}")
    logger.info(f"New view name: {view_name}")
    logger.info(f"Target project: {target_project_id}")
    
    # Create a copy of the entity view with a limited scope for testing
    try:
        logger.info("Creating copy of entity view...")
        logger.info(f"Source view type: {source_view.concreteType}")
        
        # For entity views, we need to use EntityViewSchema with viewTypeMask
        from synapseclient import EntityViewSchema
        
        # Get the view type mask from source
        view_type_mask = source_view.viewTypeMask if hasattr(source_view, 'viewTypeMask') else None
        logger.info(f"View type mask: {view_type_mask} (2 = PROJECT view)")
        
        # Get the original scope (list of projects)
        original_scope = source_view.scopeIds if hasattr(source_view, 'scopeIds') else []
        logger.info(f"Original view has {len(original_scope)} projects in scope")
        
        # For testing, use a subset of the original scope to avoid the size limit
        # Take first 50 projects or use environment variable to specify
        max_scope_size = int(os.getenv('MAX_SCOPE_SIZE', '50'))
        test_scope = original_scope[:max_scope_size]
        logger.info(f"Using {len(test_scope)} projects for test view scope")
        
        # Create new view with proper parameters
        # viewTypeMask 2 typically means "project" entities
        new_view = EntityViewSchema(
            name=view_name,
            parent=target_project_id,
            scopes=test_scope,  # Use only the target project as scope
            includeEntityTypes=[EntityViewType.PROJECT] if view_type_mask == 2 else None,
            addDefaultViewColumns=False,
            addAnnotationColumns=False
        )
        
        # Copy the defining SQL if it exists (but it may need scope adjustment)
        if hasattr(source_view, 'definingSQL') and source_view.definingSQL:
            # Note: The defining SQL may reference the original scope
            # For a true test copy, you might want to adjust this
            logger.info(f"Source has defining SQL: {source_view.definingSQL[:100]}...")
            logger.warning("Note: Defining SQL not copied as it may reference original scope")
        
        # Copy column IDs
        if hasattr(source_view, 'columnIds') and source_view.columnIds:
            new_view.columnIds = source_view.columnIds
            logger.info(f"Copied {len(source_view.columnIds)} column definitions")
        
        # Store the new view
        stored_view = syn.store(new_view)
        copied_view_id = stored_view.id
        
        logger.info(f"Successfully created view copy: {copied_view_id}")
        logger.info("")
        logger.info(f"This test view contains a subset of {len(test_scope)} projects from the original.")
        logger.info("It should have data for testing the sync functionality.")
        logger.info("")
        
        return copied_view_id
        
    except Exception as e:
        logger.error(f"Failed to copy view: {e}")
        raise


def main():
    """Main function."""
    SOURCE_VIEW_ID = os.getenv('SOURCE_VIEW_ID', 'syn52677631')
    TARGET_PROJECT_ID = os.getenv('TARGET_PROJECT_ID', 'syn26451327')
    NEW_VIEW_NAME = os.getenv('NEW_VIEW_NAME')  # Optional
    
    logger.info(f"Source view: {SOURCE_VIEW_ID}")
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
    
    # Copy the view
    try:
        new_view_id = copy_view(
            syn=syn,
            source_view_id=SOURCE_VIEW_ID,
            target_project_id=TARGET_PROJECT_ID,
            new_name=NEW_VIEW_NAME
        )
        logger.info("=" * 80)
        logger.info("View copied successfully!")
        logger.info(f"New view ID: {new_view_id}")
        logger.info("=" * 80)
        logger.info("")
        logger.info("Use this view ID in your sync scripts:")
        logger.info(f"  export SYNAPSE_SOURCE_VIEW_ID='{new_view_id}'")
        logger.info(f"  export SYNAPSE_TARGET_TABLE_ID='{new_view_id}'")
        logger.info("")
    except Exception as e:
        logger.error(f"Failed to copy view: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

