"""
Utility for rebuilding the Virtuoso full-text index.

This module provides functionality to rebuild the Virtuoso RDF Quad store's
full-text index, which is used for optimal querying of RDF object values
using the bif:contains function in SPARQL queries.
"""
import argparse
import sys
from typing import Tuple

DOCKER_PATH = "docker"
DOCKER_ISQL_PATH = "/usr/local/virtuoso-opensource/bin/isql"
ISQL_PATH = "isql"

from virtuoso_utilities.isql_helpers import run_isql_command


def drop_fulltext_tables(args: argparse.Namespace) -> Tuple[bool, str, str]:
    """
    Drop the full-text index tables.
    
    Args:
        args: Command-line arguments containing connection details
        
    Returns:
        Tuple of (success, stdout, stderr)
    """
    sql_command = """
    drop table DB.DBA.VTLOG_DB_DBA_RDF_OBJ;
    drop table DB.DBA.RDF_OBJ_RO_FLAGS_WORDS;
    """
    print("Dropping existing full-text index tables...", file=sys.stderr)
    return run_isql_command(args, sql_command=sql_command, capture=True)


def recreate_fulltext_index(args: argparse.Namespace) -> Tuple[bool, str, str]:
    """
    Recreate the full-text index.
    
    Args:
        args: Command-line arguments containing connection details
        
    Returns:
        Tuple of (success, stdout, stderr)
    """
    sql_command = """
    DB.DBA.vt_create_text_index (
      fix_identifier_case ('DB.DBA.RDF_OBJ'),
      fix_identifier_case ('RO_FLAGS'),
      fix_identifier_case ('RO_ID'),
      0, 0, vector (), 1, '*ini*', 'UTF-8-QR');
    
    DB.DBA.vt_batch_update (fix_identifier_case ('DB.DBA.RDF_OBJ'), 'ON', 1);
    """
    print("Recreating full-text index...", file=sys.stderr)
    return run_isql_command(args, sql_command=sql_command, capture=True)


def refill_fulltext_index(args: argparse.Namespace) -> Tuple[bool, str, str]:
    """
    Refill the full-text index.
    
    Args:
        args: Command-line arguments containing connection details
        
    Returns:
        Tuple of (success, stdout, stderr)
    """
    sql_command = "DB.DBA.RDF_OBJ_FT_RECOVER();"
    print("Refilling full-text index (this may take a while)...", file=sys.stderr)
    return run_isql_command(args, sql_command=sql_command, capture=True)


def rebuild_fulltext_index(args: argparse.Namespace) -> bool:
    """
    Complete process to rebuild the Virtuoso full-text index.
    
    This function will:
    1. Drop existing full-text index tables
    2. Recreate the index
    3. Refill the index
    
    After this process completes, the Virtuoso database MUST be restarted
    for the index rebuild to take effect.
    
    Args:
        args: Command-line arguments containing connection details
        
    Returns:
        True if the rebuild process completed successfully, False otherwise
    """
    success, stdout, stderr = drop_fulltext_tables(args)
    if not success:
        print(f"Error dropping full-text index tables: {stderr}", file=sys.stderr)
        return False
    
    success, stdout, stderr = recreate_fulltext_index(args)
    if not success:
        print(f"Error recreating full-text index: {stderr}", file=sys.stderr)
        return False
    
    success, stdout, stderr = refill_fulltext_index(args)
    if not success:
        print(f"Error refilling full-text index: {stderr}", file=sys.stderr)
        return False
    
    print("Full-text index rebuild completed successfully.", file=sys.stderr)
    print("IMPORTANT: The Virtuoso database MUST be restarted for the index rebuild to take effect.", file=sys.stderr)
    return True


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Rebuild the Virtuoso full-text index."
    )
    
    parser.add_argument("--host", default="localhost", help="Virtuoso host")
    parser.add_argument("--port", default="1111", help="Virtuoso port")
    parser.add_argument("--user", default="dba", help="Virtuoso username")
    parser.add_argument("--password", default="dba", help="Virtuoso password")
    parser.add_argument(
        "--docker-container", 
        help="Docker container name/ID to execute isql inside"
    )
    
    args = parser.parse_args()
    
    args.docker_path = DOCKER_PATH
    args.docker_isql_path = DOCKER_ISQL_PATH
    args.isql_path = ISQL_PATH
    
    return args


def main() -> int:
    """Main entry point."""
    args = parse_args()
    success = rebuild_fulltext_index(args)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())