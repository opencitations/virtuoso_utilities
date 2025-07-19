#!/usr/bin/env python3
"""
Utility for rebuilding the Virtuoso full-text index.

This module provides functionality to rebuild the Virtuoso RDF Quad store's
full-text index, which is used for optimal querying of RDF object values
using the bif:contains function in SPARQL queries.
"""
import argparse
import subprocess
import sys
import shutil
import time
from typing import Tuple

from virtuoso_utilities.isql_helpers import run_isql_command




def drop_fulltext_tables(args: argparse.Namespace) -> Tuple[bool, str, str]:
    """
    Drop the full-text index tables.
    
    Args:
        args: Command-line arguments containing connection details
        
    Returns:
        Tuple of (success, stdout, stderr)
    """
    sql_commands = [
        "drop table DB.DBA.VTLOG_DB_DBA_RDF_OBJ;",
        "drop table DB.DBA.RDF_OBJ_RO_FLAGS_WORDS;"
    ]
    
    print("Dropping existing full-text index tables...", file=sys.stderr)
    
    for sql_command in sql_commands:
        success, stdout, stderr = run_isql_command(args, sql_command=sql_command, capture=True, ignore_errors=True)
        
        # Don't fail if tables don't exist - this is expected on first run
        if not success and "does not exist" not in stderr.lower():
            return False, stdout, stderr
    
    return True, "", ""


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
    """
    
    print("Recreating full-text index...", file=sys.stderr)
    
    return run_isql_command(args, sql_command=sql_command, capture=True)


def enable_batch_update(args: argparse.Namespace) -> Tuple[bool, str, str]:
    """
    Enable batch update for the full-text index.
    
    Args:
        args: Command-line arguments containing connection details
        
    Returns:
        Tuple of (success, stdout, stderr)
    """
    sql_command = "DB.DBA.vt_batch_update (fix_identifier_case ('DB.DBA.RDF_OBJ'), 'ON', 1);"
    
    print("Enabling batch update for full-text index...", file=sys.stderr)
    
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
    3. Enable batch update
    4. Refill the index
    
    After this process completes, the Virtuoso database should be restarted
    for optimal text index performance.
    
    Args:
        args: Command-line arguments containing connection details
        
    Returns:
        True if the rebuild process completed successfully, False otherwise
    """
    # Check prerequisites
    if args.docker_container:
        if not shutil.which('docker'):
            print("Error: Docker command not found in PATH", file=sys.stderr)
            return False
        
        # Check if container is running
        try:
            result = subprocess.run(
                ['docker', 'ps', '--format', '{{.Names}}'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if args.docker_container not in result.stdout:
                print(f"Error: Container '{args.docker_container}' not found or not running", file=sys.stderr)
                return False
                
        except Exception as e:
            print(f"Error checking container status: {e}", file=sys.stderr)
            return False
    else:
        if not shutil.which('isql'):
            print("Error: isql command not found in PATH", file=sys.stderr)
            return False
    
    # Step 1: Drop existing tables
    success, _, stderr = drop_fulltext_tables(args)
    if not success:
        print(f"Error dropping full-text index tables: {stderr}", file=sys.stderr)
        return False
    
    # Step 2: Recreate index
    success, _, stderr = recreate_fulltext_index(args)
    if not success:
        print(f"Error recreating full-text index: {stderr}", file=sys.stderr)
        return False
    
    # Step 3: Enable batch update
    success, _, stderr = enable_batch_update(args)
    if not success:
        print(f"Error enabling batch update: {stderr}", file=sys.stderr)
        return False
    
    # Step 4: Refill index
    success, _, stderr = refill_fulltext_index(args)
    if not success:
        print(f"Error refilling full-text index: {stderr}", file=sys.stderr)
        return False
    
    print("Full-text index rebuild completed successfully.", file=sys.stderr)
    
    # Restart container if requested and using Docker
    if args.restart_container and args.docker_container:
        print("Restarting Docker container to activate text index...", file=sys.stderr)
        
        try:
            # Restart the container
            result = subprocess.run(
                ['docker', 'restart', args.docker_container],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0:
                print(f"Error restarting container: {result.stderr}", file=sys.stderr)
                print("Note: Manual restart recommended for full text index activation.", file=sys.stderr)
                return True
            
            print("Waiting for container to be ready after restart...", file=sys.stderr)
            
            # Wait for container to be ready
            max_wait = 60
            wait_time = 0
            while wait_time < max_wait:
                try:
                    success, _, _ = run_isql_command(args, sql_command="status();", capture=True, ignore_errors=True)
                    
                    if success:
                        print("Container restarted and ready!", file=sys.stderr)
                        return True
                        
                except subprocess.TimeoutExpired:
                    pass
                
                time.sleep(2)
                wait_time += 2
            
            print("Container restarted but may still be initializing.", file=sys.stderr)
            
        except Exception as e:
            print(f"Error during container restart: {e}", file=sys.stderr)
            print("Note: Manual restart recommended for full text index activation.", file=sys.stderr)
    
    elif args.restart_container and not args.docker_container:
        print("Note: --restart-container requires --docker-container to be specified.", file=sys.stderr)
        print("Note: Restart the Virtuoso database manually for optimal text index performance.", file=sys.stderr)
    
    else:
        print("Note: Restart the Virtuoso database for optimal text index performance.", file=sys.stderr)
    
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
    parser.add_argument(
        "--restart-container",
        action="store_true",
        help="Restart the Docker container after rebuilding the index (recommended for full activation)"
    )
    
    # Add arguments required by isql_helpers
    parser.add_argument("--docker-path", default="docker", help="Path to docker executable")
    parser.add_argument("--docker-isql-path", default="isql", help="Path to isql inside container")
    parser.add_argument("--isql-path", default="isql", help="Path to isql executable")
    
    return parser.parse_args()


def main() -> int:
    """Main entry point."""
    args = parse_args()
    success = rebuild_fulltext_index(args)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())