#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Performs sequential bulk loading of RDF N-Quads Gzipped files (`.nq.gz`)
into OpenLink Virtuoso using the official `ld_dir`/`ld_dir_all` and
`rdf_loader_run` method.

Registers files matching *.nq.gz using ld_dir/ld_dir_all into DB.DBA.load_list,
then runs a single `rdf_loader_run()` process to load the registered files.

IMPORTANT:
- Only files with the extension `.nq.gz` will be processed.
- The data directory specified ('-d' or '--data-directory') MUST be
  accessible by the Virtuoso server process itself.
- This directory path MUST be listed in the 'DirsAllowed' parameter
  within the Virtuoso INI file (e.g., virtuoso.ini).
- When using Docker, data-directory is the path INSIDE the container.
  Files will be accessed and loaded from within the container.

Reference:
- https://vos.openlinksw.com/owiki/wiki/VOS/VirtBulkRDFLoader
"""

import argparse
import glob
import os
import subprocess
import sys
import time

from virtuoso_utilities.isql_helpers import run_isql_command

DEFAULT_VIRTUOSO_HOST = "localhost"
DEFAULT_VIRTUOSO_PORT = 1111
DEFAULT_VIRTUOSO_USER = "dba"
# --- File pattern is now fixed ---
NQ_GZ_PATTERN = '*.nq.gz'
DEFAULT_ISQL_PATH_HOST = "isql"
DEFAULT_ISQL_PATH_DOCKER = "isql"
DEFAULT_DOCKER_PATH = "docker"
DEFAULT_CHECKPOINT_INTERVAL = 60
DEFAULT_SCHEDULER_INTERVAL = 10
DEFAULT_PLACEHOLDER_GRAPH = "http://localhost:8890/DAV/ignored"


def find_nquads_files_local(directory, recursive=False):
    """
    Find all N-Quads Gzipped files (`*.nq.gz`) in a directory on local filesystem.
    Returns a list of file paths.
    """
    pattern = NQ_GZ_PATTERN # Use the fixed pattern
    if recursive:
        matches = []
        for root, _, _ in os.walk(directory):
            path_pattern = os.path.join(root, pattern)
            matches.extend(glob.glob(path_pattern))
        return matches
    else:
        path_pattern = os.path.join(directory, pattern)
        return glob.glob(path_pattern)


def find_nquads_files_docker(container, directory, recursive, docker_path="docker"):
    """
    Find all N-Quads Gzipped files (`*.nq.gz`) in a directory inside a Docker container.
    Uses 'docker exec' to run find command inside the container.
    Returns a list of file paths.
    """
    pattern = NQ_GZ_PATTERN # Use the fixed pattern
    if recursive:
        cmd = f"{docker_path} exec {container} find {directory} -type f -name '{pattern}' -print"
    else:
        cmd = f"{docker_path} exec {container} find {directory} -maxdepth 1 -type f -name '{pattern}' -print"
    
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            check=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        
        files = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        return files
        
    except subprocess.CalledProcessError as e:
        print(f"Error finding files in Docker container: {e}", file=sys.stderr)
        print(f"Command: {cmd}", file=sys.stderr)
        print(f"Error output: {e.stderr}", file=sys.stderr)
        return []


def main():
    """
    Main function to parse arguments and orchestrate the sequential bulk loading
    of `.nq.gz` files using the official Virtuoso `ld_dir`/`ld_dir_all` and
    `rdf_loader_run` method.
    """
    parser = argparse.ArgumentParser(
        description=f"Sequential N-Quads Gzipped (`{NQ_GZ_PATTERN}`) bulk loader for OpenLink Virtuoso using ld_dir/rdf_loader_run.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example usage:
  # Load all *.nq.gz files from /data/rdf (local mode)
  python bulk_load_parallel.py -d /data/rdf -k mypassword

  # Load *.nq.gz recursively using Docker, files at /database/data in container
  python bulk_load_parallel.py -d /database/data -k mypassword --recursive \
    --docker-container virtuoso_container \
    --docker-isql-path /opt/virtuoso/bin/isql

IMPORTANT:
- Only files with the extension `.nq.gz` will be loaded.
- The data directory (-d) must be accessible by the Virtuoso process
  and listed in the 'DirsAllowed' setting in virtuoso.ini.
- When using Docker mode, data-directory is the path INSIDE the container.
  Files are accessed and loaded directly inside the container.
"""
    )

    parser.add_argument("-d", "--data-directory", required=True,
                        help="Path to the N-Quads Gzipped (`.nq.gz`) files. When using Docker, this must be the path INSIDE the container.")
    parser.add_argument("-H", "--host", default=DEFAULT_VIRTUOSO_HOST,
                        help=f"Virtuoso server host (Default: {DEFAULT_VIRTUOSO_HOST}).")
    parser.add_argument("-P", "--port", type=int, default=DEFAULT_VIRTUOSO_PORT,
                        help=f"Virtuoso server port (Default: {DEFAULT_VIRTUOSO_PORT}).")
    parser.add_argument("-u", "--user", default=DEFAULT_VIRTUOSO_USER,
                        help=f"Virtuoso username (Default: {DEFAULT_VIRTUOSO_USER}).")
    parser.add_argument("-k", "--password", required=True,
                        help="Virtuoso password.")
    parser.add_argument("--recursive", action='store_true',
                        help="Load files recursively from subdirectories (uses ld_dir_all).")
    parser.add_argument("--isql-path", default=DEFAULT_ISQL_PATH_HOST,
                        help=f"Path to the Virtuoso 'isql' executable on the HOST system (Default: '{DEFAULT_ISQL_PATH_HOST}'). Used only if not in Docker mode.")
    parser.add_argument("--checkpoint-interval", type=int, default=DEFAULT_CHECKPOINT_INTERVAL,
                         help=f"Interval (seconds) to set for checkpointing after load (Default: {DEFAULT_CHECKPOINT_INTERVAL}).")
    parser.add_argument("--scheduler-interval", type=int, default=DEFAULT_SCHEDULER_INTERVAL,
                         help=f"Interval (seconds) to set for the scheduler after load (Default: {DEFAULT_SCHEDULER_INTERVAL}).")

    docker_group = parser.add_argument_group('Docker Options')
    docker_group.add_argument("--docker-container",
                        help="Name or ID of the running Virtuoso Docker container. If provided, 'isql' will be run via 'docker exec'.")
    docker_group.add_argument("--docker-isql-path", default=DEFAULT_ISQL_PATH_DOCKER,
                        help=f"Path to the 'isql' executable INSIDE the Docker container (Default: '{DEFAULT_ISQL_PATH_DOCKER}').")
    docker_group.add_argument("--docker-path", default=DEFAULT_DOCKER_PATH,
                        help=f"Path to the 'docker' executable on the HOST system (Default: '{DEFAULT_DOCKER_PATH}').")

    args = parser.parse_args()

    data_dir = args.data_directory

    if not args.docker_container and not os.path.isabs(data_dir):
        data_dir = os.path.abspath(data_dir)
        print(f"Converted data directory to absolute path: {data_dir}")

    if args.docker_container:
        print(f"Info: Using Docker container '{args.docker_container}'")
        print(f"      Files will be accessed at '{data_dir}' inside the container.")
        print(f"      Ensure this path is correctly mounted and listed in container's DirsAllowed.")
    else:
        print(f"Info: Running locally. Files will be accessed at: {data_dir}")
        print(f"      Ensure this path is accessible by the Virtuoso process and listed in DirsAllowed.")

    print("-" * 40)
    print("Configuration:")
    print(f"  Host: {args.host}:{args.port}")
    print(f"  User: {args.user}")
    print(f"  Mode: {'Docker' if args.docker_container else 'Local'}")
    print(f"  Data Dir: {data_dir}")
    print(f"  File Pattern: {NQ_GZ_PATTERN}")
    print(f"  Recursive: {args.recursive}")
    print("-" * 40)

    print(f"Step 1: Finding '{NQ_GZ_PATTERN}' files in {data_dir}...")

    if args.docker_container:
        print(f"Searching for files inside Docker container '{args.docker_container}'...")
        files = find_nquads_files_docker(
            container=args.docker_container,
            directory=data_dir,
            recursive=args.recursive,
            docker_path=args.docker_path
        )
    else:
        print(f"Searching for files on local filesystem...")
        files = find_nquads_files_local(data_dir, args.recursive) 

    if not files:
        print(f"Info: No files matching '{NQ_GZ_PATTERN}' found in '{data_dir}'. Nothing to load.", file=sys.stderr)
        sys.exit(0)

    print(f"Found {len(files)} files matching pattern.")

    print(f"Step 2: Validating Virtuoso access to directory/files...")

    test_file = files[0]
    test_file_sql_escaped = test_file.replace("'", "''")
    test_sql = f"SELECT file_stat('{test_file_sql_escaped}');"

    print(f"Testing Virtuoso access with file: {test_file}")
    success, stdout, stderr = run_isql_command(args, sql_command=test_sql, capture=True)
    if not success or "Security violation" in stderr or "cannot" in stderr:
        print(f"Error: Virtuoso cannot access the data files.", file=sys.stderr)
        print(f"  Test file: {test_file}", file=sys.stderr)
        print(f"  Ensure the path '{data_dir}' is in Virtuoso's DirsAllowed configuration.", file=sys.stderr)
        print(f"  Error: {stderr}", file=sys.stderr)
        sys.exit(1)

    print(f"Virtuoso can access the data files.")

    print(f"Step 3: Registering files in directory '{data_dir}' with Virtuoso...")

    if args.recursive:
        ld_function = "ld_dir_all"
        print(f"Using {ld_function} for recursive loading.")
    else:
        ld_function = "ld_dir"
        print(f"Using {ld_function} for non-recursive loading.")

    data_dir_sql_escaped = data_dir.replace("'", "''")
    file_pattern_sql_escaped = NQ_GZ_PATTERN.replace("'", "''")
    placeholder_graph_sql_escaped = DEFAULT_PLACEHOLDER_GRAPH.replace("'", "''")

    register_sql = f"{ld_function}('{data_dir_sql_escaped}', '{file_pattern_sql_escaped}', '{placeholder_graph_sql_escaped}');"
    print(f"Executing: {register_sql}")
    success_reg, stdout_reg, stderr_reg = run_isql_command(args, sql_command=register_sql)

    if not success_reg:
        print(f"Error: Failed to register files using {ld_function}.", file=sys.stderr)
        print(f"  Directory: {data_dir}", file=sys.stderr)
        print(f"  Pattern: {NQ_GZ_PATTERN}", file=sys.stderr)
        print(f"  Error: {stderr_reg}", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"Successfully registered files using {ld_function} (check DB.DBA.load_list).")
        if stdout_reg and stdout_reg.strip():
             print(f"  Output: {stdout_reg.strip()}")

    print(f"\nStep 4: Starting sequential rdf_loader_run() process...")
    print(f"Load progress can be monitored by querying 'SELECT ll_state, count(*) FROM DB.DBA.load_list GROUP BY ll_state;' in isql.")
    start_load_time = time.time()

    loader_sql = "rdf_loader_run();"
    print(f"Executing: {loader_sql}")
    success_load, stdout_load, stderr_load = run_isql_command(args, sql_command=loader_sql)

    load_duration = time.time() - start_load_time

    if success_load:
        print(f"rdf_loader_run() completed in {load_duration:.2f} seconds.")
        if stdout_load and stdout_load.strip():
            print(f"  Output: {stdout_load.strip()}")
    else:
        print(f"Error: rdf_loader_run() failed after {load_duration:.2f} seconds.", file=sys.stderr)
        print(f"  Error: {stderr_load}", file=sys.stderr)
        print("WARNING: rdf_loader_run() reported an error. Check load status carefully.", file=sys.stderr)

    print("\nStep 5: Checking load status from DB.DBA.load_list...")

    check_sql = "SELECT ll_file, ll_graph, ll_state, ll_error FROM DB.DBA.load_list WHERE ll_error IS NOT NULL OR ll_state <> 2;"
    print(f"Executing check: {check_sql}")
    success_check, stdout_check, stderr_check = run_isql_command(args, sql_command=check_sql, capture=True)

    failed_files_details = []
    if not success_check:
        print("Error: Failed to query DB.DBA.load_list for status.", file=sys.stderr)
        print(f"  Error: {stderr_check}", file=sys.stderr)
        print("WARNING: Cannot confirm load success. Proceeding with cleanup and checkpoint.", file=sys.stderr)
    else:
        lines = stdout_check.strip().splitlines()
        if len(lines) > 4 and not lines[-1].endswith("Rows."):
             header = lines[3]
             data_lines = lines[4:-1]
             print(f"Found potential issues in DB.DBA.load_list:")
             for line in data_lines:
                 parts = line.split()
                 if len(parts) >= 4:
                     file = parts[0]
                     graph = parts[1]
                     state = parts[2]
                     error = " ".join(parts[3:])
                     failed_files_details.append({'file': file, 'graph': graph, 'state': state, 'error': error})
                     print(f"  - File: {file}, State: {state}, Error: {error}")
        elif "0 Rows." not in stdout_check:
             print(f"Warning: Unexpected output when checking load_list. Output:\n{stdout_check}", file=sys.stderr)

    print("\nStep 6: Restoring default settings and running final checkpoint...")

    cleanup_sql = f"log_enable(3, 1); checkpoint; checkpoint_interval({args.checkpoint_interval}); scheduler_interval({args.scheduler_interval});"
    print(f"Executing: {cleanup_sql}")
    success_final, _, stderr_final = run_isql_command(args, sql_command=cleanup_sql)
    if not success_final:
        print("Error: Failed to run final checkpoint and restore settings.", file=sys.stderr)
        print(f"Error details: {stderr_final}", file=sys.stderr)
        print("CRITICAL WARNING: Checkpoint failed after bulk load. LOADED DATA MAY BE LOST or transactions left open.", file=sys.stderr)
        print("Manually run 'checkpoint;' in isql immediately and verify data integrity.", file=sys.stderr)
        sys.exit(1)

    print("Checkpoint successful and settings restored.")
    print("-" * 40)

    print("Bulk Load Summary:")
    num_failed = len(failed_files_details)
    print(f"- Files matching '{NQ_GZ_PATTERN}' registered and rdf_loader_run() executed in {load_duration:.2f} seconds.")
    if num_failed > 0:
        print(f"- Found {num_failed} potential errors/incomplete loads reported in DB.DBA.load_list.")
    elif not success_load:
        print(f"- rdf_loader_run() itself failed, but no specific file errors found in DB.DBA.load_list check.")
    else:
        print("- No errors found in DB.DBA.load_list check and rdf_loader_run() succeeded.")

    if failed_files_details:
        print("\nErrors/Issues reported in DB.DBA.load_list:")
        for detail in failed_files_details[:20]:
            print(f"  - File: {detail['file']}, State: {detail['state']}, Error: {detail['error']}")
        if len(failed_files_details) > 20:
            print(f"  (and {len(failed_files_details) - 20} more...)")

    print("-" * 40)
    if failed_files_details:
        print(f"Sequential bulk load finished with potential errors reported in DB.DBA.load_list. Only '{NQ_GZ_PATTERN}' files were considered.", file=sys.stderr)
        sys.exit(1)
    elif not success_load:
        print(f"Sequential bulk load finished, but rdf_loader_run() reported an error. Only '{NQ_GZ_PATTERN}' files were considered.", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"Bulk load finished for '{NQ_GZ_PATTERN}' files. Checkpoint complete.")
        sys.exit(0)


if __name__ == "__main__":
    main() 