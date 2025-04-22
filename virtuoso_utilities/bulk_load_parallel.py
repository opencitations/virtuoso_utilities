#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Performs parallel bulk loading of RDF files into OpenLink Virtuoso.

Uses the Virtuoso bulk loading procedure (ld_dir/ld_dir_all, rdf_loader_run)
to register files from a specified directory and load them in parallel
using multiple 'isql' command-line utility instances.

IMPORTANT:
- The data directory specified ('-d' or '--data-directory') MUST be
  accessible by the Virtuoso server process itself.
- This directory path MUST be listed in the 'DirsAllowed' parameter
  within the Virtuoso INI file (e.g., virtuoso.ini).
- If using Docker, the '--docker-data-mount-path' MUST be the path
  INSIDE the container where the host's data directory is mounted,
  and this container path must be in the container's 'DirsAllowed'.
"""

import argparse
import os
import subprocess
import sys
import time
from multiprocessing import cpu_count

DEFAULT_VIRTUOSO_HOST = "localhost"
DEFAULT_VIRTUOSO_PORT = 1111
DEFAULT_VIRTUOSO_USER = "dba"
DEFAULT_NUM_PROCESSES = max(1, int(cpu_count() / 2.5))
DEFAULT_GRAPH_URI = "http://localhost:8890/DAV" # Default graph if .graph files are not used
DEFAULT_FILE_PATTERN = '*.*' # Default pattern for ld_dir/ld_dir_all
DEFAULT_ISQL_PATH_HOST = "isql"
DEFAULT_ISQL_PATH_DOCKER = "isql" # Often '/opt/virtuoso-opensource/bin/isql' in containers
DEFAULT_DOCKER_PATH = "docker"
DEFAULT_CHECKPOINT_INTERVAL = 60 # Default Virtuoso checkpoint interval (seconds)
DEFAULT_SCHEDULER_INTERVAL = 10 # Default Virtuoso scheduler interval (seconds)

def run_isql_command(
    sql_command: str,
    args: argparse.Namespace,
    capture: bool = False,
    ignore_errors: bool = False # Add option to ignore errors for non-critical steps
) -> tuple[bool, str, str]:
    """
    Executes a SQL command using the 'isql' utility, either directly
    or via 'docker exec'.

    Args:
        sql_command (str): The SQL command or procedure call to execute.
        args (argparse.Namespace): Parsed command-line arguments containing
                                   connection details and paths.
        capture (bool): If True, capture stdout and stderr.
        ignore_errors (bool): If True, print errors but return True anyway.

    Returns:
        tuple: (success_status, stdout, stderr)
               success_status is True if the command ran without error (exit code 0)
               or if ignore_errors is True.
               stdout and stderr contain the respective outputs.
    """
    base_command = []
    effective_isql_path_for_error = ""

    if args.docker_container:
        base_command = [
            args.docker_path,
            'exec',
            args.docker_container,
            args.docker_isql_path,
            f"{args.host}:{args.port}",
            args.user,
            args.password,
            f"exec={sql_command}"
        ]
        effective_isql_path_for_error = f"'{args.docker_isql_path}' inside container '{args.docker_container}'"
    else:
        base_command = [
            args.isql_path,
            f"{args.host}:{args.port}",
            args.user,
            args.password,
            f"exec={sql_command}"
        ]
        effective_isql_path_for_error = f"'{args.isql_path}' on host"

    try:
        process = subprocess.run(
            base_command,
            capture_output=capture,
            text=True,
            check=False,
            encoding='utf-8'
        )
        stdout = process.stdout.strip() if process.stdout else ""
        stderr = process.stderr.strip() if process.stderr else ""

        if process.returncode != 0:
            print(f"Error executing ISQL command.", file=sys.stderr)
            print(f"Command: {' '.join(base_command)}", file=sys.stderr)
            print(f"Return Code: {process.returncode}", file=sys.stderr)
            if stderr:
                print(f"Stderr: {stderr}", file=sys.stderr)
            if stdout:
                print(f"Stdout: {stdout}", file=sys.stderr)
            return ignore_errors, stdout, stderr # Return True if ignoring errors
        return True, stdout, stderr
    except FileNotFoundError:
        executable = args.docker_path if args.docker_container else args.isql_path
        print(f"Error: Command '{executable}' not found.", file=sys.stderr)
        if args.docker_container:
            print(f"Make sure '{args.docker_path}' is installed and in your PATH, and the container is running.", file=sys.stderr)
        else:
            print(f"Make sure Virtuoso client tools (containing {effective_isql_path_for_error}) are installed and in your PATH.", file=sys.stderr)
        return False, "", f"Executable not found: {executable}"
    except Exception as e:
        print(f"An unexpected error occurred running ISQL: {e}", file=sys.stderr)
        print(f"Command: {' '.join(base_command)}", file=sys.stderr)
        return False, "", str(e)


def main():
    """
    Main function to parse arguments and orchestrate the parallel bulk loading.
    """
    parser = argparse.ArgumentParser(
        description="Parallel RDF bulk loader for OpenLink Virtuoso using the official bulk load procedure.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example usage:
  # Load all *.ttl files from /data/rdf into graph <http://example.org/graph>
  python bulk_load_parallel.py -d /data/rdf -k mypassword -f '*.ttl' -g http://example.org/graph

  # Load recursively from /data/rdf using 4 loader processes, via Docker.
  # Note: The script will automatically clear the load list and suspend/rebuild
  # the RDF_OBJ full-text index during the process.
  python bulk_load_parallel.py -d /host/path/to/data \\
    -k mypassword --recursive -n 4 \\
    --docker-container virtuoso_container \\
    --docker-data-mount-path /container/data/mount \\
    --docker-isql-path /opt/virtuoso/bin/isql

IMPORTANT: The data directory (-d) must be accessible by the Virtuoso server process
and listed in the 'DirsAllowed' setting in virtuoso.ini.
For Docker, --docker-data-mount-path must be the path *inside* the container.
"""
    )

    # Core arguments
    parser.add_argument("-d", "--data-directory", required=True,
                        help="Directory containing the RDF files to load (MUST be accessible by Virtuoso server process and listed in DirsAllowed in virtuoso.ini).")
    parser.add_argument("-H", "--host", default=DEFAULT_VIRTUOSO_HOST,
                        help=f"Virtuoso server host (Default: {DEFAULT_VIRTUOSO_HOST}).")
    parser.add_argument("-P", "--port", type=int, default=DEFAULT_VIRTUOSO_PORT,
                        help=f"Virtuoso server port (Default: {DEFAULT_VIRTUOSO_PORT}).")
    parser.add_argument("-u", "--user", default=DEFAULT_VIRTUOSO_USER,
                        help=f"Virtuoso username (Default: {DEFAULT_VIRTUOSO_USER}).")
    parser.add_argument("-k", "--password", required=True,
                        help="Virtuoso password.")
    parser.add_argument("-n", "--num-processes", type=int, default=DEFAULT_NUM_PROCESSES,
                        help=f"Number of parallel rdf_loader_run() processes (Default: {DEFAULT_NUM_PROCESSES}, based on CPU cores / 2.5).")
    parser.add_argument("-f", "--file-pattern", default=DEFAULT_FILE_PATTERN,
                        help=f"File pattern for Virtuoso's ld_dir/ld_dir_all function (e.g., '*.nq', '*.ttl', Default: '{DEFAULT_FILE_PATTERN}').")
    parser.add_argument("-g", "--graph-uri", default=DEFAULT_GRAPH_URI,
                        help=f"Default target graph URI if no .graph file is present (Default: {DEFAULT_GRAPH_URI}). Quad files (.nq, .trig) will use their embedded graph names.")
    parser.add_argument("--recursive", action='store_true',
                        help="Load files recursively from subdirectories using ld_dir_all() instead of ld_dir().")
    parser.add_argument("--log-enable", type=int, default=2, choices=[2, 3],
                        help="log_enable mode for rdf_loader_run(). 2 (default) disables triggers for speed, 3 keeps triggers enabled (e.g., for replication).")
    parser.add_argument("--isql-path", default=DEFAULT_ISQL_PATH_HOST,
                        help=f"Path to the Virtuoso 'isql' executable on the HOST system (Default: '{DEFAULT_ISQL_PATH_HOST}'). Used only if not in Docker mode.")
    parser.add_argument("--checkpoint-interval", type=int, default=DEFAULT_CHECKPOINT_INTERVAL,
                         help=f"Interval (seconds) to set for checkpointing after load (Default: {DEFAULT_CHECKPOINT_INTERVAL}).")
    parser.add_argument("--scheduler-interval", type=int, default=DEFAULT_SCHEDULER_INTERVAL,
                         help=f"Interval (seconds) to set for the scheduler after load (Default: {DEFAULT_SCHEDULER_INTERVAL}).")

    # Docker arguments
    docker_group = parser.add_argument_group('Docker Options')
    docker_group.add_argument("--docker-container",
                        help="Name or ID of the running Virtuoso Docker container. If provided, 'isql' will be run via 'docker exec'.")
    docker_group.add_argument("--docker-data-mount-path",
                        help="The absolute path INSIDE the container where the host data directory (from -d) is mounted. REQUIRED with --docker-container. This path must be in DirsAllowed in the container's virtuoso.ini.")
    docker_group.add_argument("--docker-isql-path", default=DEFAULT_ISQL_PATH_DOCKER,
                        help=f"Path to the 'isql' executable INSIDE the Docker container (Default: '{DEFAULT_ISQL_PATH_DOCKER}').")
    docker_group.add_argument("--docker-path", default=DEFAULT_DOCKER_PATH,
                        help=f"Path to the 'docker' executable on the HOST system (Default: '{DEFAULT_DOCKER_PATH}').")

    args = parser.parse_args()

    host_data_dir_abs = os.path.abspath(args.data_directory)
    if not os.path.isdir(host_data_dir_abs):
        print(f"Error: Host data directory '{host_data_dir_abs}' not found.", file=sys.stderr)
        sys.exit(1)

    if args.docker_container and not args.docker_data_mount_path:
        parser.error("--docker-data-mount-path is required when --docker-container is specified.")
    if not args.docker_container and args.docker_data_mount_path:
        print("Warning: --docker-data-mount-path provided without --docker-container. It will be ignored.", file=sys.stderr)

    if args.num_processes < 1:
        print(f"Error: Number of processes must be at least 1.", file=sys.stderr)
        sys.exit(1)

    # Determine the path Virtuoso server needs to see
    if args.docker_container:
        virtuoso_accessible_data_dir = args.docker_data_mount_path
        print(f"Info: Using Docker. Virtuoso server will access data from container path: '{virtuoso_accessible_data_dir}'")
        print(f"      Ensure this path corresponds to host '{host_data_dir_abs}' and is in the container's DirsAllowed.")
    else:
        virtuoso_accessible_data_dir = host_data_dir_abs # Assuming server runs locally or has direct access
        print(f"Info: Running locally. Virtuoso server will access data from host path: '{virtuoso_accessible_data_dir}'")
        print(f"      Ensure this path is in the server's DirsAllowed configuration.")

    print("-" * 40)
    print("Configuration:")
    print(f"  Host: {args.host}:{args.port}")
    print(f"  User: {args.user}")
    print(f"  Mode: {'Docker' if args.docker_container else 'Local'}")
    print(f"  Virtuoso Data Dir: {virtuoso_accessible_data_dir}")
    print(f"  File Pattern: {args.file_pattern}")
    print(f"  Recursive: {args.recursive}")
    print(f"  Default Graph URI: {args.graph_uri}")
    print(f"  Parallel Loaders: {args.num_processes}")
    print(f"  Log Enable Mode: {args.log_enable}")
    print("-" * 40)

    print("Step 0: Clearing DB.DBA.load_list...")
    clear_sql = "DELETE FROM DB.DBA.load_list;"
    print(f"Executing: {clear_sql}")
    success, _, _ = run_isql_command(clear_sql, args, ignore_errors=True)
    if not success:
        print("Warning: Failed to clear DB.DBA.load_list. Attempting to continue...", file=sys.stderr)
    else:
        print("DB.DBA.load_list cleared or command executed.")
    print("-" * 40)

    print("Step 1: Registering files with Virtuoso...")
    ld_function = "ld_dir_all" if args.recursive else "ld_dir"
    virtuoso_path = virtuoso_accessible_data_dir.replace(os.sep, '/')
    register_sql = f"{ld_function}('{virtuoso_path}', '{args.file_pattern}', '{args.graph_uri}');"
    print(f"Executing: {register_sql}")

    success, _, stderr = run_isql_command(register_sql, args)
    if not success:
        print("Error: Failed to register files with Virtuoso.", file=sys.stderr)
        if "Security violation" in stderr or "cannot process file" in stderr:
             print("Hint: This might be due to the data directory not being listed in 'DirsAllowed'", file=sys.stderr)
             print(f"      in the virtuoso.ini file used by the server process. Required path: '{virtuoso_path}'", file=sys.stderr)
        sys.exit(1)

    print("File registration successful.")
    print("-" * 40)

    print(f"Step 2: Starting {args.num_processes} parallel bulk loader processes...")
    loader_sql = f"rdf_loader_run(log_enable=>{args.log_enable});"
    print(f"Executing {args.num_processes} instances of: {loader_sql}")

    loader_processes = []
    for i in range(args.num_processes):
        command = []
        if args.docker_container:
            command = [
                args.docker_path, 'exec', args.docker_container, args.docker_isql_path,
                f"{args.host}:{args.port}", args.user, args.password, f"exec={loader_sql}"
            ]
        else:
            command = [
                args.isql_path, f"{args.host}:{args.port}", args.user, args.password, f"exec={loader_sql}"
            ]

        try:
            proc = subprocess.Popen(command, text=True, encoding='utf-8', stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            loader_processes.append((proc, i + 1))
            print(f"  Loader process {i+1} started (PID: {proc.pid})...")
            time.sleep(0.2) # Small delay to avoid overwhelming the system/server
        except FileNotFoundError:
             executable = args.docker_path if args.docker_container else args.isql_path
             print(f"Error: Command '{executable}' not found when trying to start loader {i+1}.", file=sys.stderr)
             # Terminate already started processes
             for p, _ in loader_processes:
                 try: p.terminate()
                 except ProcessLookupError: pass
             sys.exit(1)
        except Exception as e:
             print(f"Error starting loader process {i+1}: {e}", file=sys.stderr)
             for p, _ in loader_processes:
                 try: p.terminate()
                 except ProcessLookupError: pass
             sys.exit(1)

    print(f"Waiting for {len(loader_processes)} loader processes to complete...")
    loader_errors = []
    all_loaders_ok = True
    for proc, num in loader_processes:
        stdout, stderr = proc.communicate() # Wait for process to finish
        if proc.returncode != 0:
            all_loaders_ok = False
            error_msg = f"Loader process {num} (PID: {proc.pid}) failed with exit code {proc.returncode}."
            if stderr: error_msg += f"\n  Stderr: {stderr.strip()}"
            if stdout: error_msg += f"\n  Stdout: {stdout.strip()}" # Check stdout too
            print(error_msg, file=sys.stderr)
            loader_errors.append(error_msg)
        else:
            print(f"  Loader process {num} (PID: {proc.pid}) finished successfully.")

    if not all_loaders_ok:
        print("Error: One or more loader processes failed.", file=sys.stderr)
        # Don't exit immediately, proceed to check DB.DBA.load_list and final steps
    else:
        print("All loader processes finished.")
    print("-" * 40)

    print("Step 3: Checking final status from DB.DBA.load_list...")
    status_sql = "SELECT ll_file, ll_graph, ll_state, ll_error FROM DB.DBA.load_list WHERE ll_state != 2 OR ll_error IS NOT NULL ORDER BY ll_file;"
    success, stdout, _ = run_isql_command(status_sql, args, capture=True)

    load_list_errors = []
    if not success:
        print("Warning: Could not query DB.DBA.load_list to check for errors.", file=sys.stderr)
    elif stdout and "rows" in stdout.lower(): # Check if any rows were returned
        print("Found potential issues in DB.DBA.load_list:", file=sys.stderr)
        print(stdout, file=sys.stderr) # Print the raw output for details
        # Basic parsing attempt
        lines = stdout.splitlines()
        header_found = False
        for line in lines:
            if "ll_file" in line and "ll_error" in line: # Find header
                header_found = True
                continue
            if header_found and line.strip() and not line.startswith("____") and not line.startswith("Done."):
                 load_list_errors.append(line.strip())
        if not load_list_errors:
             print("No error rows parsed from DB.DBA.load_list output.")
    else:
        print("No errors found in DB.DBA.load_list (ll_state=2 and ll_error=NULL for all registered files).")

    print("-" * 40)

    print("Step 4: Running final checkpoint...")
    # Restore default intervals as bulk load might disable them
    cleanup_sql = f"checkpoint; checkpoint_interval({args.checkpoint_interval}); scheduler_interval({args.scheduler_interval});"
    print(f"Executing: {cleanup_sql}")
    success, _, _ = run_isql_command(cleanup_sql, args)
    if not success:
        print("Error: Failed to run final checkpoint.", file=sys.stderr)
        if args.log_enable == 2:
            print("CRITICAL WARNING: Checkpoint failed and log_enable=2 was used. BULK LOADED DATA MAY BE LOST.", file=sys.stderr)
            print("Manually run 'checkpoint;' in isql immediately.", file=sys.stderr)
        # Exit with error if checkpoint fails, as it's critical
        sys.exit(1)

    print("Checkpoint successful.")
    print("-" * 40)

    # --- Final Summary ---
    print("Bulk Load Summary:")
    final_status = 0 # 0 = success, 1 = error

    if not all_loaders_ok:
        print(f"- {len(loader_errors)} loader process(es) reported errors during execution.", file=sys.stderr)
        final_status = 1
    else:
        print("- All loader processes completed without exit errors.")

    if load_list_errors:
        print(f"- Found {len(load_list_errors)} entries with errors or non-complete status in DB.DBA.load_list:", file=sys.stderr)
        for err_line in load_list_errors:
             print(f"  - {err_line}", file=sys.stderr)
        final_status = 1
    else:
         # Only report fully successful if loaders were OK *and* load_list is clean
        if all_loaders_ok:
             print("- DB.DBA.load_list indicates all registered files were loaded successfully.")
        else:
             print("- DB.DBA.load_list check passed, but loader processes had issues (see above).")


    print("-" * 40)
    if final_status == 0:
        print("Bulk load finished successfully.")
    else:
        print("Bulk load finished with errors.", file=sys.stderr)

    sys.exit(final_status)


if __name__ == "__main__":
    main() 