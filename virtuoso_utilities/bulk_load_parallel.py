#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Performs parallel bulk loading of RDF N-Quads files into OpenLink Virtuoso.

Uses direct file loading approach to avoid the DB.DBA.load_list bottleneck
by handling file tracking directly in Python. Optimized specifically for
N-Quads files where graph URIs are embedded in the data.

Uses multiprocessing instead of threading to achieve true parallelization,
avoiding Python's Global Interpreter Lock (GIL) limitations.

IMPORTANT:
- The data directory specified ('-d' or '--data-directory') MUST be
  accessible by the Virtuoso server process itself.
- This directory path MUST be listed in the 'DirsAllowed' parameter
  within the Virtuoso INI file (e.g., virtuoso.ini).
- When using Docker, data-directory is the path INSIDE the container.
  Files will be accessed and loaded from within the container.
"""

import argparse
import os
import sys
import time
import glob
import datetime
import subprocess
from multiprocessing import Process, Queue, Manager, cpu_count, current_process
from collections import defaultdict
from tqdm import tqdm
from virtuoso_utilities.isql_helpers import run_isql_command

DEFAULT_VIRTUOSO_HOST = "localhost"
DEFAULT_VIRTUOSO_PORT = 1111
DEFAULT_VIRTUOSO_USER = "dba"
DEFAULT_NUM_PROCESSES = max(1, int(cpu_count() / 2.5))
DEFAULT_FILE_PATTERN = '*.nq' # Default N-Quads file pattern
DEFAULT_ISQL_PATH_HOST = "isql"
DEFAULT_ISQL_PATH_DOCKER = "isql" # Often '/opt/virtuoso-opensource/bin/isql' in containers
DEFAULT_DOCKER_PATH = "docker"
DEFAULT_CHECKPOINT_INTERVAL = 60 # Default Virtuoso checkpoint interval (seconds)
DEFAULT_SCHEDULER_INTERVAL = 10 # Default Virtuoso scheduler interval (seconds)
DEFAULT_BATCH_SIZE = 100 # Default number of files per batch
# Placeholder graph URI required by TTLP even when ignored for N-Quads (flag 512)
DEFAULT_PLACEHOLDER_GRAPH = "http://localhost:8890/DAV/ignored"


def find_nquads_files_local(directory, pattern, recursive=False):
    """
    Find all N-Quads files in a directory on local filesystem.
    Returns a list of file paths.
    """
    if recursive:
        matches = []
        for root, _, _ in os.walk(directory):
            path_pattern = os.path.join(root, pattern)
            matches.extend(glob.glob(path_pattern))
        return matches
    else:
        path_pattern = os.path.join(directory, pattern)
        return glob.glob(path_pattern)


def find_nquads_files_docker(container, directory, pattern, recursive, docker_path="docker"):
    """
    Find all N-Quads files in a directory inside a Docker container.
    Uses 'docker exec' to run find command inside the container.
    Returns a list of file paths.
    """
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


def load_nquads_file(file_path, args):
    """
    Load a single N-Quads file into Virtuoso using the TTLP function directly.
    Returns tuple (success, error_message, time_taken)
    """
    start_time = time.time()
    
    # N-Quads specific flags: 512 tells TTLP to get graph from the quad itself
    flags = "512"
    
    # TTLP requires a non-null base URI (use '') and a placeholder graph URI (3rd arg)
    # when using flag 512. The placeholder graph is ignored.
    placeholder_graph = DEFAULT_PLACEHOLDER_GRAPH
    if file_path.endswith('.gz'):
        sql_command = f"TTLP(gz_file_open('{file_path}'), '', '{placeholder_graph}', {flags});"
    else:
        sql_command = f"TTLP(file_open('{file_path}'), '', '{placeholder_graph}', {flags});"
    
    success, stdout, stderr = run_isql_command(args, sql_command=sql_command, capture=True)
    end_time = time.time()
    
    error_msg = stderr if not success else ""
    return success, error_msg, end_time - start_time


def file_loader_process(task_queue, result_dict, args, process_id):
    """
    Worker process function that loads files from a queue.
    Uses multiprocessing for true parallelization.
    """
    proc_name = current_process().name
    files_processed = 0
    total_time = 0
    
    try:
        while True:
            try:
                # Get a file path from the queue with timeout
                file_path = task_queue.get(timeout=1)
                
                # Check for termination signal
                if file_path == "DONE":
                    break
                
                # Process the file
                success, error, time_taken = load_nquads_file(file_path, args)
                
                # Store the result
                result_dict[file_path] = {
                    'success': success,
                    'error': error,
                    'time': time_taken,
                    'worker': process_id,
                    'timestamp': datetime.datetime.now().isoformat()
                }
                
                files_processed += 1
                total_time += time_taken
                
                # Signal task completion for progress tracking
                task_queue.task_done()
                
            except Queue.Empty:
                # No more tasks in queue
                break
                
    except Exception as e:
        print(f"Process {proc_name} (ID: {process_id}) encountered an error: {str(e)}", 
              file=sys.stderr)
    
    print(f"Process {proc_name} (ID: {process_id}) completed {files_processed} files " +
          f"in {total_time:.2f} seconds")
    return


def main():
    """
    Main function to parse arguments and orchestrate the parallel bulk loading.
    """
    parser = argparse.ArgumentParser(
        description="Parallel N-Quads bulk loader for OpenLink Virtuoso with multiprocessing-based file tracking.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example usage:
  # Load all *.nq files from /data/rdf (local mode)
  python bulk_load_parallel.py -d /data/rdf -k mypassword

  # Load using Docker, where files are at /database/data in container
  python bulk_load_parallel.py -d /database/data -k mypassword --recursive -n 4 \\
    --docker-container virtuoso_container \\
    --docker-isql-path /opt/virtuoso/bin/isql

IMPORTANT: 
- The data directory (-d) must be accessible by the Virtuoso process
  and listed in the 'DirsAllowed' setting in virtuoso.ini.
- When using Docker mode, data-directory is the path INSIDE the container.
  Files are accessed and loaded directly inside the container.
"""
    )

    # Core arguments
    parser.add_argument("-d", "--data-directory", required=True,
                        help="Path to the N-Quads files. When using Docker, this must be the path INSIDE the container.")
    parser.add_argument("-H", "--host", default=DEFAULT_VIRTUOSO_HOST,
                        help=f"Virtuoso server host (Default: {DEFAULT_VIRTUOSO_HOST}).")
    parser.add_argument("-P", "--port", type=int, default=DEFAULT_VIRTUOSO_PORT,
                        help=f"Virtuoso server port (Default: {DEFAULT_VIRTUOSO_PORT}).")
    parser.add_argument("-u", "--user", default=DEFAULT_VIRTUOSO_USER,
                        help=f"Virtuoso username (Default: {DEFAULT_VIRTUOSO_USER}).")
    parser.add_argument("-k", "--password", required=True,
                        help="Virtuoso password.")
    parser.add_argument("-n", "--num-processes", type=int, default=DEFAULT_NUM_PROCESSES,
                        help=f"Number of parallel file loading processes (Default: {DEFAULT_NUM_PROCESSES}, based on CPU cores / 2.5).")
    parser.add_argument("-f", "--file-pattern", default=DEFAULT_FILE_PATTERN,
                        help=f"File pattern for finding N-Quads files (Default: '{DEFAULT_FILE_PATTERN}').")
    parser.add_argument("--recursive", action='store_true',
                        help="Load files recursively from subdirectories.")
    parser.add_argument("--isql-path", default=DEFAULT_ISQL_PATH_HOST,
                        help=f"Path to the Virtuoso 'isql' executable on the HOST system (Default: '{DEFAULT_ISQL_PATH_HOST}'). Used only if not in Docker mode.")
    parser.add_argument("--checkpoint-interval", type=int, default=DEFAULT_CHECKPOINT_INTERVAL,
                         help=f"Interval (seconds) to set for checkpointing after load (Default: {DEFAULT_CHECKPOINT_INTERVAL}).")
    parser.add_argument("--scheduler-interval", type=int, default=DEFAULT_SCHEDULER_INTERVAL,
                         help=f"Interval (seconds) to set for the scheduler after load (Default: {DEFAULT_SCHEDULER_INTERVAL}).")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Number of files to load in a batch (Default: {DEFAULT_BATCH_SIZE}).")

    # Docker arguments
    docker_group = parser.add_argument_group('Docker Options')
    docker_group.add_argument("--docker-container",
                        help="Name or ID of the running Virtuoso Docker container. If provided, 'isql' will be run via 'docker exec'.")
    docker_group.add_argument("--docker-isql-path", default=DEFAULT_ISQL_PATH_DOCKER,
                        help=f"Path to the 'isql' executable INSIDE the Docker container (Default: '{DEFAULT_ISQL_PATH_DOCKER}').")
    docker_group.add_argument("--docker-path", default=DEFAULT_DOCKER_PATH,
                        help=f"Path to the 'docker' executable on the HOST system (Default: '{DEFAULT_DOCKER_PATH}').")

    args = parser.parse_args()

    # --- Argument Validation and Path Handling ---
    data_dir = args.data_directory

    # Ensure absolute path
    if not os.path.isabs(data_dir):
        data_dir = os.path.abspath(data_dir)
        print(f"Converted data directory to absolute path: {data_dir}")

    if args.docker_container:
        print(f"Info: Using Docker container '{args.docker_container}'")
        print(f"      Files will be accessed at '{data_dir}' inside the container.")
        print(f"      Ensure this path is correctly mounted and listed in container's DirsAllowed.")
    else:
        print(f"Info: Running locally. Files will be accessed at: {data_dir}")
        print(f"      Ensure this path is accessible by the Virtuoso process and listed in DirsAllowed.")

    if args.num_processes < 1:
        print(f"Error: Number of processes must be at least 1.", file=sys.stderr)
        sys.exit(1)

    print("-" * 40)
    print("Configuration:")
    print(f"  Host: {args.host}:{args.port}")
    print(f"  User: {args.user}")
    print(f"  Mode: {'Docker' if args.docker_container else 'Local'}")
    print(f"  Data Dir: {data_dir}")
    print(f"  File Pattern: {args.file_pattern}")
    print(f"  Recursive: {args.recursive}")
    print(f"  Parallel Loaders: {args.num_processes}")
    print(f"  Batch Size: {args.batch_size}")
    print("-" * 40)

    # --- Step 1: Find all N-Quads files ---
    print(f"Step 1: Finding N-Quads files in {data_dir}...")
    
    if args.docker_container:
        # Find files INSIDE the Docker container
        print(f"Searching for files inside Docker container '{args.docker_container}'...")
        files = find_nquads_files_docker(
            container=args.docker_container,
            directory=data_dir,
            pattern=args.file_pattern,
            recursive=args.recursive,
            docker_path=args.docker_path
        )
    else:
        # Find files on local system
        print(f"Searching for files on local filesystem...")
        files = find_nquads_files_local(data_dir, args.file_pattern, args.recursive)
    
    if not files:
        print(f"Error: No files matching '{args.file_pattern}' found in '{data_dir}'.", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(files)} files matching pattern.")
    
    # --- Step 2: Check that Virtuoso can access the files ---
    print(f"Step 2: Validating Virtuoso access to files...")
    
    test_file = files[0]
    test_sql = f"SELECT file_stat('{test_file}');"
    
    print(f"Testing Virtuoso access with file: {test_file}")
    success, stdout, stderr = run_isql_command(args, sql_command=test_sql, capture=True)
    if not success or "Security violation" in stderr or "cannot" in stderr:
        print(f"Error: Virtuoso cannot access the data files.", file=sys.stderr)
        print(f"  Test file: {test_file}", file=sys.stderr)
        print(f"  Ensure the path is in Virtuoso's DirsAllowed configuration.", file=sys.stderr)
        print(f"  Error: {stderr}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Virtuoso can access the data files.")
    
    # --- Step 3: Set Virtuoso log_enable(2) for optimized loading ---
    print(f"Step 3: Setting Virtuoso log_enable(2) for bulk loading...")
    
    set_log_enable_sql = "log_enable(2, 1);" # Always use mode 2 (quiet)
    print(f"Executing: {set_log_enable_sql}")
    success_log, _, stderr_log = run_isql_command(args, sql_command=set_log_enable_sql)
    if not success_log:
        print(f"Warning: Failed to set log_enable(2). Error: {stderr_log}", file=sys.stderr)
        # Decide whether to exit or continue. Continuing might impact performance/stability.
        # sys.exit(1) 

    # --- Step 4: Load files in parallel using multiprocessing ---
    print(f"Step 4: Loading {len(files)} N-Quads files using {args.num_processes} worker processes...")
    
    # Create a multiprocessing.JoinableQueue for tasks
    task_queue = Manager().JoinableQueue()
    
    # Create a shared dictionary to store results
    manager = Manager()
    results_dict = manager.dict()
    
    # Add all files to the queue
    for file_path in files:
        task_queue.put(file_path)
    
    # Start worker processes
    processes = []
    for i in range(args.num_processes):
        p = Process(
            target=file_loader_process, 
            args=(task_queue, results_dict, args, i+1),
            name=f"FileLoader-{i+1}"
        )
        processes.append(p)
        p.daemon = True
        p.start()
        print(f"Started process {p.name} (PID: {p.pid})")
    
    # Monitor progress with tqdm - this is trickier with multiprocessing
    total_files = len(files)
    with tqdm(total=total_files, desc="Loading N-Quads files") as pbar:
        completed = 0
        try:
            # Monitor until all files are processed
            while completed < total_files:
                # Use queue size to determine progress
                remaining = task_queue.qsize()
                new_completed = total_files - remaining
                if new_completed > completed:
                    pbar.update(new_completed - completed)
                    completed = new_completed
                time.sleep(0.1)
            
        except KeyboardInterrupt:
            print("\nInterrupted by user. Stopping processes...", file=sys.stderr)
            # Send termination signal to all processes
            for _ in range(args.num_processes):
                task_queue.put("DONE")
            
            # Wait for processes to terminate
            for p in processes:
                p.join(timeout=2.0)
                if p.is_alive():
                    p.terminate()
            
            print("Processes stopped. Some files may not have been loaded.", file=sys.stderr)
            sys.exit(1)
    
    # Send termination signal to all processes
    for _ in range(args.num_processes):
        task_queue.put("DONE")
    
    # Wait for all processes to finish
    for p in processes:
        p.join()
    
    # Convert manager dict to regular dict for easier processing
    results = dict(results_dict)
    
    # --- Step 5: Restore default log_enable(3) and run checkpoint ---
    print("\nStep 5: Restoring default settings and running final checkpoint...")
    
    # Restore log_enable(3) THEN checkpoint
    cleanup_sql = f"log_enable(3, 1); checkpoint; checkpoint_interval({args.checkpoint_interval}); scheduler_interval({args.scheduler_interval});"
    print(f"Executing: {cleanup_sql}")
    success_final, _, stderr_final = run_isql_command(args, sql_command=cleanup_sql)
    if not success_final:
        print("Error: Failed to run final checkpoint and restore settings.", file=sys.stderr)
        print(f"Error details: {stderr_final}", file=sys.stderr)
        # Always assume log_enable(2) was used if checkpoint fails
        print("CRITICAL WARNING: Checkpoint failed after using log_enable(2). LOADED DATA MAY BE LOST.", file=sys.stderr)
        print("Manually run 'checkpoint;' in isql immediately and verify data.", file=sys.stderr)
        sys.exit(1)

    print("Checkpoint successful and settings restored.")
    print("-" * 40)
    
    # --- Final Summary ---
    print("Bulk Load Summary:")
    successful_files = [f for f, r in results.items() if r['success']]
    failed_files = [f for f, r in results.items() if not r['success']]
    
    print(f"- Successfully loaded: {len(successful_files)} files")
    print(f"- Failed to load: {len(failed_files)} files")
    
    if failed_files:
        print("\nErrors encountered:")
        for failed_file in failed_files[:10]:  # Show first 10 errors
            error = results[failed_file]['error']
            print(f"  - {os.path.basename(failed_file)}: {error}")
        
        if len(failed_files) > 10:
            print(f"  (and {len(failed_files) - 10} more...)")
    
    # Calculate statistics
    total_time = sum(r['time'] for r in results.values())
    avg_time = total_time / len(results) if results else 0
    
    print("\nPerformance Statistics:")
    print(f"- Total processing time: {total_time:.2f} seconds")
    print(f"- Average time per file: {avg_time:.2f} seconds")
    
    # Worker statistics
    print("\nWorker Statistics:")
    worker_files = defaultdict(int)
    worker_times = defaultdict(float)
    
    for file_path, result in results.items():
        worker_id = result['worker']
        worker_files[worker_id] += 1
        worker_times[worker_id] += result['time']
    
    for worker_id in sorted(worker_files.keys()):
        files_processed = worker_files[worker_id]
        time_spent = worker_times[worker_id]
        print(f"  - Worker {worker_id}: {files_processed} files in {time_spent:.2f} seconds " +
              f"({files_processed/time_spent:.2f} files/sec)")
    
    print("-" * 40)
    if failed_files:
        print("Bulk load finished with errors. See details above.", file=sys.stderr)
        sys.exit(1)
    else:
        print("Bulk load finished successfully.")
        sys.exit(0)


if __name__ == "__main__":
    main() 