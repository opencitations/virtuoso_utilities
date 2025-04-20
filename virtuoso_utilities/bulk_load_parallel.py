#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Performs parallel bulk loading of RDF files into OpenLink Virtuoso.

Finds RDF files in a specified directory (either by pattern or by common RDF extensions)
and uses multiprocessing to load them in parallel into a Virtuoso database
using the 'isql' command-line utility.
"""

import argparse
import glob
import os
import subprocess
import sys
from functools import partial
from multiprocessing import Pool, cpu_count

from tqdm import tqdm

DEFAULT_VIRTUOSO_HOST = "localhost"
DEFAULT_VIRTUOSO_PORT = 1111
DEFAULT_VIRTUOSO_USER = "dba"
DEFAULT_NUM_PROCESSES = cpu_count()
DEFAULT_GRAPH_URI = "http://localhost:8890/DAV"
DEFAULT_ISQL_PATH_HOST = "isql"
DEFAULT_ISQL_PATH_DOCKER = "isql"
DEFAULT_DOCKER_PATH = "docker"

COMMON_RDF_EXTENSIONS = ('.ttl', '.rdf', '.owl', '.nq', '.trig', '.trix', '.xml')

def load_file_virtuoso(
    file_path,
    host, port, user, password, graph_uri,
    host_data_dir,
    docker_container=None,
    docker_data_mount_path=None,
    docker_isql_path=DEFAULT_ISQL_PATH_DOCKER,
    docker_path=DEFAULT_DOCKER_PATH,
    host_isql_path=DEFAULT_ISQL_PATH_HOST
):
    """
    Loads a single RDF file into Virtuoso using the isql command,
    potentially executing it inside a Docker container.

    Args:
        file_path (str): The absolute path to the RDF file **on the host**.
        host (str): Virtuoso server host.
        port (int): Virtuoso server port.
        user (str): Virtuoso username.
        password (str): Virtuoso password.
        graph_uri (str): Target graph URI.
        host_data_dir (str): The data directory path **on the host** as provided via -d.
        docker_container (str | None): Name or ID of the Docker container, if used.
        docker_data_mount_path (str | None): Path where host_data_dir is mounted **inside the container**.
        docker_isql_path (str): Path to 'isql' executable **inside the container**.
        docker_path (str): Path to 'docker' executable **on the host**.
        host_isql_path (str): Path to 'isql' executable **on the host**.

    Returns:
        tuple: (file_path, success_status, message)
               success_status is True if loading succeeded, False otherwise.
               message contains stdout or stderr.
    """

    file_extension = os.path.splitext(file_path)[1].lower()
    load_command = ""
    container_file_path = None 

    if docker_container:
        if not docker_data_mount_path:
            return file_path, False, "Error: --docker-data-mount-path is required with --docker-container"
        try:
            relative_path = os.path.relpath(file_path, os.path.abspath(host_data_dir))
            container_file_path = os.path.join(docker_data_mount_path, relative_path).replace(os.sep, '/')
        except ValueError as e:
            return file_path, False, f"Error calculating container path for {file_path} relative to {host_data_dir}: {e}"
        path_in_virtuoso_cmd = container_file_path
    else:
        path_in_virtuoso_cmd = file_path

    if file_extension == ".ttl":
        load_command = f"DB.DBA.TTLP(file_to_string_output('{path_in_virtuoso_cmd}'), '', '{graph_uri}');"
    elif file_extension in [".rdf", ".xml", ".owl"]:
        load_command = f"DB.DBA.RDF_LOAD_RDFXML(file_to_string_output('{path_in_virtuoso_cmd}'), '', '{graph_uri}');"
    elif file_extension in [".nq", ".trig", ".trix"]:
        load_command = f"DB.DBA.TTLP(file_to_string_output('{path_in_virtuoso_cmd}'), '', '');"
    else:
        return file_path, False, f"Unsupported file extension: {file_extension}"

    virtuoso_procedure = f"log_enable(2); {load_command}"

    command = []
    if docker_container:
        command = [
            docker_path,
            'exec',
            docker_container,
            docker_isql_path,
            f"{host}:{port}",
            user,
            password,
            f"exec={virtuoso_procedure}"
        ]
        effective_isql_path_for_error = f"'{docker_isql_path}' inside container '{docker_container}'"
    else:
        command = [
            host_isql_path,
            f"{host}:{port}",
            user,
            password,
            f"exec={virtuoso_procedure}"
        ]
        effective_isql_path_for_error = f"'{host_isql_path}' on host"

    try:
        process = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        return file_path, True, process.stdout.strip()
    except FileNotFoundError:
        if docker_container:
            print(f"Error: '{docker_path}' command not found or failed to execute. Make sure Docker is installed and running.", file=sys.stderr)
        else:
            print(f"Error: {effective_isql_path_for_error} command not found. Make sure client tools are installed and in your PATH.", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        error_message = f"Error loading {os.path.basename(file_path)}."
        error_message += f"\nCommand: {' '.join(command)}"
        error_message += f"\nReturn Code: {e.returncode}"
        error_message += f"\nStderr: {e.stderr.strip()}"
        error_message += f"\nStdout: {e.stdout.strip()}"
        print(error_message, file=sys.stderr)
        return file_path, False, error_message
    except Exception as e:
        error_message = f"An unexpected error occurred loading {os.path.basename(file_path)}: {e}"
        print(error_message, file=sys.stderr)
        return file_path, False, error_message

def main():
    """
    Main function to parse arguments and orchestrate the parallel loading.
    """
    parser = argparse.ArgumentParser(description="Parallel RDF bulk loader for OpenLink Virtuoso.")

    parser.add_argument("-d", "--data-directory", required=True,
                        help="Directory containing the RDF files to load.")
    parser.add_argument("-H", "--host", default=DEFAULT_VIRTUOSO_HOST,
                        help=f"Virtuoso server host (Default: {DEFAULT_VIRTUOSO_HOST}).")
    parser.add_argument("-P", "--port", type=int, default=DEFAULT_VIRTUOSO_PORT,
                        help=f"Virtuoso server port (Default: {DEFAULT_VIRTUOSO_PORT}).")
    parser.add_argument("-u", "--user", default=DEFAULT_VIRTUOSO_USER,
                        help=f"Virtuoso username (Default: {DEFAULT_VIRTUOSO_USER}).")
    parser.add_argument("-k", "--password", required=True,
                        help="Virtuoso password.")
    parser.add_argument("-n", "--num-processes", type=int, default=DEFAULT_NUM_PROCESSES,
                        help=f"Number of parallel processes (Default: {DEFAULT_NUM_PROCESSES}).")
    parser.add_argument("-f", "--file-pattern", default=None,
                        help="File pattern to match (e.g., '*.nq', '*.rdf'). If not provided, automatically searches for common RDF extensions: " + ", ".join(COMMON_RDF_EXTENSIONS))
    parser.add_argument("-g", "--graph-uri", default=DEFAULT_GRAPH_URI,
                        help=f"Target graph URI (Default: {DEFAULT_GRAPH_URI}).")
    parser.add_argument("--isql-path", default="isql",
                        help=f"Path to the Virtuoso 'isql' executable on the HOST system (Default: '{DEFAULT_ISQL_PATH_HOST}'). Used only if not in Docker mode.")

    docker_group = parser.add_argument_group('Docker Options')
    docker_group.add_argument("--docker-container",
                        help="Name or ID of the running Virtuoso Docker container. If provided, 'isql' will be run via 'docker exec'.")
    docker_group.add_argument("--docker-data-mount-path",
                        help="The absolute path INSIDE the container where the host data directory (from -d) is mounted. REQUIRED with --docker-container.")
    docker_group.add_argument("--docker-isql-path", default=DEFAULT_ISQL_PATH_DOCKER,
                        help=f"Path to the 'isql' executable INSIDE the Docker container (Default: '{DEFAULT_ISQL_PATH_DOCKER}').")
    docker_group.add_argument("--docker-path", default=DEFAULT_DOCKER_PATH,
                        help=f"Path to the 'docker' executable on the HOST system (Default: '{DEFAULT_DOCKER_PATH}').")

    args = parser.parse_args()

    if not os.path.isdir(args.data_directory):
        print(f"Error: Data directory '{args.data_directory}' not found.", file=sys.stderr)
        sys.exit(1)

    # Validate Docker arguments
    if args.docker_container and not args.docker_data_mount_path:
        parser.error("--docker-data-mount-path is required when --docker-container is specified.")
    if not args.docker_container and args.docker_data_mount_path:
        print("Warning: --docker-data-mount-path provided without --docker-container. It will be ignored.", file=sys.stderr)

    if args.num_processes < 1:
        print(f"Error: Number of processes must be at least 1.", file=sys.stderr)
        sys.exit(1)

    files_to_load = []
    search_description = ""

    if args.file_pattern:
        search_path = os.path.join(args.data_directory, args.file_pattern)
        files_to_load = [os.path.abspath(f) for f in glob.glob(search_path)]
        search_description = f"pattern '{args.file_pattern}'"
    else:
        print(f"No file pattern provided. Searching for files with extensions: {', '.join(COMMON_RDF_EXTENSIONS)} in '{args.data_directory}'...")
        try:
            for filename in os.listdir(args.data_directory):
                if filename.lower().endswith(COMMON_RDF_EXTENSIONS):
                    full_path = os.path.join(args.data_directory, filename)
                    if os.path.isfile(full_path):
                        files_to_load.append(os.path.abspath(full_path))
            search_description = f"common RDF extensions ({', '.join(COMMON_RDF_EXTENSIONS)})"
        except OSError as e:
            print(f"Error reading directory '{args.data_directory}': {e}", file=sys.stderr)
            sys.exit(1)

    if not files_to_load:
        print(f"No files found matching {search_description} in directory '{args.data_directory}'.", file=sys.stderr)
        sys.exit(0)

    files_to_load.sort()

    print(f"Found {len(files_to_load)} files to load using {search_description}.")
    print(f"Starting parallel bulk load with {args.num_processes} processes...")
    print(f"Host: {args.host}:{args.port}")
    if args.docker_container:
        print(f"Mode: Docker (Container: '{args.docker_container}', Mount Path: '{args.docker_data_mount_path}')")
    else:
        print("Mode: Local")
    print(f"User: {args.user}")

    has_quad_files = any(f.lower().endswith(('.nq', '.trig', '.trix')) for f in files_to_load)
    has_triple_files = any(f.lower().endswith(('.ttl', '.rdf', '.owl', '.xml')) for f in files_to_load)

    if has_triple_files:
        if has_quad_files:
            print(f"Target Graph (for .ttl/.rdf/.owl/.xml): {args.graph_uri} (Note: Quad files define their own graphs)")
        else:
            print(f"Target Graph: {args.graph_uri}")
    elif has_quad_files:
        print("Target Graph: Determined by quad files (.nq/.trig/.trix)")

    print("-------------------------------------------")

    worker_func = partial(load_file_virtuoso,
                          host=args.host,
                          port=args.port,
                          user=args.user,
                          password=args.password,
                          graph_uri=args.graph_uri,
                          host_data_dir=args.data_directory,
                          docker_container=args.docker_container,
                          docker_data_mount_path=args.docker_data_mount_path,
                          docker_isql_path=args.docker_isql_path,
                          docker_path=args.docker_path,
                          host_isql_path=args.isql_path)

    successful_loads = 0
    failed_loads = 0
    with Pool(processes=args.num_processes) as pool:
        results = list(tqdm(pool.imap(worker_func, files_to_load), total=len(files_to_load), desc="Loading files"))

    print("-------------------------------------------")
    print("Load process finished. Results:")
    for file_path, success, message in results:
        if success:
            successful_loads += 1
        else:
            failed_loads += 1
            print(f"Failure: {os.path.basename(file_path)}")

    print("-------------------------------------------")
    print("All parallel loading processes finished.")

    print("Running final checkpoint...")
    checkpoint_command = []

    if args.docker_container:
        checkpoint_command = [
            args.docker_path,
            'exec',
            args.docker_container,
            args.docker_isql_path,
            f"{args.host}:{args.port}",
            args.user,
            args.password,
            "exec=checkpoint;"
        ]
        effective_checkpoint_isql_path = f"'{args.docker_isql_path}' inside container '{args.docker_container}'"
    else:
        checkpoint_command = [
            args.isql_path,
            f"{args.host}:{args.port}",
            args.user,
            args.password,
            "exec=checkpoint;"
        ]
        effective_checkpoint_isql_path = f"'{args.isql_path}' on host"

    try:
        cp_process = subprocess.run(checkpoint_command, capture_output=True, text=True, check=True, encoding='utf-8')
        print("Checkpoint successful.")
        print(f"Output: {cp_process.stdout.strip()}")
    except FileNotFoundError:
        if args.docker_container:
            print(f"Error: '{args.docker_path}' command not found or failed during checkpoint. Make sure Docker is installed and running.", file=sys.stderr)
        else:
            print(f"Error: {effective_checkpoint_isql_path} command not found during checkpoint.", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        error_message = f"Error running checkpoint.\n"
        error_message += f"Command: {' '.join(checkpoint_command)}\n"
        error_message += f"Return Code: {e.returncode}\n"
        error_message += f"Stderr: {e.stderr.strip()}\n"
        error_message += f"Stdout: {e.stdout.strip()}"
        print(error_message, file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        error_message = f"An unexpected error occurred during checkpoint: {e}"
        print(error_message, file=sys.stderr)
        sys.exit(1)

    print("-------------------------------------------")
    print(f"Summary: {successful_loads} files loaded successfully, {failed_loads} files failed.")

    if failed_loads > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main() 