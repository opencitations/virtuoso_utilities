#!/usr/bin/env python3
"""
Virtuoso Docker Launcher

This script launches an OpenLink Virtuoso database instance using Docker.
Configuration parameters can be customized through command-line arguments.
"""

import argparse
import configparser
import os
import re
import subprocess
import sys
import time
from typing import List, Tuple

import psutil

DEFAULT_WAIT_TIMEOUT = 120
DOCKER_EXEC_PATH = "docker"
DOCKER_ISQL_PATH_INSIDE_CONTAINER = "isql"

# Default values for container configuration
DEFAULT_IMAGE = "openlink/virtuoso-opensource-7@sha256:e07868a3db9090400332eaa8ee694b8cf9bf7eebc26db6bbdc3bb92fd30ed010"
DEFAULT_CONTAINER_DATA_DIR = "/opt/virtuoso-opensource/database"
DEFAULT_MAX_ROWS = 100000

VIRTUOSO_MEMORY_PERCENTAGE = 0.85
BYTES_PER_BUFFER = 8700  # Each buffer occupies ~8700 bytes (8K page + overhead) according to https://docs.openlinksw.com/virtuoso/ch-server/

from virtuoso_utilities.isql_helpers import run_isql_command

# Minimum database size in bytes to trigger MaxCheckpointRemap calculation
MIN_DB_SIZE_FOR_CHECKPOINT_REMAP_GB = 1
MIN_DB_SIZE_BYTES_FOR_CHECKPOINT_REMAP = MIN_DB_SIZE_FOR_CHECKPOINT_REMAP_GB * 1024**3

# Default directories allowed in Virtuoso
DEFAULT_DIRS_ALLOWED = {".", "../vad", "/usr/share/proj", "../virtuoso_input"}

# Connection error patterns for retry logic
CONNECTION_ERROR_PATTERNS = [
    "connection refused",
    "connect failed",
    "connection failed",
    "cannot connect",
    "no route to host",
]


def bytes_to_docker_mem_str(num_bytes: int) -> str:
    """
    Convert a number of bytes to a Docker memory string (e.g., "85g", "512m").
    Tries to find the largest unit (G, M, K) without losing precision for integers.
    """
    if num_bytes % (1024**3) == 0:
        return f"{num_bytes // (1024**3)}g"
    elif num_bytes % (1024**2) == 0:
        return f"{num_bytes // (1024**2)}m"
    elif num_bytes % 1024 == 0:
         return f"{num_bytes // 1024}k"
    else:
        # Fallback for non-exact multiples (shouldn't happen often with RAM)
        # Prefer GiB for consistency
        gb_val = num_bytes / (1024**3)
        return f"{int(gb_val)}g"


def parse_memory_value(memory_str: str) -> int:
    """
    Parse memory value from Docker memory format (e.g., "2g", "4096m") to bytes.
    
    Args:
        memory_str: Memory string in Docker format
        
    Returns:
        int: Memory size in bytes
    """
    memory_str = memory_str.lower()
    
    match = re.match(r'^(\d+)([kmg]?)$', memory_str)
    if not match:
        # Default to 2GB if parsing fails
        print(f"Warning: Could not parse memory string '{memory_str}'. Defaulting to 2g.", file=sys.stderr)
        return 2 * 1024 * 1024 * 1024
    
    value, unit = match.groups()
    value = int(value)
    
    if unit == 'k':
        return value * 1024
    elif unit == 'm':
        return value * 1024 * 1024
    elif unit == 'g':
        return value * 1024 * 1024 * 1024
    else:  # No unit, assume bytes
        return value


def get_directory_size(directory_path: str) -> int:
    """
    Calculate the total size of all files within a directory.

    Args:
        directory_path: The path to the directory.

    Returns:
        Total size in bytes.
    """
    total_size = 0
    if not os.path.isdir(directory_path):
        return 0
    try:
        for dirpath, dirnames, filenames in os.walk(directory_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    try:
                        total_size += os.path.getsize(fp)
                    except OSError as e:
                        print(f"Warning: Could not get size of file '{fp}': {e}", file=sys.stderr)
    except OSError as e:
        print(f"Warning: Could not walk directory '{directory_path}': {e}", file=sys.stderr)

    return total_size


def get_optimal_buffer_values(memory_limit: str) -> Tuple[int, int]:
    """
    Determine optimal values for NumberOfBuffers and MaxDirtyBuffers
    based on the specified container memory limit.

    Uses the formula recommended by OpenLink:
    NumberOfBuffers = (MemoryInBytes * VIRTUOSO_MEMORY_PERCENTAGE * 0.66) / 8000
    MaxDirtyBuffers = NumberOfBuffers * 0.75

    The memory_limit is reduced by VIRTUOSO_MEMORY_PERCENTAGE to leave
    headroom for Virtuoso process overhead and prevent container OOM crashes.

    Args:
        memory_limit: Memory limit string in Docker format (e.g., "2g", "4096m")

    Returns:
        Tuple[int, int]: Calculated values for NumberOfBuffers and MaxDirtyBuffers
    """
    try:
        memory_bytes = parse_memory_value(memory_limit)

        memory_bytes = int(memory_bytes * VIRTUOSO_MEMORY_PERCENTAGE)

        number_of_buffers = int((memory_bytes * 0.66) / BYTES_PER_BUFFER)

        max_dirty_buffers = int(number_of_buffers * 0.75)

        return number_of_buffers, max_dirty_buffers

    except Exception as e:
        print(f"Warning: Error calculating buffer values: {e}. Using default values.", file=sys.stderr)
        # Default values approximately suitable for 1-2GB RAM if calculation fails
        return 170000, 130000


def calculate_max_checkpoint_remap(size_bytes: int) -> int:
    return int(size_bytes / 8192 / 4)


def get_default_memory() -> str:
    try:
        total_ram = psutil.virtual_memory().total
        default_mem = max(int(total_ram * (2 / 3)), 1 * 1024**3)
        return bytes_to_docker_mem_str(default_mem)
    except Exception:
        return "2g"


def calculate_threading_config(parallel_threads=None):
    cpu_cores = parallel_threads if parallel_threads else (os.cpu_count() or 1)
    return {
        "async_queue_max_threads": int(cpu_cores * 1.5),
        "threads_per_query": cpu_cores,
        "max_client_connections": cpu_cores * 2,
    }


def calculate_max_query_mem(memory, number_of_buffers):
    buffer_memory_bytes = number_of_buffers * BYTES_PER_BUFFER
    effective_memory_bytes = int(parse_memory_value(memory) * VIRTUOSO_MEMORY_PERCENTAGE)
    max_query_mem_bytes = int((effective_memory_bytes - buffer_memory_bytes) * 0.8)
    if max_query_mem_bytes > 0:
        return bytes_to_docker_mem_str(max_query_mem_bytes)
    return None


def get_virt_env_vars(memory, number_of_buffers, max_dirty_buffers, parallel_threads, estimated_db_size_gb=0.0, dirs_allowed=None):
    env_vars = {}
    env_vars["VIRT_Parameters_NumberOfBuffers"] = str(number_of_buffers)
    env_vars["VIRT_Parameters_MaxDirtyBuffers"] = str(max_dirty_buffers)

    threading = calculate_threading_config(parallel_threads)
    env_vars["VIRT_Parameters_AsyncQueueMaxThreads"] = str(threading["async_queue_max_threads"])
    env_vars["VIRT_Parameters_ThreadsPerQuery"] = str(threading["threads_per_query"])
    env_vars["VIRT_Parameters_MaxClientConnections"] = str(threading["max_client_connections"])
    env_vars["VIRT_HTTPServer_ServerThreads"] = str(threading["max_client_connections"])

    env_vars["VIRT_Parameters_AdjustVectorSize"] = "0"
    env_vars["VIRT_Parameters_VectorSize"] = "1000"
    env_vars["VIRT_Parameters_CheckpointInterval"] = "1"
    env_vars["VIRT_Parameters_ThreadCleanupInterval"] = "1"
    env_vars["VIRT_Parameters_ResourcesCleanupInterval"] = "1"

    max_query_mem = calculate_max_query_mem(memory, number_of_buffers)
    max_query_mem_str = max_query_mem if max_query_mem else "N/A"
    if max_query_mem:
        env_vars["VIRT_Parameters_MaxQueryMem"] = max_query_mem

    env_vars["VIRT_Client_SQL_QUERY_TIMEOUT"] = "0"
    env_vars["VIRT_Client_SQL_TXN_TIMEOUT"] = "0"

    if estimated_db_size_gb > 0:
        estimated_size_bytes = int(estimated_db_size_gb * 1024**3)
        if estimated_size_bytes >= MIN_DB_SIZE_BYTES_FOR_CHECKPOINT_REMAP:
            max_checkpoint_remap = calculate_max_checkpoint_remap(estimated_size_bytes)
            env_vars["VIRT_Database_MaxCheckpointRemap"] = str(max_checkpoint_remap)
            env_vars["VIRT_TempDatabase_MaxCheckpointRemap"] = str(max_checkpoint_remap)
            print(f"Info: Using estimated database size of {estimated_db_size_gb} GB to set MaxCheckpointRemap to {max_checkpoint_remap}")

    if dirs_allowed:
        env_vars["VIRT_Parameters_DirsAllowed"] = dirs_allowed

    print(f"Info: Threading: AsyncQueueMaxThreads={threading['async_queue_max_threads']}, "
          f"ThreadsPerQuery={threading['threads_per_query']}, "
          f"MaxClientConnections={threading['max_client_connections']}")
    print(f"Info: MaxQueryMem={max_query_mem_str}, AdjustVectorSize=0, VectorSize=1000, CheckpointInterval=1, ThreadCleanupInterval=1, ResourcesCleanupInterval=1")

    return env_vars


def is_connection_error(stderr):
    stderr_lower = stderr.lower()
    return any(err in stderr_lower for err in CONNECTION_ERROR_PATTERNS)


def create_isql_args(dba_password, docker_container=None):
    if docker_container:
        return argparse.Namespace(
            host="localhost",
            port=1111,
            user="dba",
            password=dba_password,
            docker_container=docker_container,
            docker_path=DOCKER_EXEC_PATH,
            docker_isql_path=DOCKER_ISQL_PATH_INSIDE_CONTAINER,
            isql_path=None,
        )
    return argparse.Namespace(
        host="localhost",
        port=1111,
        user="dba",
        password=dba_password,
        docker_container=None,
        docker_path=None,
        docker_isql_path=None,
        isql_path="isql",
    )


def update_ini_memory_settings(
    ini_path: str,
    data_dir_path: str,
    number_of_buffers: int = None,
    max_dirty_buffers: int = None,
    dirs_allowed: str = None,
    async_queue_max_threads: int = None,
    threads_per_query: int = None,
    max_client_connections: int = None,
    adjust_vector_size: int = None,
    vector_size: int = None,
    checkpoint_interval: int = None,
    max_query_mem: str = None,
    http_server_threads: int = None,
    thread_cleanup_interval: int = None,
    resources_cleanup_interval: int = None,
):
    if not os.path.exists(ini_path):
        print(f"Info: virtuoso.ini not found at '{ini_path}'. Likely first run. Skipping settings update.")
        return

    print(f"Info: Checking existing virtuoso.ini at '{ini_path}' for settings update...")
    actual_db_size_bytes = get_directory_size(data_dir_path)

    # Calculate MaxCheckpointRemap if database is large enough
    calculate_remap = actual_db_size_bytes >= MIN_DB_SIZE_BYTES_FOR_CHECKPOINT_REMAP
    calculated_remap_value = calculate_max_checkpoint_remap(actual_db_size_bytes) if calculate_remap else None

    config = configparser.ConfigParser(interpolation=None, strict=False)
    config.optionxform = str # Keep case sensitivity
    made_changes = False
    try:
        # Read with UTF-8, ignore errors initially if file has issues
        config.read(ini_path, encoding='utf-8')

        # Update [Parameters] section for buffer settings and DirsAllowed
        if not config.has_section('Parameters'):
            config.add_section('Parameters')
            print(f"Info: Added [Parameters] section to '{ini_path}'.")
        
        # Update NumberOfBuffers if provided
        if number_of_buffers is not None:
            current_number_of_buffers = config.get('Parameters', 'NumberOfBuffers', fallback=None)
            number_of_buffers_str = str(number_of_buffers)
            if current_number_of_buffers != number_of_buffers_str:
                config.set('Parameters', 'NumberOfBuffers', number_of_buffers_str)
                print(f"Info: Updating [Parameters] NumberOfBuffers from '{current_number_of_buffers}' to '{number_of_buffers_str}' in '{ini_path}'.")
                made_changes = True

        # Ensure [Client] section has SQL timeouts set to 0
        if not config.has_section('Client'):
            config.add_section('Client')
            print(f"Info: Added [Client] section to '{ini_path}'.")

        current_sql_query_timeout = config.get('Client', 'SQL_QUERY_TIMEOUT', fallback=None)
        if current_sql_query_timeout != '0':
            config.set('Client', 'SQL_QUERY_TIMEOUT', '0')
            print(f"Info: Setting [Client] SQL_QUERY_TIMEOUT to '0' in '{ini_path}'.")
            made_changes = True

        current_sql_txn_timeout = config.get('Client', 'SQL_TXN_TIMEOUT', fallback=None)
        if current_sql_txn_timeout != '0':
            config.set('Client', 'SQL_TXN_TIMEOUT', '0')
            print(f"Info: Setting [Client] SQL_TXN_TIMEOUT to '0' in '{ini_path}'.")
            made_changes = True

        # Update MaxDirtyBuffers if provided
        if max_dirty_buffers is not None:
            current_max_dirty_buffers = config.get('Parameters', 'MaxDirtyBuffers', fallback=None)
            max_dirty_buffers_str = str(max_dirty_buffers)
            if current_max_dirty_buffers != max_dirty_buffers_str:
                config.set('Parameters', 'MaxDirtyBuffers', max_dirty_buffers_str)
                print(f"Info: Updating [Parameters] MaxDirtyBuffers from '{current_max_dirty_buffers}' to '{max_dirty_buffers_str}' in '{ini_path}'.")
                made_changes = True

        if dirs_allowed is not None:
            current_dirs_allowed = config.get('Parameters', 'DirsAllowed', fallback=None)
            def normalize_dirs(val):
                if val is None:
                    return set()
                return set([x.strip() for x in val.split(',') if x.strip()])
            if normalize_dirs(current_dirs_allowed) != normalize_dirs(dirs_allowed):
                config.set('Parameters', 'DirsAllowed', dirs_allowed)
                print(f"Info: Updating [Parameters] DirsAllowed from '{current_dirs_allowed}' to '{dirs_allowed}' in '{ini_path}'.")
                made_changes = True

        if async_queue_max_threads is not None:
            current_val = config.get('Parameters', 'AsyncQueueMaxThreads', fallback=None)
            new_val = str(async_queue_max_threads)
            if current_val != new_val:
                config.set('Parameters', 'AsyncQueueMaxThreads', new_val)
                print(f"Info: Updating [Parameters] AsyncQueueMaxThreads from '{current_val}' to '{new_val}' in '{ini_path}'.")
                made_changes = True

        if threads_per_query is not None:
            current_val = config.get('Parameters', 'ThreadsPerQuery', fallback=None)
            new_val = str(threads_per_query)
            if current_val != new_val:
                config.set('Parameters', 'ThreadsPerQuery', new_val)
                print(f"Info: Updating [Parameters] ThreadsPerQuery from '{current_val}' to '{new_val}' in '{ini_path}'.")
                made_changes = True

        if max_client_connections is not None:
            current_val = config.get('Parameters', 'MaxClientConnections', fallback=None)
            new_val = str(max_client_connections)
            if current_val != new_val:
                config.set('Parameters', 'MaxClientConnections', new_val)
                print(f"Info: Updating [Parameters] MaxClientConnections from '{current_val}' to '{new_val}' in '{ini_path}'.")
                made_changes = True

        if adjust_vector_size is not None:
            current_val = config.get('Parameters', 'AdjustVectorSize', fallback=None)
            new_val = str(adjust_vector_size)
            if current_val != new_val:
                config.set('Parameters', 'AdjustVectorSize', new_val)
                print(f"Info: Updating [Parameters] AdjustVectorSize from '{current_val}' to '{new_val}' in '{ini_path}'.")
                made_changes = True

        if vector_size is not None:
            current_val = config.get('Parameters', 'VectorSize', fallback=None)
            new_val = str(vector_size)
            if current_val != new_val:
                config.set('Parameters', 'VectorSize', new_val)
                print(f"Info: Updating [Parameters] VectorSize from '{current_val}' to '{new_val}' in '{ini_path}'.")
                made_changes = True

        if checkpoint_interval is not None:
            current_val = config.get('Parameters', 'CheckpointInterval', fallback=None)
            new_val = str(checkpoint_interval)
            if current_val != new_val:
                config.set('Parameters', 'CheckpointInterval', new_val)
                print(f"Info: Updating [Parameters] CheckpointInterval from '{current_val}' to '{new_val}' in '{ini_path}'.")
                made_changes = True

        if max_query_mem is not None:
            current_val = config.get('Parameters', 'MaxQueryMem', fallback=None)
            if current_val != max_query_mem:
                config.set('Parameters', 'MaxQueryMem', max_query_mem)
                print(f"Info: Updating [Parameters] MaxQueryMem from '{current_val}' to '{max_query_mem}' in '{ini_path}'.")
                made_changes = True

        if http_server_threads is not None:
            if not config.has_section('HTTPServer'):
                config.add_section('HTTPServer')
                print(f"Info: Added [HTTPServer] section to '{ini_path}'.")
            current_val = config.get('HTTPServer', 'ServerThreads', fallback=None)
            new_val = str(http_server_threads)
            if current_val != new_val:
                config.set('HTTPServer', 'ServerThreads', new_val)
                print(f"Info: Updating [HTTPServer] ServerThreads from '{current_val}' to '{new_val}' in '{ini_path}'.")
                made_changes = True

        if thread_cleanup_interval is not None:
            current_val = config.get('Parameters', 'ThreadCleanupInterval', fallback=None)
            new_val = str(thread_cleanup_interval)
            if current_val != new_val:
                config.set('Parameters', 'ThreadCleanupInterval', new_val)
                print(f"Info: Updating [Parameters] ThreadCleanupInterval from '{current_val}' to '{new_val}' in '{ini_path}'.")
                made_changes = True

        if resources_cleanup_interval is not None:
            current_val = config.get('Parameters', 'ResourcesCleanupInterval', fallback=None)
            new_val = str(resources_cleanup_interval)
            if current_val != new_val:
                config.set('Parameters', 'ResourcesCleanupInterval', new_val)
                print(f"Info: Updating [Parameters] ResourcesCleanupInterval from '{current_val}' to '{new_val}' in '{ini_path}'.")
                made_changes = True

        # Update MaxCheckpointRemap if database is large enough
        if calculate_remap:
            # Update [Database] section
            if not config.has_section('Database'):
                config.add_section('Database')
                print(f"Info: Added [Database] section to '{ini_path}'.")
            
            current_db_remap = config.get('Database', 'MaxCheckpointRemap', fallback=None)
            calculated_remap_str = str(calculated_remap_value)
            if current_db_remap != calculated_remap_str:
                config.set('Database', 'MaxCheckpointRemap', calculated_remap_str)
                print(f"Info: Updating [Database] MaxCheckpointRemap from '{current_db_remap}' to '{calculated_remap_str}' in '{ini_path}'.")
                made_changes = True

            # Update [TempDatabase] section
            if not config.has_section('TempDatabase'):
                config.add_section('TempDatabase')
                print(f"Info: Added [TempDatabase] section to '{ini_path}'.")
            
            current_temp_db_remap = config.get('TempDatabase', 'MaxCheckpointRemap', fallback=None)
            if current_temp_db_remap != calculated_remap_str:
                config.set('TempDatabase', 'MaxCheckpointRemap', calculated_remap_str)
                print(f"Info: Updating [TempDatabase] MaxCheckpointRemap from '{current_temp_db_remap}' to '{calculated_remap_str}' in '{ini_path}'.")
                made_changes = True
        else:
            print(f"Info: Host data directory '{data_dir_path}' size ({actual_db_size_bytes / (1024**3):.2f} GiB) is below threshold ({MIN_DB_SIZE_FOR_CHECKPOINT_REMAP_GB} GiB). No changes made to MaxCheckpointRemap in virtuoso.ini.")

        if made_changes:
            # Write changes back with UTF-8 encoding
            with open(ini_path, 'w', encoding='utf-8') as configfile:
                config.write(configfile)
            print(f"Info: Successfully saved changes to '{ini_path}'.")
        else:
            print(f"Info: No changes needed in '{ini_path}'.")

    except configparser.Error as e:
        print(f"Error: Failed to parse or update virtuoso.ini at '{ini_path}': {e}", file=sys.stderr)
    except IOError as e:
        print(f"Error: Failed to read or write virtuoso.ini at '{ini_path}': {e}", file=sys.stderr)
    except Exception as e:
        print(f"Error: An unexpected error occurred while updating virtuoso.ini: {e}", file=sys.stderr)


def parse_arguments() -> argparse.Namespace:  # pragma: no cover
    """
    Parse command-line arguments for Virtuoso Docker launcher.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    # First create a parser for a preliminary parse to check if --memory is provided
    preliminary_parser = argparse.ArgumentParser(add_help=False)
    preliminary_parser.add_argument("--memory", default=None)
    preliminary_args, _ = preliminary_parser.parse_known_args()
    memory_specified = preliminary_args.memory is not None
    
    # Full parser with all arguments
    parser = argparse.ArgumentParser(
        description="Launch a Virtuoso database using Docker",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # --- Calculate default memory based on host RAM (2/3) ---
    default_memory_str = "2g" # Fallback default
    if psutil and not memory_specified:
        try:
            total_host_ram = psutil.virtual_memory().total
            # Calculate 2/3 of total RAM in bytes
            default_mem_bytes = int(total_host_ram * (2/3))
            # Ensure at least 1GB is allocated as a minimum default
            min_default_bytes = 1 * 1024 * 1024 * 1024
            if default_mem_bytes < min_default_bytes:
                default_mem_bytes = min_default_bytes

            default_memory_str = bytes_to_docker_mem_str(default_mem_bytes)
            print(f"Info: Detected {total_host_ram / (1024**3):.1f} GiB total host RAM. "
                  f"Setting default container memory limit to {default_memory_str} (approx. 2/3). "
                  f"Use --memory to override.")
        except Exception as e:
            print(f"Warning: Could not auto-detect host RAM using psutil: {e}. "
                  f"Falling back to default memory limit '{default_memory_str}'.", file=sys.stderr)
    elif psutil and memory_specified:
        # Silently use the user-specified value
        pass
    else:
         print(f"Warning: psutil not found. Cannot auto-detect host RAM. "
               f"Falling back to default memory limit '{default_memory_str}'. "
               f"Install psutil for automatic calculation.", file=sys.stderr)

    parser.add_argument(
        "--name", 
        default="virtuoso",
        help="Name for the Docker container"
    )
    parser.add_argument(
        "--http-port", 
        type=int, 
        default=8890,
        help="HTTP port to expose Virtuoso on"
    )
    parser.add_argument(
        "--isql-port", 
        type=int, 
        default=1111,
        help="ISQL port to expose Virtuoso on"
    )
    
    parser.add_argument(
        "--data-dir", 
        default="./virtuoso-data",
        help="Host directory to mount as Virtuoso data directory"
    )
    
    parser.add_argument(
        "--mount-volume",
        action="append",
        dest="extra_volumes",
        metavar="HOST_PATH:CONTAINER_PATH",
        help="Mount an additional host directory into the container. "
             "Format: /path/on/host:/path/in/container. "
             "Can be specified multiple times."
    )
    
    parser.add_argument(
        "--memory", 
        default=default_memory_str,
        help="Memory limit for the container (e.g., 2g, 4g). "
             f"Defaults to approx. 2/3 of host RAM if psutil is installed, otherwise '{default_memory_str}'."
    )
    parser.add_argument(
        "--cpu-limit", 
        type=float, 
        default=0,
        help="CPU limit for the container (0 means no limit)"
    )
    
    parser.add_argument(
        "--dba-password", 
        default="dba",
        help="Password for the Virtuoso dba user"
    )
    
    parser.add_argument(
        "--force-remove", 
        action="store_true",
        help="Force removal of existing container with the same name"
    )
    
    parser.add_argument(
        "--network",
        help="Docker network to connect the container to (must be a pre-existing network)"
    )

    parser.add_argument(
        "--wait-ready", 
        action="store_true",
        help="Wait until Virtuoso is ready to accept connections"
    )
    parser.add_argument(
        "--detach", 
        action="store_true",
        help="Run container in detached mode"
    )
    
    parser.add_argument(
        "--enable-write-permissions",
        action="store_true",
        help="Enable write permissions for 'nobody' and 'SPARQL' users. "
             "This makes the database publicly writable. "
             "Forces waiting for the container to be ready."
    )
    
    parser.add_argument(
        "--estimated-db-size-gb",
        type=float,
        default=0,
        help="Estimated database size in GB. If provided, MaxCheckpointRemap will be preconfigured "
             "based on this estimate rather than measuring existing data."
    )
    
    parser.add_argument(
        "--virtuoso-version",
        default=None,
        help="Virtuoso Docker image version/tag to use (e.g., 'latest', '7.2.11', '7.2.12'). If not specified, uses the default pinned version."
    )
    
    parser.add_argument(
        "--virtuoso-sha",
        default=None,
        help="Virtuoso Docker image SHA256 digest to use (e.g., 'sha256:e07868a3db9090400332eaa8ee694b8cf9bf7eebc26db6bbdc3bb92fd30ed010'). Takes precedence over --virtuoso-version."
    )

    parser.add_argument(
        "--parallel-threads",
        type=int,
        default=None,
        help="Maximum parallel threads for query execution. "
             "If not specified, uses all available CPU cores. "
             "Sets AsyncQueueMaxThreads to cores * 1.5 and ThreadsPerQuery to cores."
    )

    args_temp, _ = parser.parse_known_args()

    optimal_number_of_buffers, optimal_max_dirty_buffers = get_optimal_buffer_values(args_temp.memory)
    
    parser.add_argument(
        "--max-dirty-buffers", 
        type=int, 
        default=optimal_max_dirty_buffers,
        help="Maximum dirty buffers before checkpoint (auto-calculated based on --memory value, requires integer)"
    )
    parser.add_argument(
        "--number-of-buffers", 
        type=int, 
        default=optimal_number_of_buffers,
        help="Number of buffers (auto-calculated based on --memory value, requires integer)"
    )
    
    return parser.parse_args()


def check_docker_installed() -> bool:
    """
    Check if Docker is installed and accessible.
    
    Returns:
        bool: True if Docker is installed, False otherwise
    """
    try:
        subprocess.run(
            ["docker", "--version"], 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            check=True
        )
        return True
    except (subprocess.SubprocessError, FileNotFoundError):
        return False


def check_container_exists(container_name: str) -> bool:
    """
    Check if a Docker container with the specified name exists.
    
    Args:
        container_name: Name of the container to check
        
    Returns:
        bool: True if container exists, False otherwise
    """
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"name=^{container_name}$", "--format", "{{.Names}}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    return container_name in result.stdout.strip()


def remove_container(container_name: str) -> bool:
    """
    Remove a Docker container.
    
    Args:
        container_name: Name of the container to remove
        
    Returns:
        bool: True if container was removed successfully, False otherwise
    """
    try:
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True
        )
        return True
    except subprocess.SubprocessError:
        return False


def get_docker_image(version: str, sha: str) -> str:
    """
    Get the appropriate Docker image based on version or SHA parameter.
    
    Args:
        version: Version string (e.g., 'latest', '7.2.11', '7.2.12') or None for default
        sha: SHA256 digest string or None
        
    Returns:
        str: Full Docker image reference
    """
    if sha is not None:
        return f"openlink/virtuoso-opensource-7@{sha}"
    elif version is None:
        return DEFAULT_IMAGE
    elif version == "latest":
        return "openlink/virtuoso-opensource-7:latest"
    else:
        return f"openlink/virtuoso-opensource-7:{version}"


def build_docker_run_command(args: argparse.Namespace) -> Tuple[List[str], List[str]]:
    """
    Build the Docker run command based on provided arguments.
    
    Args:
        args: Command-line arguments
        
    Returns:
        Tuple[List[str], List[str]]: 
            - Command parts for subprocess.run
            - List of unique container paths intended for DirsAllowed
    """
    host_data_dir_abs = os.path.abspath(args.data_dir)
    os.makedirs(host_data_dir_abs, exist_ok=True)
    
    cmd = [DOCKER_EXEC_PATH, "run"]
    
    cmd.extend(["--name", args.name])
    
    # Add user mapping to run as the host user
    try:
        cmd.extend(["--user", f"{os.getuid()}:{os.getgid()}"])
    except AttributeError:
        print("Warning: os.getuid/os.getgid not available on this system (likely Windows). Skipping user mapping.", file=sys.stderr)

    cmd.extend(["-p", f"{args.http_port}:8890"])
    cmd.extend(["-p", f"{args.isql_port}:1111"])
    
    if args.network:
        cmd.extend(["--network", args.network])
    
    # Ensure container_data_dir is absolute-like for consistency
    container_data_dir_path = DEFAULT_CONTAINER_DATA_DIR
    cmd.extend(["-v", f"{host_data_dir_abs}:{container_data_dir_path}"])

    # Mount additional volumes
    if args.extra_volumes:
        for volume_spec in args.extra_volumes:
            if ':' in volume_spec:
                host_path, container_path = volume_spec.split(':', 1)
                host_path_abs = os.path.abspath(host_path)
                cmd.extend(["-v", f"{host_path_abs}:{container_path}"])

    # Start with default Virtuoso paths
    paths_to_allow_in_container = DEFAULT_DIRS_ALLOWED.copy()
    paths_to_allow_in_container.add(container_data_dir_path)
    
    # Add extra mounted volumes to paths_to_allow_in_container
    if args.extra_volumes:
        for volume_spec in args.extra_volumes:
            if ':' in volume_spec:
                _, container_path = volume_spec.split(':', 1)
                container_path_abs = container_path if container_path.startswith('/') else '/' + container_path
                paths_to_allow_in_container.add(container_path_abs)
                print(f"Info: Adding mounted volume path '{container_path_abs}' to DirsAllowed.")

    memory_bytes = parse_memory_value(args.memory)
    reservation_bytes = int(memory_bytes * VIRTUOSO_MEMORY_PERCENTAGE)
    reservation_str = bytes_to_docker_mem_str(reservation_bytes)
    cmd.extend(["--memory-reservation", reservation_str])
    cmd.extend(["--memory", args.memory])
    if args.cpu_limit > 0:
        cmd.extend(["--cpus", str(args.cpu_limit)])
    
    env_vars = {
        "DBA_PASSWORD": args.dba_password,
        "VIRT_Parameters_ResultSetMaxRows": str(DEFAULT_MAX_ROWS),
        "VIRT_SPARQL_DefaultQuery": "SELECT (COUNT(*) AS ?quadCount) WHERE { GRAPH ?g { ?s ?p ?o } }",
    }

    virt_env_vars = get_virt_env_vars(
        memory=args.memory,
        number_of_buffers=args.number_of_buffers,
        max_dirty_buffers=args.max_dirty_buffers,
        parallel_threads=args.parallel_threads,
        estimated_db_size_gb=args.estimated_db_size_gb,
        dirs_allowed=",".join(paths_to_allow_in_container),
    )
    env_vars.update(virt_env_vars)

    for key, value in env_vars.items():
        cmd.extend(["-e", f"{key}={value}"])
    
    if args.detach:
        cmd.append("-d")
    
    # Ensure --rm is added if not running detached
    if not args.detach:
        cmd.insert(2, "--rm") # Insert after "docker run"
    
    # Append image name
    docker_image = get_docker_image(args.virtuoso_version, args.virtuoso_sha)
    cmd.append(docker_image)
    
    return cmd, paths_to_allow_in_container


def wait_for_virtuoso_ready(
    dba_password: str,
    docker_container: str = None,
    timeout: int = DEFAULT_WAIT_TIMEOUT,
    poll_interval: int = 3,
) -> bool:
    print(f"Waiting for Virtuoso to be ready (timeout: {timeout}s)...")
    start_time = time.time()
    isql_args = create_isql_args(dba_password, docker_container)

    while time.time() - start_time < timeout:
        try:
            success, _, stderr = run_isql_command(isql_args, sql_command="status();")
            if success:
                print("Virtuoso is ready.")
                return True
            if is_connection_error(stderr):
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0:
                    print(f"  Waiting for Virtuoso... ({elapsed}s elapsed)")
            else:
                print(f"ISQL check failed: {stderr}", file=sys.stderr)
                return False
            time.sleep(poll_interval)
        except Exception as e:
            print(f"Warning: Error in readiness check: {e}", file=sys.stderr)
            time.sleep(poll_interval + 2)

    print(f"Timeout ({timeout}s) waiting for Virtuoso.", file=sys.stderr)
    return False


def run_docker_command(cmd: List[str], capture_output=False, check=True, suppress_error=False):
    """Helper to run Docker commands and handle errors."""
    print(f"Executing: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE if capture_output else sys.stdout,
            stderr=subprocess.PIPE if capture_output else sys.stderr,
            text=True,
            check=check
        )
        return result
    except subprocess.CalledProcessError as e:
        if not suppress_error:
            print(f"Error executing Docker command: {e}", file=sys.stderr)
            if capture_output:
                print(f"Stderr: {e.stderr}", file=sys.stderr)
                print(f"Stdout: {e.stdout}", file=sys.stderr)
        raise
    except FileNotFoundError:
         if not suppress_error:
            print("Error: 'docker' command not found. Make sure Docker is installed and in your PATH.", file=sys.stderr)
         raise


def grant_write_permissions(dba_password: str, docker_container: str = None) -> bool:
    print("Granting write permissions...")
    isql_args = create_isql_args(dba_password, docker_container)

    success1, _, stderr1 = run_isql_command(
        isql_args, sql_command="DB.DBA.RDF_DEFAULT_USER_PERMS_SET('nobody', 7);"
    )
    if success1:
        print("  Set permissions for 'nobody' user.")
    else:
        print(f"  Warning: Failed to set 'nobody' permissions: {stderr1}", file=sys.stderr)

    success2, _, stderr2 = run_isql_command(
        isql_args, sql_command="DB.DBA.USER_GRANT_ROLE('SPARQL', 'SPARQL_UPDATE');"
    )
    if success2:
        print("  Granted SPARQL_UPDATE role to 'SPARQL' user.")
    else:
        print(f"  Warning: Failed to grant SPARQL_UPDATE: {stderr2}", file=sys.stderr)

    return success1 and success2


def launch_virtuoso(  # pragma: no cover
    name: str = "virtuoso",
    data_dir: str = "./virtuoso-data",
    http_port: int = 8890,
    isql_port: int = 1111,
    memory: str = None,
    dba_password: str = "dba",
    detach: bool = True,
    wait_ready: bool = True,
    enable_write_permissions: bool = False,
    force_remove: bool = False,
    extra_volumes: list = None,
    network: str = None,
    cpu_limit: float = 0,
    virtuoso_version: str = None,
    virtuoso_sha: str = None,
    estimated_db_size_gb: float = 0,
    parallel_threads: int = None,
) -> None:
    """
    Launch Virtuoso Docker container.

    Args:
        name: Container name
        data_dir: Host directory for Virtuoso data
        http_port: HTTP port to expose
        isql_port: ISQL port to expose
        memory: Memory limit (e.g., "4g"). Auto-calculated from host RAM if None.
        dba_password: DBA password
        detach: Run in detached mode
        wait_ready: Wait for Virtuoso to be ready
        enable_write_permissions: Enable SPARQL write permissions for 'nobody' and 'SPARQL' users
        force_remove: Force remove existing container with same name
        extra_volumes: Additional volumes to mount (list of "host:container" strings)
        network: Docker network to connect
        cpu_limit: CPU limit (0 = no limit)
        virtuoso_version: Docker image version tag
        virtuoso_sha: Docker image SHA digest (takes precedence over version)
        estimated_db_size_gb: Estimated DB size for MaxCheckpointRemap config
        parallel_threads: Max parallel threads for query execution. If None, uses all CPU cores.

    Raises:
        RuntimeError: If Docker is not installed or launch fails
    """
    if not check_docker_installed():
        raise RuntimeError("Docker command not found. Please install Docker.")

    if memory is None:
        if psutil:
            try:
                total_host_ram = psutil.virtual_memory().total
                default_mem_bytes = max(int(total_host_ram * (2/3)), 1 * 1024**3)
                memory = bytes_to_docker_mem_str(default_mem_bytes)
            except Exception:
                memory = "2g"
        else:
            memory = "2g"

    number_of_buffers, max_dirty_buffers = get_optimal_buffer_values(memory)

    args = argparse.Namespace(
        name=name,
        data_dir=data_dir,
        http_port=http_port,
        isql_port=isql_port,
        memory=memory,
        dba_password=dba_password,
        detach=detach,
        wait_ready=wait_ready,
        enable_write_permissions=enable_write_permissions,
        force_remove=force_remove,
        extra_volumes=extra_volumes,
        network=network,
        cpu_limit=cpu_limit,
        virtuoso_version=virtuoso_version,
        virtuoso_sha=virtuoso_sha,
        estimated_db_size_gb=estimated_db_size_gb,
        number_of_buffers=number_of_buffers,
        max_dirty_buffers=max_dirty_buffers,
        parallel_threads=parallel_threads,
    )

    host_data_dir_abs = os.path.abspath(data_dir)
    ini_file_path = os.path.join(host_data_dir_abs, "virtuoso.ini")

    docker_cmd, unique_paths_to_allow = build_docker_run_command(args)
    dirs_allowed_str = ",".join(unique_paths_to_allow) if unique_paths_to_allow else None

    threading = calculate_threading_config(parallel_threads)
    max_query_mem_value = calculate_max_query_mem(memory, number_of_buffers)

    update_ini_memory_settings(
        ini_path=ini_file_path,
        data_dir_path=host_data_dir_abs,
        number_of_buffers=number_of_buffers,
        max_dirty_buffers=max_dirty_buffers,
        dirs_allowed=dirs_allowed_str,
        async_queue_max_threads=threading["async_queue_max_threads"],
        threads_per_query=threading["threads_per_query"],
        max_client_connections=threading["max_client_connections"],
        adjust_vector_size=0,
        vector_size=1000,
        checkpoint_interval=1,
        max_query_mem=max_query_mem_value,
        http_server_threads=threading["max_client_connections"],
        thread_cleanup_interval=1,
        resources_cleanup_interval=1,
    )

    if check_container_exists(name):
        result = subprocess.run(
            [DOCKER_EXEC_PATH, "ps", "--filter", f"name=^{name}$", "--format", "{{.Status}}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        is_running = "Up" in result.stdout

        if force_remove:
            print(f"Container '{name}' already exists. Forcing removal...")
            if not remove_container(name):
                raise RuntimeError(f"Failed to remove existing container '{name}'")
        elif is_running:
            raise RuntimeError(f"Container '{name}' is already running. Stop it first or use force_remove=True.")
        else:
            print(f"Container '{name}' exists but is stopped. Removing...")
            if not remove_container(name):
                raise RuntimeError(f"Failed to remove existing stopped container '{name}'")

    try:
        run_docker_command(docker_cmd, check=not detach)

        should_wait = wait_ready or enable_write_permissions

        if detach and should_wait:
            print("Waiting for Virtuoso readiness...")
            ready = wait_for_virtuoso_ready(dba_password, docker_container=name)
            if not ready:
                raise RuntimeError("Virtuoso readiness check timed out or failed.")

            if enable_write_permissions:
                if not grant_write_permissions(dba_password, docker_container=name):
                    print("Warning: One or more commands to enable write permissions failed.", file=sys.stderr)

        print(f"Virtuoso launched successfully on http://localhost:{http_port}/sparql")

    except subprocess.CalledProcessError as e:
        if detach and check_container_exists(name):
            run_docker_command([DOCKER_EXEC_PATH, "stop", name], suppress_error=True, check=False)
            run_docker_command([DOCKER_EXEC_PATH, "rm", name], suppress_error=True, check=False)
        raise RuntimeError(f"Virtuoso launch failed: {e}")
    except FileNotFoundError:
        raise RuntimeError("Docker command not found.")


def main() -> int: # pragma: no cover
    """
    CLI entry point that parses arguments and calls launch_virtuoso().
    """
    args = parse_arguments()

    try:
        launch_virtuoso(
            name=args.name,
            data_dir=args.data_dir,
            http_port=args.http_port,
            isql_port=args.isql_port,
            memory=args.memory,
            dba_password=args.dba_password,
            detach=args.detach,
            wait_ready=args.wait_ready,
            enable_write_permissions=args.enable_write_permissions,
            force_remove=args.force_remove,
            extra_volumes=args.extra_volumes,
            network=args.network,
            cpu_limit=args.cpu_limit,
            virtuoso_version=args.virtuoso_version,
            virtuoso_sha=args.virtuoso_sha,
            estimated_db_size_gb=args.estimated_db_size_gb,
            parallel_threads=args.parallel_threads,
        )
        return 0
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())