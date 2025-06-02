#!/usr/bin/env python3
"""
Virtuoso Docker Launcher

This script launches an OpenLink Virtuoso database instance using Docker.
Configuration parameters can be customized through command-line arguments.
"""

import argparse
import os
import re
import subprocess
import sys
import time
from typing import List, Tuple
import configparser

import psutil

DEFAULT_WAIT_TIMEOUT = 120
DOCKER_EXEC_PATH = "docker"
DOCKER_ISQL_PATH_INSIDE_CONTAINER = "isql"

# Default values for container configuration
DEFAULT_IMAGE = "openlink/virtuoso-opensource-7@sha256:e07868a3db9090400332eaa8ee694b8cf9bf7eebc26db6bbdc3bb92fd30ed010"
DEFAULT_CONTAINER_DATA_DIR = "/opt/virtuoso-opensource/database"
DEFAULT_MAX_ROWS = 100000

from virtuoso_utilities.isql_helpers import run_isql_command

# Minimum database size in bytes to trigger MaxCheckpointRemap calculation
MIN_DB_SIZE_FOR_CHECKPOINT_REMAP_GB = 1
MIN_DB_SIZE_BYTES_FOR_CHECKPOINT_REMAP = MIN_DB_SIZE_FOR_CHECKPOINT_REMAP_GB * 1024**3

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
    NumberOfBuffers = (MemoryInBytes * 0.66) / 8000
    MaxDirtyBuffers = NumberOfBuffers * 0.75
    
    Args:
        memory_limit: Memory limit string in Docker format (e.g., "2g", "4096m")
        
    Returns:
        Tuple[int, int]: Calculated values for NumberOfBuffers and MaxDirtyBuffers
    """
    try:
        memory_bytes = parse_memory_value(memory_limit)
        
        number_of_buffers = int((memory_bytes * 0.66) / 8000)
        
        max_dirty_buffers = int(number_of_buffers * 0.75)
                    
        return number_of_buffers, max_dirty_buffers

    except Exception as e:
        print(f"Warning: Error calculating buffer values: {e}. Using default values.", file=sys.stderr)
        # Default values approximately suitable for 1-2GB RAM if calculation fails
        return 170000, 130000


def calculate_max_checkpoint_remap(size_bytes: int) -> int:
    """
    Calculate the MaxCheckpointRemap value based on database size.
    
    Args:
        size_bytes: Database size in bytes
        
    Returns:
        int: Calculated MaxCheckpointRemap value
    """
    return int(size_bytes / 8192 / 4)


def update_ini_memory_settings(ini_path: str, data_dir_path: str, number_of_buffers: int = None, max_dirty_buffers: int = None, dirs_allowed: str = None):
    """
    Updates memory-related settings in the virtuoso.ini file:
    - MaxCheckpointRemap: based on the actual size of the data directory
    - NumberOfBuffers: if provided
    - MaxDirtyBuffers: if provided
    - DirsAllowed: if provided

    Args:
        ini_path: The full path to the virtuoso.ini file.
        data_dir_path: The path to the data directory to measure for MaxCheckpointRemap.
        number_of_buffers: Optional value for NumberOfBuffers setting.
        max_dirty_buffers: Optional value for MaxDirtyBuffers setting.
        dirs_allowed: Optional value for DirsAllowed setting (comma-separated string).
    """
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


def parse_arguments() -> argparse.Namespace:
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
        "--estimated-db-size-gb",
        type=float,
        default=0,
        help="Estimated database size in GB. If provided, MaxCheckpointRemap will be preconfigured "
             "based on this estimate rather than measuring existing data."
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
    default_dirs_allowed = {".", "../vad", "/usr/share/proj", "../virtuoso_input"}
    paths_to_allow_in_container = default_dirs_allowed
    paths_to_allow_in_container.add(container_data_dir_path)
    
    # Add extra mounted volumes to paths_to_allow_in_container
    if args.extra_volumes:
        for volume_spec in args.extra_volumes:
            if ':' in volume_spec:
                _, container_path = volume_spec.split(':', 1)
                container_path_abs = container_path if container_path.startswith('/') else '/' + container_path
                paths_to_allow_in_container.add(container_path_abs)
                print(f"Info: Adding mounted volume path '{container_path_abs}' to DirsAllowed.")
    
    cmd.extend(["--memory", args.memory])
    if args.cpu_limit > 0:
        cmd.extend(["--cpus", str(args.cpu_limit)])
    
    env_vars = {
        "DBA_PASSWORD": args.dba_password,
        "VIRT_Parameters_ResultSetMaxRows": str(DEFAULT_MAX_ROWS),
        "VIRT_Parameters_MaxDirtyBuffers": str(args.max_dirty_buffers),
        "VIRT_Parameters_NumberOfBuffers": str(args.number_of_buffers),
        "VIRT_Parameters_DirsAllowed": ",".join(paths_to_allow_in_container),
        "VIRT_SPARQL_DefaultQuery": "SELECT (COUNT(*) AS ?quadCount) WHERE { GRAPH ?g { ?s ?p ?o } }"
    }
    
    # Set MaxCheckpointRemap environment variables if estimated size is provided
    if args.estimated_db_size_gb > 0:
        estimated_size_bytes = int(args.estimated_db_size_gb * 1024**3)
        if estimated_size_bytes >= MIN_DB_SIZE_BYTES_FOR_CHECKPOINT_REMAP:
            max_checkpoint_remap = calculate_max_checkpoint_remap(estimated_size_bytes)
            env_vars["VIRT_Database_MaxCheckpointRemap"] = str(max_checkpoint_remap)
            env_vars["VIRT_TempDatabase_MaxCheckpointRemap"] = str(max_checkpoint_remap)
            print(f"Info: Using estimated database size of {args.estimated_db_size_gb} GB to set MaxCheckpointRemap to {max_checkpoint_remap}")

    for key, value in env_vars.items():
        cmd.extend(["-e", f"{key}={value}"])
    
    if args.detach:
        cmd.append("-d")
    
    # Ensure --rm is added if not running detached
    if not args.detach:
        cmd.insert(2, "--rm") # Insert after "docker run"
    
    # Append image name
    cmd.append(DEFAULT_IMAGE)
    
    return cmd, paths_to_allow_in_container


def wait_for_virtuoso_ready(
    container_name: str,
    host: str, # Usually localhost for readiness check
    isql_port: int,
    dba_password: str,
    timeout: int = 120
) -> bool:
    """
    Wait until Virtuoso is ready to accept ISQL connections.

    Uses isql_helpers.run_isql_command to execute 'status();'.

    Args:
        container_name: Name of the Virtuoso container (used for logging)
        host: Hostname or IP address to connect to (usually localhost).
        isql_port: The ISQL port Virtuoso is listening on (host port).
        dba_password: The DBA password for Virtuoso.
        timeout: Maximum time to wait in seconds.

    Returns:
        bool: True if Virtuoso is ready, False if timeout or error occurred.
    """
    print(f"Waiting for Virtuoso ISQL connection via Docker exec (timeout: {timeout}s)... using '{DOCKER_ISQL_PATH_INSIDE_CONTAINER}' in container")
    start_time = time.time()

    # Create a temporary args object compatible with run_isql_command
    isql_helper_args = argparse.Namespace(
        host="localhost",
        port=1111,
        user="dba",
        password=dba_password,
        docker_container=container_name,
        docker_path=DOCKER_EXEC_PATH,
        docker_isql_path=DOCKER_ISQL_PATH_INSIDE_CONTAINER,
        isql_path=None
    )

    while time.time() - start_time < timeout:
        try:
            success, stdout, stderr = run_isql_command(
                isql_helper_args,
                sql_command="status();",
                capture=True
            )

            if success:
                print("Virtuoso is ready! (ISQL connection successful)")
                return True
            else:
                stderr_lower = stderr.lower()
                if "connection refused" in stderr_lower or \
                   "connect failed" in stderr_lower or \
                   "connection failed" in stderr_lower or \
                   "cannot connect" in stderr_lower or \
                   "no route to host" in stderr_lower:
                    if int(time.time() - start_time) % 10 == 0:
                        print(f"  (ISQL connection failed, retrying... {int(time.time() - start_time)}s elapsed)")
                else:
                    print(f"ISQL check failed with an unexpected error. See previous logs. Stopping wait.", file=sys.stderr)
                    return False

            time.sleep(3)

        except Exception as e:
            print(f"Warning: Unexpected error in readiness check loop: {e}", file=sys.stderr)
            time.sleep(5)

    print(f"Timeout ({timeout}s) waiting for Virtuoso ISQL connection at {host}:{isql_port}.")
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


def main() -> int:
    """
    Main function to launch Virtuoso with Docker.
    """
    args = parse_arguments()

    if not check_docker_installed():
        print("Error: Docker command not found. Please install Docker.", file=sys.stderr)
        return 1

    host_data_dir_abs = os.path.abspath(args.data_dir)
    ini_file_path = os.path.join(host_data_dir_abs, "virtuoso.ini")

    docker_cmd, unique_paths_to_allow = build_docker_run_command(args)
    dirs_allowed_str = ",".join(unique_paths_to_allow) if unique_paths_to_allow else None

    update_ini_memory_settings(ini_file_path, host_data_dir_abs, args.number_of_buffers, args.max_dirty_buffers, dirs_allowed=dirs_allowed_str)

    container_exists = check_container_exists(args.name)

    if container_exists:
        print(f"Checking status of existing container '{args.name}'...")
        # Check if it's running
        result = subprocess.run(
            [DOCKER_EXEC_PATH, "ps", "--filter", f"name=^{args.name}$", "--format", "{{.Status}}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        is_running = "Up" in result.stdout

        if args.force_remove:
            print(f"Container '{args.name}' already exists. Forcing removal...")
            if not remove_container(args.name):
                print(f"Error: Failed to remove existing container '{args.name}'", file=sys.stderr)
                return 1
        elif is_running:
             print(f"Error: Container '{args.name}' is already running. Stop it first or use --force-remove.", file=sys.stderr)
             return 1
        else: # Exists but not running
             print(f"Container '{args.name}' exists but is stopped. Removing it before starting anew...")
             if not remove_container(args.name):
                print(f"Error: Failed to remove existing stopped container '{args.name}'", file=sys.stderr)
                return 1

    # Build the command and get paths for logging
    docker_cmd, unique_paths_to_allow = build_docker_run_command(args)

    try:
        run_docker_command(docker_cmd, check=not args.detach) # Don't check exit code if detached

        if args.detach and args.wait_ready:
            print("Waiting for Virtuoso readiness...")
            if not wait_for_virtuoso_ready(
                args.name,
                "localhost", # Assuming ISQL check connects via localhost mapping
                args.isql_port,
                args.dba_password,
                timeout=DEFAULT_WAIT_TIMEOUT
            ):
                print("Warning: Container started in detached mode but readiness check timed out or failed.", file=sys.stderr)
                # Don't exit with error, just warn
        elif not args.detach:
             # If running attached, it only exits when Virtuoso stops or fails
             print("Virtuoso container exited.")
             return 0 # Assume normal exit if not detached and no exception


        print(f"""
Virtuoso launched successfully!
- Data Directory Host: {host_data_dir_abs}
- Data Directory Container: {DEFAULT_CONTAINER_DATA_DIR}
- Web interface: http://localhost:{args.http_port}/conductor
- ISQL access (Host): isql localhost:{args.isql_port} dba {args.dba_password}
- ISQL access (Inside container): isql localhost:1111 dba {args.dba_password}
- Container name: {args.name}
""")
        if args.extra_volumes:
            print("Additional mounted volumes:")
            for volume_spec in args.extra_volumes:
                 if ':' in volume_spec:
                    host_path, container_path = volume_spec.split(':', 1)
                    container_path_abs = container_path if container_path.startswith('/') else '/' + container_path
                    print(f"  - Host: {os.path.abspath(host_path)} -> Container: {container_path_abs}")
        if unique_paths_to_allow:
             print(f"DirsAllowed set in container via environment variable to: {', '.join(unique_paths_to_allow)}")

        return 0

    except subprocess.CalledProcessError:
        print("\nVirtuoso launch failed. Check Docker logs for errors.", file=sys.stderr)
        # Attempt cleanup only if the container was meant to be persistent (detached)
        # or if we know it might have been created partially.
        if args.detach and check_container_exists(args.name):
             print(f"Attempting to stop potentially problematic container '{args.name}' ...", file=sys.stderr)
             run_docker_command([DOCKER_EXEC_PATH, "stop", args.name], suppress_error=True, check=False)
             print(f"Attempting to remove potentially problematic container '{args.name}' ...", file=sys.stderr)
             run_docker_command([DOCKER_EXEC_PATH, "rm", args.name], suppress_error=True, check=False)

        return 1
    except FileNotFoundError:
         # Error already printed by run_docker_command
         return 1
    except Exception as e:
        print(f"\nAn unexpected error occurred during launch: {e}", file=sys.stderr)
        if check_container_exists(args.name):
             print(f"Attempting to stop/remove potentially problematic container '{args.name}' due to unexpected error...", file=sys.stderr)
             run_docker_command([DOCKER_EXEC_PATH, "stop", args.name], suppress_error=True, check=False)
             run_docker_command([DOCKER_EXEC_PATH, "rm", args.name], suppress_error=True, check=False)
        return 1


if __name__ == "__main__":
    sys.exit(main()) 