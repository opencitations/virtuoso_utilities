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

import psutil

DEFAULT_WAIT_TIMEOUT = 120
DOCKER_EXEC_PATH = "docker"
DOCKER_ISQL_PATH_INSIDE_CONTAINER = "isql"

from virtuoso_utilities.isql_helpers import run_isql_command


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


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments for Virtuoso Docker launcher.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Launch a Virtuoso database using Docker",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # --- Calculate default memory based on host RAM (2/3) ---
    default_memory_str = "2g" # Fallback default
    if psutil:
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
        "--image", 
        default="openlink/virtuoso-opensource-7@sha256:e07868a3db9090400332eaa8ee694b8cf9bf7eebc26db6bbdc3bb92fd30ed010",
        help="Docker image to use for Virtuoso"
    )
    parser.add_argument(
        "--version", 
        default="latest",
        help="Version tag for the Virtuoso Docker image"
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
        "--container-data-dir", 
        default="/opt/virtuoso-opensource/database",
        help="Path inside container where data will be stored"
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
        "--max-rows", 
        type=int, 
        default=100000,
        help="ResultSet maximum number of rows"
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
        "--force-remove", 
        action="store_true",
        help="Force removal of existing container with the same name"
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
    container_data_dir_path = args.container_data_dir if args.container_data_dir.startswith('/') else '/' + args.container_data_dir
    cmd.extend(["-v", f"{host_data_dir_abs}:{container_data_dir_path}"])

    # Start with default Virtuoso paths
    default_dirs_allowed = {".", "../vad", "/usr/share/proj", "../virtuoso_input"}
    paths_to_allow_in_container = default_dirs_allowed
    paths_to_allow_in_container.add(container_data_dir_path)
    
    if args.extra_volumes:
        for volume_spec in args.extra_volumes:
            if ':' not in volume_spec:
                print(f"Warning: Invalid format for --mount-volume '{volume_spec}'. Skipping. "
                      "Expected format: HOST_PATH:CONTAINER_PATH", file=sys.stderr)
                continue
            host_path, container_path = volume_spec.split(':', 1)
            abs_host_path = os.path.abspath(host_path)
            if not os.path.exists(abs_host_path):
                 print(f"Warning: Host path '{abs_host_path}' for volume mount does not exist. "
                       "Docker will create it as a directory.", file=sys.stderr)
            # Ensure container path is absolute-like
            container_path_abs = container_path if container_path.startswith('/') else '/' + container_path
            cmd.extend(["-v", f"{abs_host_path}:{container_path_abs}"])
            paths_to_allow_in_container.add(container_path_abs)
    
    # Combine defaults with specified paths, ensure uniqueness and sort
    unique_paths_list = sorted(list(paths_to_allow_in_container))
    dirs_allowed_value = ",".join(unique_paths_list)

    cmd.extend(["--memory", args.memory])
    if args.cpu_limit > 0:
        cmd.extend(["--cpus", str(args.cpu_limit)])
    
    env_vars = {
        "DBA_PASSWORD": args.dba_password,
        "VIRT_Parameters_ResultSetMaxRows": str(args.max_rows),
        "VIRT_Parameters_MaxDirtyBuffers": str(args.max_dirty_buffers),
        "VIRT_Parameters_NumberOfBuffers": str(args.number_of_buffers),
        "VIRT_Parameters_DirsAllowed": dirs_allowed_value,
        "VIRT_SPARQL_DefaultQuery": "SELECT (COUNT(*) AS ?quadCount) WHERE { GRAPH ?g { ?s ?p ?o } }"
    }
    
    for key, value in env_vars.items():
        cmd.extend(["-e", f"{key}={value}"])
    
    if args.detach:
        cmd.append("-d")
    
    # Ensure --rm is added if not running detached
    if not args.detach:
        cmd.insert(2, "--rm") # Insert after "docker run"
    
    # Append image name, adding version tag only if no SHA digest is present
    image_name = args.image
    if '@sha256:' not in image_name:
        image_name = f"{image_name}:{args.version}"
    cmd.append(image_name)
    
    return cmd, unique_paths_list


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
- Data Directory Container: {args.container_data_dir}
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