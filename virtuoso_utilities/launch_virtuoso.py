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
        print(f"Warning: Error calculating buffer values: {e}. Using default values.")
        # Default values for approximately 2GB RAM if calculation fails
        return 170000, 130000


def update_dirs_allowed(ini_path: str, container_paths_to_add: List[str]):
    """
    Updates the DirsAllowed setting in a virtuoso.ini file.

    Reads the specified .ini file, finds the DirsAllowed line under the
    [Parameters] section, adds the provided absolute container paths
    (ensuring uniqueness), and writes the modified file back.

    Args:
        ini_path: Absolute path to the virtuoso.ini file on the host.
        container_paths_to_add: List of absolute paths inside the container
                                that should be allowed.
    """
    if not os.path.exists(ini_path):
        print(f"Info: '{ini_path}' not found. Skipping DirsAllowed update. "
              "Virtuoso will use default settings on first start.", file=sys.stderr)
        return

    try:
        with open(ini_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        new_lines = []
        found_dirs_allowed = False
        modified = False

        for line in lines:
            stripped_line = line.strip()
            if stripped_line.lower().startswith('dirsallowed'):
                found_dirs_allowed = True
                try:
                    key, value_str = stripped_line.split('=', 1)
                    existing_paths_str = value_str.strip()
                    existing_paths = [p.strip() for p in existing_paths_str.split(',') if p.strip()]

                    all_paths = set(existing_paths)
                    added_new = False
                    for new_path in container_paths_to_add:
                        if new_path not in all_paths:
                            all_paths.add(new_path)
                            added_new = True

                    if added_new:
                        original_key = key.strip()
                        new_value_str = ", ".join(sorted(list(all_paths)))
                        new_line = f"{original_key} = {new_value_str}\n"
                        new_lines.append(new_line)
                        modified = True
                        print(f"Updated DirsAllowed in '{ini_path}': {new_value_str}")
                    else:
                        new_lines.append(line)

                except ValueError:
                    print(f"Warning: Could not parse DirsAllowed line: {line.strip()}", file=sys.stderr)
                    new_lines.append(line)
            else:
                new_lines.append(line)

        if not found_dirs_allowed:
             print(f"Warning: 'DirsAllowed' key not found in '{ini_path}'. Cannot update.", file=sys.stderr)
             return 

        if modified:
            with open(ini_path, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)
            print(f"Successfully updated '{ini_path}'.")

    except IOError as e:
        print(f"Error reading/writing '{ini_path}': {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred while updating DirsAllowed in '{ini_path}': {e}", file=sys.stderr)


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
    
    parser.add_argument(
        "--name", 
        default="virtuoso",
        help="Name for the Docker container"
    )
    parser.add_argument(
        "--image", 
        default="openlink/virtuoso-opensource-7",
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
    
    default_memory = "2g"
    parser.add_argument(
        "--memory", 
        default=default_memory,
        help="Memory limit for the container (e.g., 2g, 4g)"
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
    
    args, _ = parser.parse_known_args()
    
    optimal_number_of_buffers, optimal_max_dirty_buffers = get_optimal_buffer_values(args.memory)
    
    parser.add_argument(
        "--max-dirty-buffers", 
        type=int, 
        default=optimal_max_dirty_buffers,
        help="Maximum dirty buffers before checkpoint (auto-calculated based on --memory value)"
    )
    parser.add_argument(
        "--number-of-buffers", 
        type=int, 
        default=optimal_number_of_buffers,
        help="Number of buffers (auto-calculated based on --memory value)"
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


def build_docker_run_command(args: argparse.Namespace) -> List[str]:
    """
    Build the Docker run command based on provided arguments.
    
    Args:
        args: Command-line arguments
        
    Returns:
        List[str]: Command parts for subprocess.run
    """
    os.makedirs(args.data_dir, exist_ok=True)
    
    cmd = ["docker", "run"]
    
    cmd.extend(["--name", args.name])
    
    cmd.extend(["-p", f"{args.http_port}:8890"])
    cmd.extend(["-p", f"{args.isql_port}:1111"])
    
    cmd.extend(["-v", f"{os.path.abspath(args.data_dir)}:{args.container_data_dir}"])
    
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
            # Ensure the host path exists or Docker might create it with root ownership
            # os.makedirs(abs_host_path, exist_ok=True) # Optionally create if needed
            cmd.extend(["-v", f"{abs_host_path}:{container_path}"])
    
    cmd.extend(["--memory", args.memory])
    if args.cpu_limit > 0:
        cmd.extend(["--cpus", str(args.cpu_limit)])
    
    env_vars = {
        "DBA_PASSWORD": args.dba_password,
        "VIRT_Parameters_ResultSetMaxRows": str(args.max_rows),
        "VIRT_Parameters_MaxDirtyBuffers": str(args.max_dirty_buffers),
        "VIRT_Parameters_NumberOfBuffers": str(args.number_of_buffers)
    }
    
    for key, value in env_vars.items():
        cmd.extend(["-e", f"{key}={value}"])
    
    if args.detach:
        cmd.append("-d")
    
    cmd.append(f"{args.image}:{args.version}")
    
    return cmd


def wait_for_virtuoso_ready(container_name: str, timeout: int = 120) -> bool:
    """
    Wait until Virtuoso is ready to accept connections.
    
    Args:
        container_name: Name of the Virtuoso container
        timeout: Maximum time to wait in seconds
        
    Returns:
        bool: True if Virtuoso is ready, False if timeout occurred
    """
    print(f"Waiting for Virtuoso to become ready (timeout: {timeout}s)...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            result = subprocess.run(
                ["docker", "logs", container_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            if "Server online at 1111 (pid" in result.stdout:
                print("Virtuoso is ready!")
                return True
                
            time.sleep(2)
        except subprocess.SubprocessError:
            pass
    
    print("Timeout waiting for Virtuoso to become ready")
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
    Main function to launch Virtuoso with Docker, handling first-time setup
    for DirsAllowed modification.
    """
    args = parse_arguments()

    if not check_docker_installed():
        return 1

    host_data_dir_abs = os.path.abspath(args.data_dir)
    virtuoso_ini_host_path = os.path.join(host_data_dir_abs, 'virtuoso.ini')
    is_first_run = not os.path.exists(virtuoso_ini_host_path)

    container_exists = check_container_exists(args.name)

    if container_exists:
        if args.force_remove:
            print(f"Container '{args.name}' already exists. Removing...")
            if not remove_container(args.name):
                print(f"Error: Failed to remove existing container '{args.name}'", file=sys.stderr)
                return 1
            container_exists = False
        else:
            print(f"Error: Container '{args.name}' already exists. Use --force-remove to replace it.",
                  file=sys.stderr)
            return 1

    paths_to_allow_in_container = []
    if args.container_data_dir:
        paths_to_allow_in_container.append(args.container_data_dir if args.container_data_dir.startswith('/') else '/' + args.container_data_dir)
    if args.extra_volumes:
        for volume_spec in args.extra_volumes:
            if ':' in volume_spec:
                _, container_path = volume_spec.split(':', 1)
                paths_to_allow_in_container.append(container_path if container_path.startswith('/') else '/' + container_path)
    unique_paths_to_allow = list(set(paths_to_allow_in_container))

    try:
        if is_first_run:
            print(f"'{virtuoso_ini_host_path}' not found. Performing first-time setup...")

            initial_docker_cmd = build_docker_run_command(args)
            if "-d" not in initial_docker_cmd:
                 try:
                     image_index = initial_docker_cmd.index(f"{args.image}:{args.version}")
                     initial_docker_cmd.insert(image_index, "-d")
                 except ValueError:
                      print("Warning: Could not precisely locate image name to insert -d flag. Appending instead.", file=sys.stderr)
                      initial_docker_cmd.append("-d")

            print("Starting initial container run in background for initialization...")
            run_docker_command(initial_docker_cmd)

            if not wait_for_virtuoso_ready(args.name, timeout=120):
                 print("Warning: Initial readiness check timed out. Proceeding with stop/modify/start, but ini file might not be ready.", file=sys.stderr)
            else:
                 print("Initial instance appears ready.")
                 time.sleep(5)

            print(f"Stopping container '{args.name}' to apply configuration changes...")
            run_docker_command(["docker", "stop", args.name])
            time.sleep(2)

            if unique_paths_to_allow:
                print(f"Attempting to update DirsAllowed in newly created '{virtuoso_ini_host_path}'...")
                update_dirs_allowed(virtuoso_ini_host_path, unique_paths_to_allow)
            else:
                 print("No extra volumes specified, skipping DirsAllowed update.")

            print(f"Restarting container '{args.name}' with updated configuration...")
            run_docker_command(["docker", "start", args.name])

            if args.wait_ready:
                 print("Waiting for final Virtuoso readiness...")
                 if not wait_for_virtuoso_ready(args.name):
                     print("Warning: Container restarted but final readiness check timed out.", file=sys.stderr)
                     pass

        else:
            print(f"'{virtuoso_ini_host_path}' found. Performing standard setup...")
            if unique_paths_to_allow:
                 update_dirs_allowed(virtuoso_ini_host_path, unique_paths_to_allow)

            docker_cmd = build_docker_run_command(args)
            run_docker_command(docker_cmd)

            if args.detach and args.wait_ready:
                if not wait_for_virtuoso_ready(args.name):
                    print("Warning: Container started but readiness check timed out.", file=sys.stderr)
                    pass


        print(f"""
Virtuoso launched successfully!
- Data Directory Host: {host_data_dir_abs}
- Data Directory Container: {args.container_data_dir}
- Web interface: http://localhost:{args.http_port}/conductor
- ISQL: isql localhost:{args.isql_port} dba <password>
- Container name: {args.name}
""")
        if args.extra_volumes:
            print("Additional mounted volumes:")
            for volume_spec in args.extra_volumes:
                 if ':' in volume_spec:
                    host_path, container_path = volume_spec.split(':', 1)
                    print(f"  - Host: {os.path.abspath(host_path)} -> Container: {container_path}")
        if unique_paths_to_allow:
             print(f"DirsAllowed in container expected to include: {', '.join(unique_paths_to_allow)}")
        return 0

    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Virtuoso launch failed.", file=sys.stderr)
        if check_container_exists(args.name):
             print(f"Attempting to stop potentially problematic container '{args.name}' ...", file=sys.stderr)
             run_docker_command(["docker", "stop", args.name], suppress_error=True, check=False)
        return 1
    except Exception as e:
        print(f"An unexpected error occurred during launch: {e}", file=sys.stderr)
        if check_container_exists(args.name):
             print(f"Attempting to stop potentially problematic container '{args.name}' due to unexpected error...", file=sys.stderr)
             run_docker_command(["docker", "stop", args.name], suppress_error=True, check=False)
        return 1


if __name__ == "__main__":
    sys.exit(main()) 