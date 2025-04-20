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
    
    Uses recommended values from Virtuoso documentation for different memory sizes.
    
    Args:
        memory_limit: Memory limit string in Docker format (e.g., "2g", "4096m")
        
    Returns:
        Tuple[int, int]: Optimal values for NumberOfBuffers and MaxDirtyBuffers
    """
    try:
        memory_bytes = parse_memory_value(memory_limit)
        memory_gb = memory_bytes / (1024 * 1024 * 1024)
        
        # Use Virtuoso recommended values for different memory sizes
        if memory_gb >= 64:
            return 5450000, 4000000
        elif memory_gb >= 48:
            return 4000000, 3000000
        elif memory_gb >= 32:
            return 2720000, 2000000
        elif memory_gb >= 16:
            return 1360000, 1000000
        elif memory_gb >= 8:
            return 680000, 500000
        elif memory_gb >= 4:
            return 340000, 250000
        else:  # 2GB or less
            return 170000, 130000
    except Exception as e:
        print(f"Warning: Error calculating buffer values: {e}. Using default values.")
        # Default values for approximately 2GB RAM if calculation fails
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
        help="Memory limit for the container (e.g., 2g, 4g, max 16g)"
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
    
    memory_bytes = parse_memory_value(args.memory)
    memory_gb = memory_bytes / (1024 * 1024 * 1024)
    if memory_gb > 16:
        print(f"Warning: Memory value {args.memory} exceeds the maximum supported limit of 16g.")
        print("Setting memory to 16g to prevent container crashes.")
        args.memory = "16g"
        memory_gb = 16
    
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


def main() -> int:
    """
    Main function to launch Virtuoso with Docker.
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    args = parse_arguments()
    
    if not check_docker_installed():
        print("Error: Docker is not installed or not in PATH", file=sys.stderr)
        return 1
    
    if check_container_exists(args.name):
        if args.force_remove:
            print(f"Container '{args.name}' already exists. Removing...")
            if not remove_container(args.name):
                print(f"Error: Failed to remove existing container '{args.name}'", file=sys.stderr)
                return 1
        else:
            print(f"Error: Container '{args.name}' already exists. Use --force-remove to replace it.",
                  file=sys.stderr)
            return 1
    
    docker_cmd = build_docker_run_command(args)
    print(f"Launching Virtuoso container: {' '.join(docker_cmd)}")
    
    try:
        process = subprocess.run(docker_cmd, check=True)
        
        if args.detach and args.wait_ready:
            if not wait_for_virtuoso_ready(args.name):
                return 1
        
        print(f"""
Virtuoso launched successfully!
- Data Directory Host: {os.path.abspath(args.data_dir)}
- Data Directory Container: {args.container_data_dir}
- Web interface: http://localhost:{args.http_port}/conductor
- ISQL: isql localhost:{args.isql_port} dba {args.dba_password}
- Container name: {args.name}
""")
        if args.extra_volumes:
            print("Additional mounted volumes:")
            for volume_spec in args.extra_volumes:
                 if ':' in volume_spec:
                    host_path, container_path = volume_spec.split(':', 1)
                    print(f"  - Host: {os.path.abspath(host_path)} -> Container: {container_path}")
        return 0
        
    except subprocess.SubprocessError as e:
        print(f"Error launching Virtuoso container: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main()) 