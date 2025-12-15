#!/usr/bin/env python3
"""
Virtuoso native mode entrypoint.

Wrapper script that configures virtuoso.ini from environment variables,
then execs the original Virtuoso entrypoint. Designed for use as a Docker
container entrypoint.

Usage in Dockerfile:
    FROM openlink/virtuoso-opensource-7:latest
    RUN pip install virtuoso-utilities
    ENTRYPOINT ["virtuoso-native-launch"]

Environment variables:
    VIRTUOSO_MEMORY: Memory limit (e.g., "8g"). Default: 2/3 of available RAM
    VIRTUOSO_DBA_PASSWORD: DBA password. Also accepts DBA_PASSWORD for compatibility
    VIRTUOSO_ESTIMATED_DB_SIZE_GB: Estimated DB size for MaxCheckpointRemap
    VIRTUOSO_PARALLEL_THREADS: CPU cores for query parallelization
    VIRTUOSO_ENABLE_WRITE_PERMISSIONS: Enable SPARQL write ("true"/"1")
    VIRTUOSO_NUMBER_OF_BUFFERS: Override automatic buffer calculation
    VIRTUOSO_MAX_DIRTY_BUFFERS: Override automatic dirty buffer calculation
    VIRTUOSO_DATA_DIR: Data directory path
    VIRTUOSO_EXTRA_DIRS_ALLOWED: Additional DirsAllowed paths (comma-separated)
    VIRTUOSO_ORIGINAL_ENTRYPOINT: Original entrypoint to exec
"""

import os
import sys

from virtuoso_utilities.launch_virtuoso import (
    DEFAULT_CONTAINER_DATA_DIR,
    DEFAULT_DIRS_ALLOWED,
    bytes_to_docker_mem_str,
    calculate_max_query_mem,
    calculate_threading_config,
    get_default_memory,
    get_optimal_buffer_values,
    get_virt_env_vars,
    grant_write_permissions,
    update_ini_memory_settings,
    wait_for_virtuoso_ready,
)

ENV_MEMORY = "VIRTUOSO_MEMORY"
ENV_DBA_PASSWORD = "VIRTUOSO_DBA_PASSWORD"
ENV_ESTIMATED_DB_SIZE_GB = "VIRTUOSO_ESTIMATED_DB_SIZE_GB"
ENV_PARALLEL_THREADS = "VIRTUOSO_PARALLEL_THREADS"
ENV_ENABLE_WRITE_PERMISSIONS = "VIRTUOSO_ENABLE_WRITE_PERMISSIONS"
ENV_NUMBER_OF_BUFFERS = "VIRTUOSO_NUMBER_OF_BUFFERS"
ENV_MAX_DIRTY_BUFFERS = "VIRTUOSO_MAX_DIRTY_BUFFERS"
ENV_DATA_DIR = "VIRTUOSO_DATA_DIR"
ENV_EXTRA_DIRS_ALLOWED = "VIRTUOSO_EXTRA_DIRS_ALLOWED"
ENV_ORIGINAL_ENTRYPOINT = "VIRTUOSO_ORIGINAL_ENTRYPOINT"

DEFAULT_ORIGINAL_ENTRYPOINT = "/virtuoso-entrypoint.sh"

CGROUP_V2_MEMORY_MAX = "/sys/fs/cgroup/memory.max"
CGROUP_V1_MEMORY_LIMIT = "/sys/fs/cgroup/memory/memory.limit_in_bytes"


def get_container_memory_limit():
    for path in [CGROUP_V2_MEMORY_MAX, CGROUP_V1_MEMORY_LIMIT]:
        try:
            with open(path) as f:
                value = f.read().strip()
                if value != "max":
                    return int(value)
        except (FileNotFoundError, ValueError, PermissionError):
            continue
    return None


def get_native_default_memory():
    container_limit = get_container_memory_limit()
    if container_limit:
        default_mem = max(int(container_limit * (2 / 3)), 1 * 1024**3)
        return bytes_to_docker_mem_str(default_mem)
    return get_default_memory()


def get_config_from_env():
    config = {}
    config["memory"] = os.environ.get(ENV_MEMORY) or get_native_default_memory()
    config["dba_password"] = (
        os.environ.get(ENV_DBA_PASSWORD) or os.environ.get("DBA_PASSWORD", "dba")
    )
    config["estimated_db_size_gb"] = float(
        os.environ.get(ENV_ESTIMATED_DB_SIZE_GB, "0")
    )
    threads_str = os.environ.get(ENV_PARALLEL_THREADS)
    config["parallel_threads"] = int(threads_str) if threads_str else None
    config["enable_write_permissions"] = os.environ.get(
        ENV_ENABLE_WRITE_PERMISSIONS, ""
    ).lower() in ("1", "true", "yes")
    buffers_str = os.environ.get(ENV_NUMBER_OF_BUFFERS)
    config["number_of_buffers"] = int(buffers_str) if buffers_str else None
    dirty_str = os.environ.get(ENV_MAX_DIRTY_BUFFERS)
    config["max_dirty_buffers"] = int(dirty_str) if dirty_str else None
    config["data_dir"] = os.environ.get(ENV_DATA_DIR, DEFAULT_CONTAINER_DATA_DIR)
    config["extra_dirs_allowed"] = os.environ.get(ENV_EXTRA_DIRS_ALLOWED, "")
    config["original_entrypoint"] = os.environ.get(
        ENV_ORIGINAL_ENTRYPOINT, DEFAULT_ORIGINAL_ENTRYPOINT
    )
    return config


def configure_virtuoso(config):
    data_dir = config["data_dir"]
    ini_path = os.path.join(data_dir, "virtuoso.ini")

    if config["number_of_buffers"] is None or config["max_dirty_buffers"] is None:
        num_buffers, max_dirty = get_optimal_buffer_values(config["memory"])
        if config["number_of_buffers"] is None:
            config["number_of_buffers"] = num_buffers
        if config["max_dirty_buffers"] is None:
            config["max_dirty_buffers"] = max_dirty

    dirs = DEFAULT_DIRS_ALLOWED.copy()
    dirs.add(data_dir)
    if config["extra_dirs_allowed"]:
        extra = set(
            d.strip() for d in config["extra_dirs_allowed"].split(",") if d.strip()
        )
        dirs.update(extra)

    threading = calculate_threading_config(config["parallel_threads"])
    max_query_mem_value = calculate_max_query_mem(
        config["memory"], config["number_of_buffers"]
    )
    update_ini_memory_settings(
        ini_path=ini_path,
        data_dir_path=data_dir,
        number_of_buffers=config["number_of_buffers"],
        max_dirty_buffers=config["max_dirty_buffers"],
        dirs_allowed=",".join(dirs),
        async_queue_max_threads=threading["async_queue_max_threads"],
        threads_per_query=threading["threads_per_query"],
        max_client_connections=threading["max_client_connections"],
        adjust_vector_size=0,
        vector_size=1000,
        checkpoint_interval=1,
        max_query_mem=max_query_mem_value,
        http_server_threads=threading["max_client_connections"],
    )

    print(
        f"Info: Configured Virtuoso with NumberOfBuffers={config['number_of_buffers']}, "
        f"MaxDirtyBuffers={config['max_dirty_buffers']}"
    )


def set_virt_env_vars(config):
    env_vars = get_virt_env_vars(
        memory=config["memory"],
        number_of_buffers=config["number_of_buffers"],
        max_dirty_buffers=config["max_dirty_buffers"],
        parallel_threads=config["parallel_threads"],
        estimated_db_size_gb=config["estimated_db_size_gb"],
    )
    for key, value in env_vars.items():
        os.environ[key] = value


def apply_write_permissions_async(dba_password):
    pid = os.fork()
    if pid == 0:
        try:
            if wait_for_virtuoso_ready(dba_password):
                grant_write_permissions(dba_password)
        except Exception as e:
            print(f"Error applying write permissions: {e}", file=sys.stderr)
        os._exit(0)


def main():
    config = get_config_from_env()

    print("=" * 70)
    print("Virtuoso Native Mode Configuration")
    print(f"  Memory: {config['memory']}")
    print(f"  Data Dir: {config['data_dir']}")
    print(f"  Write Permissions: {config['enable_write_permissions']}")
    print(f"  Original Entrypoint: {config['original_entrypoint']}")
    print("=" * 70)

    configure_virtuoso(config)
    set_virt_env_vars(config)

    if config["enable_write_permissions"]:
        apply_write_permissions_async(config["dba_password"])

    remaining_args = sys.argv[1:] if len(sys.argv) > 1 else []
    print(f"Info: Executing original entrypoint: {config['original_entrypoint']}")
    sys.stdout.flush()
    sys.stderr.flush()
    os.execve(config["original_entrypoint"], [config["original_entrypoint"]] + remaining_args, os.environ)

    return 0


if __name__ == "__main__":
    sys.exit(main())