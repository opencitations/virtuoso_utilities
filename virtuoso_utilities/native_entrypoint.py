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
    MIN_DB_SIZE_BYTES_FOR_CHECKPOINT_REMAP,
    calculate_max_checkpoint_remap,
    calculate_max_query_mem,
    calculate_threading_config,
    get_default_memory,
    get_optimal_buffer_values,
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


def get_config_from_env():
    config = {}
    config["memory"] = os.environ.get(ENV_MEMORY) or get_default_memory()
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

    update_ini_memory_settings(
        ini_path=ini_path,
        data_dir_path=data_dir,
        number_of_buffers=config["number_of_buffers"],
        max_dirty_buffers=config["max_dirty_buffers"],
        dirs_allowed=",".join(dirs),
    )

    print(
        f"Info: Configured Virtuoso with NumberOfBuffers={config['number_of_buffers']}, "
        f"MaxDirtyBuffers={config['max_dirty_buffers']}"
    )


def set_virt_env_vars(config):
    threading = calculate_threading_config(config["parallel_threads"])
    os.environ["VIRT_Parameters_AsyncQueueMaxThreads"] = str(
        threading["async_queue_max_threads"]
    )
    os.environ["VIRT_Parameters_ThreadsPerQuery"] = str(threading["threads_per_query"])
    os.environ["VIRT_Parameters_MaxClientConnections"] = str(
        threading["max_client_connections"]
    )
    os.environ["VIRT_HTTPServer_ServerThreads"] = str(
        threading["max_client_connections"]
    )

    os.environ["VIRT_Parameters_AdjustVectorSize"] = "1"
    os.environ["VIRT_Parameters_MaxVectorSize"] = "1000000"

    max_query_mem = calculate_max_query_mem(
        config["memory"], config["number_of_buffers"]
    )
    if max_query_mem:
        os.environ["VIRT_Parameters_MaxQueryMem"] = max_query_mem

    os.environ["VIRT_Client_SQL_QUERY_TIMEOUT"] = "0"
    os.environ["VIRT_Client_SQL_TXN_TIMEOUT"] = "0"

    if config["estimated_db_size_gb"] > 0:
        estimated_size_bytes = int(config["estimated_db_size_gb"] * 1024**3)
        if estimated_size_bytes >= MIN_DB_SIZE_BYTES_FOR_CHECKPOINT_REMAP:
            max_checkpoint_remap = calculate_max_checkpoint_remap(estimated_size_bytes)
            os.environ["VIRT_Database_MaxCheckpointRemap"] = str(max_checkpoint_remap)
            os.environ["VIRT_TempDatabase_MaxCheckpointRemap"] = str(
                max_checkpoint_remap
            )

    print(
        f"Info: Threading config: AsyncQueueMaxThreads={threading['async_queue_max_threads']}, "
        f"ThreadsPerQuery={threading['threads_per_query']}, "
        f"MaxClientConnections={threading['max_client_connections']}"
    )


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
    os.execv(config["original_entrypoint"], [config["original_entrypoint"]] + remaining_args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
