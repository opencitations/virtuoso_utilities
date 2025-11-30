#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Performs sequential bulk loading of RDF N-Quads Gzipped files (`.nq.gz`)
into OpenLink Virtuoso using the official `ld_dir`/`ld_dir_all` and
`rdf_loader_run` method.

Registers files matching *.nq.gz using ld_dir/ld_dir_all into DB.DBA.load_list,
then runs a single `rdf_loader_run()` process to load the registered files.

IMPORTANT:
- Only files with the extension `.nq.gz` will be processed.
- The data directory specified ('-d' or '--data-directory') MUST be
  accessible by the Virtuoso server process itself.
- This directory path MUST be listed in the 'DirsAllowed' parameter
  within the Virtuoso INI file (e.g., virtuoso.ini).
- When using Docker, data-directory is the path INSIDE the container.
  Files will be accessed and loaded from within the container.

Reference:
- https://vos.openlinksw.com/owiki/wiki/VOS/VirtBulkRDFLoader
"""

import argparse
import glob
import logging
import os
import sys

from virtuoso_utilities.isql_helpers import run_isql_command

logger = logging.getLogger(__name__)

DEFAULT_VIRTUOSO_HOST = "localhost"
DEFAULT_VIRTUOSO_PORT = 1111
DEFAULT_VIRTUOSO_USER = "dba"

ISQL_PATH_HOST = "isql"
ISQL_PATH_DOCKER = "isql"
DOCKER_PATH = "docker"
CHECKPOINT_INTERVAL = 60
SCHEDULER_INTERVAL = 10

NQ_GZ_PATTERN = '*.nq.gz'


def find_nquads_files_local(directory, recursive=False):
    """
    Find all N-Quads Gzipped files (`*.nq.gz`) in a directory on local filesystem.
    Returns a list of file paths.
    """
    pattern = NQ_GZ_PATTERN
    if recursive:
        matches = []
        for root, _, _ in os.walk(directory):
            path_pattern = os.path.join(root, pattern)
            matches.extend(glob.glob(path_pattern))
        return matches
    else:
        path_pattern = os.path.join(directory, pattern)
        return glob.glob(path_pattern)


def bulk_load(
    data_directory: str,
    password: str,
    host: str = "localhost",
    port: int = 1111,
    user: str = "dba",
    recursive: bool = False,
    docker_container: str = None,
    isql_path: str = ISQL_PATH_HOST,
    docker_isql_path: str = ISQL_PATH_DOCKER,
    docker_path: str = DOCKER_PATH,
    container_data_directory: str = None,
    log_level: str = "ERROR"
) -> None:
    """
    Perform Virtuoso bulk loading of N-Quads files.

    This function can be imported and called programmatically, avoiding subprocess overhead.

    Args:
        data_directory: Path to directory containing .nq.gz files (host path for file operations)
        password: Virtuoso DBA password
        host: Virtuoso server host
        port: Virtuoso server port
        user: Virtuoso username
        recursive: Use ld_dir_all instead of ld_dir for recursive loading
        docker_container: Docker container name (for ISQL commands only)
        isql_path: Path to isql binary (local)
        docker_isql_path: Path to isql binary inside Docker container
        docker_path: Path to docker binary
        container_data_directory: Path INSIDE container where Virtuoso accesses files (if different from data_directory)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default: ERROR

    Raises:
        RuntimeError: If bulk load fails
    """

    logging.basicConfig(level=getattr(logging, log_level.upper()), format='%(levelname)s: %(message)s', force=True)

    args = argparse.Namespace()
    args.data_directory = data_directory
    args.host = host
    args.port = port
    args.user = user
    args.password = password
    args.recursive = recursive
    args.docker_container = docker_container
    args.isql_path = isql_path
    args.docker_isql_path = docker_isql_path
    args.docker_path = docker_path
    args.container_data_directory = container_data_directory

    data_dir = args.data_directory
    container_dir = args.container_data_directory if args.container_data_directory else data_dir

    files = find_nquads_files_local(data_dir, args.recursive)

    if not files:
        logger.warning(f"No files matching '{NQ_GZ_PATTERN}' found in '{data_dir}'.")
        return

    # Clean up any leftover entries from previous bulk loads
    cleanup_sql = "DELETE FROM DB.DBA.load_list;"
    run_isql_command(args, sql_command=cleanup_sql)

    ld_function = "ld_dir_all" if args.recursive else "ld_dir"

    container_dir_sql_escaped = container_dir.replace("'", "''")
    file_pattern_sql_escaped = NQ_GZ_PATTERN.replace("'", "''")

    # Use empty string to let Virtuoso read graph URIs from nquads (4th field)
    register_sql = f"{ld_function}('{container_dir_sql_escaped}', '{file_pattern_sql_escaped}', '');"
    success_reg, _, stderr_reg = run_isql_command(args, sql_command=register_sql)

    if not success_reg or "Unable to list files" in stderr_reg or "FA020" in stderr_reg:
        raise RuntimeError(f"Failed to register files using {ld_function}.\nError: {stderr_reg}")

    loader_sql = "rdf_loader_run();"
    success_load, _, stderr_load = run_isql_command(args, sql_command=loader_sql)

    if not success_load:
        raise RuntimeError(f"rdf_loader_run() failed.\nError: {stderr_load}")

    stats_sql = "SELECT COUNT(*) AS total_files, SUM(CASE WHEN ll_state = 2 THEN 1 ELSE 0 END) AS loaded, SUM(CASE WHEN ll_state <> 2 OR ll_error IS NOT NULL THEN 1 ELSE 0 END) AS issues FROM DB.DBA.load_list;"
    success_stats, stdout_stats, _ = run_isql_command(args, sql_command=stats_sql)

    if success_stats:
        lines = stdout_stats.strip().splitlines()
        for i, line in enumerate(lines):
            if i > 3 and not line.endswith("Rows.") and "INTEGER" not in line and "VARCHAR" not in line:
                parts = line.split()
                if len(parts) >= 3 and parts[0].isdigit():
                    total_files = int(parts[0])
                    loaded_files = int(parts[1]) if parts[1] != "NULL" else 0
                    issues = int(parts[2]) if parts[2] != "NULL" else 0

                    if total_files != loaded_files or issues != 0:
                        failed_sql = "SELECT ll_file FROM DB.DBA.load_list WHERE ll_state <> 2 OR ll_error IS NOT NULL;"
                        success_failed, stdout_failed, _ = run_isql_command(args, sql_command=failed_sql)
                        failed_files = []
                        if success_failed:
                            for line in stdout_failed.strip().splitlines():
                                line = line.strip()
                                if line and not line.startswith("ll_file") and not line.endswith("Rows.") and line != "VARCHAR":
                                    failed_files.append(line)
                        raise RuntimeError(
                            f"Bulk load failed: {issues} file(s) had issues.\n"
                            f"Failed files:\n" + "\n".join(f"  - {f}" for f in failed_files)
                        )
                    break

    delete_loaded_sql = "DELETE FROM DB.DBA.load_list WHERE ll_state = 2;"
    success_delete, _, stderr_delete = run_isql_command(args, sql_command=delete_loaded_sql)
    if not success_delete:
        raise RuntimeError(f"Failed to clean up load_list table.\nError: {stderr_delete}")

    cleanup_sql = f"log_enable(3, 1); checkpoint; checkpoint_interval({CHECKPOINT_INTERVAL}); scheduler_interval({SCHEDULER_INTERVAL});"
    success_final, _, stderr_final = run_isql_command(args, sql_command=cleanup_sql)
    if not success_final:
        raise RuntimeError(f"Failed to run final checkpoint.\nError details: {stderr_final}")


def main():  # pragma: no cover
    """
    CLI entry point that parses arguments and calls bulk_load().
    """
    parser = argparse.ArgumentParser(
        description=f"Sequential N-Quads Gzipped (`{NQ_GZ_PATTERN}`) bulk loader for OpenLink Virtuoso using ld_dir/rdf_loader_run.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Example usage:
  # Load all *.nq.gz files from /data/rdf (local mode)
  python bulk_load.py -d /data/rdf -k mypassword

  # Load *.nq.gz recursively using Docker, files at /database/data in container
  python bulk_load.py -d /database/data -k mypassword --recursive \
    --docker-container virtuoso_container

IMPORTANT:
- Only files with the extension `.nq.gz` will be loaded.
- The data directory (-d) must be accessible by the Virtuoso process
  and listed in the 'DirsAllowed' setting in virtuoso.ini.
- When using Docker mode, data-directory is the path INSIDE the container.
  Files are accessed and loaded directly inside the container.
"""
    )

    parser.add_argument("-d", "--data-directory", required=True,
                        help="Path to the N-Quads Gzipped (`.nq.gz`) files. When using Docker, this must be the path INSIDE the container.")
    parser.add_argument("-H", "--host", default=DEFAULT_VIRTUOSO_HOST,
                        help=f"Virtuoso server host (Default: {DEFAULT_VIRTUOSO_HOST}).")
    parser.add_argument("-P", "--port", type=int, default=DEFAULT_VIRTUOSO_PORT,
                        help=f"Virtuoso server port (Default: {DEFAULT_VIRTUOSO_PORT}).")
    parser.add_argument("-u", "--user", default=DEFAULT_VIRTUOSO_USER,
                        help=f"Virtuoso username (Default: {DEFAULT_VIRTUOSO_USER}).")
    parser.add_argument("-k", "--password", required=True,
                        help="Virtuoso password.")
    parser.add_argument("--recursive", action='store_true',
                        help="Load files recursively from subdirectories (uses ld_dir_all).")
    parser.add_argument("--log-level", default="ERROR", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level (Default: ERROR).")

    docker_group = parser.add_argument_group('Docker Options')
    docker_group.add_argument("--docker-container",
                        help="Name or ID of the running Virtuoso Docker container. If provided, 'isql' will be run via 'docker exec'.")

    args = parser.parse_args()

    try:
        bulk_load(
            data_directory=args.data_directory,
            password=args.password, 
            host=args.host,
            port=args.port,
            user=args.user,
            recursive=args.recursive,
            docker_container=args.docker_container,
            log_level=args.log_level
        )
        sys.exit(0)
    except RuntimeError as e:
        logger.error(str(e))
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()