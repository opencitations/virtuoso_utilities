#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dumps the entire content of an OpenLink Virtuoso quadstore using the official
dump_nquads stored procedure.

This script utilizes Virtuoso's optimized dump_nquads procedure for dumping
RDF data in N-Quads format, preserving Named Graph information.
The procedure is based on the official OpenLink Virtuoso documentation:
https://vos.openlinksw.com/owiki/wiki/VOS/VirtRDFDumpNQuad

Features:
- Uses official Virtuoso dump_nquads stored procedure
- Outputs in N-Quads format preserving Named Graph IRI information
- Supports both local Virtuoso and Docker-based execution
- Automatic compression (.nq.gz files)
- Configurable file size limits and starting file numbers
- Progress monitoring during export
- Excludes internal virtrdf: graphs automatically

The script first installs the necessary stored procedure, then calls it
to perform the actual dump operation producing compressed N-Quads files.
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time
from typing import List

from virtuoso_utilities.isql_helpers import run_isql_command

DEFAULT_VIRTUOSO_HOST = "localhost"
DEFAULT_VIRTUOSO_PORT = 1111
DEFAULT_VIRTUOSO_USER = "dba"
DEFAULT_ISQL_PATH_HOST = "isql"
DEFAULT_ISQL_PATH_DOCKER = "isql"
DEFAULT_DOCKER_PATH = "docker"
DEFAULT_OUTPUT_DIR = "./virtuoso_dump"
DEFAULT_FILE_LENGTH_LIMIT = 100000000  # 100MB per file
DEFAULT_START_FROM = 1
DEFAULT_COMPRESSION = 1  # Enable compression by default

DUMP_NQUADS_PROCEDURE = """
CREATE PROCEDURE dump_nquads 
  ( IN  dir                VARCHAR := 'dumps'
  , IN  start_from             INT := 1
  , IN  file_length_limit  INTEGER := 100000000
  , IN  comp                   INT := 1
  )
  {
    DECLARE  inx, ses_len  INT
  ; DECLARE  file_name     VARCHAR
  ; DECLARE  env, ses      ANY
  ;

  inx := start_from;
  SET isolation = 'uncommitted';
  env := vector (0,0,0);
  ses := string_output (10000000);
  FOR (SELECT * FROM (sparql define input:storage "" SELECT ?s ?p ?o ?g { GRAPH ?g { ?s ?p ?o } . FILTER ( ?g != virtrdf: ) } ) AS sub OPTION (loop)) DO
    {
      DECLARE EXIT HANDLER FOR SQLSTATE '22023' 
	{
	  GOTO next;
	};
      http_nquad (env, "s", "p", "o", "g", ses);
      ses_len := LENGTH (ses);
      IF (ses_len >= file_length_limit)
	{
	  file_name := sprintf ('%s/output%06d.nq', dir, inx);
	  string_to_file (file_name, ses, -2);
	  IF (comp)
	    {
	      gz_compress_file (file_name, file_name||'.gz');
	      file_delete (file_name);
	    }
	  inx := inx + 1;
	  env := vector (0,0,0);
	  ses := string_output (10000000);
	}
      next:;
    }
  IF (length (ses))
    {
      file_name := sprintf ('%s/output%06d.nq', dir, inx);
      string_to_file (file_name, ses, -2);
      IF (comp)
	{
	  gz_compress_file (file_name, file_name||'.gz');
	  file_delete (file_name);
	}
      inx := inx + 1;
      env := vector (0,0,0);
    }
}
;
"""


def create_output_directory(output_dir: str, use_docker: bool = False) -> bool:
    """
    Ensure the output directory exists. If running in Docker mode, skip creation on the host.
    
    Args:
        output_dir: Path to the output directory
        use_docker: True if running with --docker-container
        
    Returns:
        True if directory exists or was created successfully (or skipped in Docker mode), False otherwise
    """
    if use_docker:
        return True
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            print(f"Created output directory: {output_dir}")
        return True
    except Exception as e:
        print(f"Error creating output directory '{output_dir}': {e}", file=sys.stderr)
        return False


def install_dump_procedure(args: argparse.Namespace) -> bool:
    """
    Install the dump_nquads stored procedure in Virtuoso by saving it to a file and loading it with LOAD.
    If using Docker, copy the file into the container and LOAD it there.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        True if successful, False otherwise
    """
    print("Installing Virtuoso dump_nquads procedure via LOAD ...")
    try:
        if args.docker_container:
            with tempfile.NamedTemporaryFile("w", delete=False, suffix="_dump_nquads_procedure.sql", encoding="utf-8") as f:
                f.write(DUMP_NQUADS_PROCEDURE)
                host_tmp_path = f.name
            container_tmp_path = "/tmp/dump_nquads_procedure.sql"
            cp_cmd = [args.docker_path, "cp", host_tmp_path, f"{args.docker_container}:{container_tmp_path}"]
            cp_result = subprocess.run(cp_cmd, capture_output=True, text=True)
            if cp_result.returncode != 0:
                print(f"Error copying procedure file into container: {cp_result.stderr}", file=sys.stderr)
                os.unlink(host_tmp_path)
                return False
            load_command = f"LOAD '{container_tmp_path}';"
            success, stdout, stderr = run_isql_command(args, sql_command=load_command)
            rm_cmd = [args.docker_path, "exec", args.docker_container, "rm", "-f", container_tmp_path]
            subprocess.run(rm_cmd, capture_output=True)
            os.unlink(host_tmp_path)
        else:
            with tempfile.NamedTemporaryFile("w", delete=False, suffix="_dump_nquads_procedure.sql", encoding="utf-8") as f:
                f.write(DUMP_NQUADS_PROCEDURE)
                procedure_file = f.name
            load_command = f"LOAD '{procedure_file}';"
            success, stdout, stderr = run_isql_command(args, sql_command=load_command)
            os.unlink(procedure_file)
        if not success:
            print(f"Error installing dump_nquads procedure: {stderr}", file=sys.stderr)
            return False
        print("dump_nquads procedure installed successfully!")
        return True
    except Exception as e:
        print(f"Error writing or loading dump_nquads procedure: {e}", file=sys.stderr)
        return False



def dump_nquads(args: argparse.Namespace) -> bool:
    """
    Execute the dump_nquads procedure to dump all graphs.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        True if successful, False otherwise
    """
    print("Starting N-Quads dump using dump_nquads procedure...")
    
    compression_flag = 1 if args.compression else 0
    dump_command = f"dump_nquads('{args.output_dir}', {DEFAULT_START_FROM}, {args.file_length_limit}, {compression_flag});"
    
    print(f"Executing: {dump_command}")
    
    success, stdout, stderr = run_isql_command(args, sql_command=dump_command)
    
    if not success:
        print(f"Error executing dump_nquads: {stderr}", file=sys.stderr)
        return False
    
    print("dump_nquads procedure completed successfully!")
    return True


def list_output_files(output_dir: str, compressed: bool = True) -> List[str]:
    """
    List all the output files created in the dump directory.
    
    Args:
        output_dir: Output directory path
        compressed: Whether to look for compressed files
        
    Returns:
        List of output file paths
    """
    try:
        files = []
        if os.path.exists(output_dir):
            for filename in os.listdir(output_dir):
                if compressed and filename.endswith('.nq.gz'):
                    files.append(os.path.join(output_dir, filename))
                elif not compressed and filename.endswith('.nq'):
                    files.append(os.path.join(output_dir, filename))
        return sorted(files)
    except Exception as e:
        print(f"Error listing output files: {e}", file=sys.stderr)
        return []


def dump_quadstore(args: argparse.Namespace) -> bool:
    """
    Main function to dump the quadstore content using dump_nquads procedure.
    Args:
        args: Parsed command-line arguments
    Returns:
        True if successful, False otherwise
    """
    if not create_output_directory(args.output_dir, args.docker_container):
        return False
    if not install_dump_procedure(args):
        return False
    print(f"\nStep 1: Executing dump_nquads procedure...")
    success = dump_nquads(args)
    if success:
        output_files = list_output_files(args.output_dir, args.compression)
        print(f"\nDump completed successfully!")
        print(f"Total files created: {len(output_files)}")
        print(f"Output directory: {args.output_dir}")
        print(f"Output format: N-Quads ({'compressed' if args.compression else 'uncompressed'})")
        if output_files:
            print("\nCreated files:")
            total_size = 0
            for file_path in output_files:
                try:
                    size = os.path.getsize(file_path)
                    total_size += size
                    print(f"  {os.path.basename(file_path)} ({size:,} bytes)")
                except OSError:
                    print(f"  {os.path.basename(file_path)} (size unknown)")
            if total_size > 0:
                print(f"\nTotal size: {total_size:,} bytes ({total_size / (1024*1024):.2f} MB)")
    return success


def main():
    """
    Main function to parse arguments and orchestrate the quadstore dump.
    """
    parser = argparse.ArgumentParser(
        description="Dump the entire content of an OpenLink Virtuoso quadstore using the official dump_nquads procedure.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script uses Virtuoso's official dump_nquads stored procedure for optimal
performance and N-Quads output format. The procedure is documented at:
https://vos.openlinksw.com/owiki/wiki/VOS/VirtRDFDumpNQuad

The dump_nquads procedure automatically:
- Excludes internal virtrdf: graphs
- Preserves Named Graph information in N-Quads format
- Handles compression and file splitting
- Optimizes memory usage during export

Example usage:
  # Dump entire quadstore to compressed N-Quads files
  python dump_quadstore.py --password mypassword --output-dir ./dump

  # Dump with custom file size limit (50MB per file)
  python dump_quadstore.py --password mypassword --file-length-limit 50000000

  # Dump uncompressed files
  python dump_quadstore.py --password mypassword --no-compression

  # Dump using Docker
  python dump_quadstore.py --password mypassword --docker-container virtuoso \\
    --output-dir /dumps

Important Notes:
- Output files are in N-Quads format: output000001.nq.gz, output000002.nq.gz, etc.
- The output directory must be accessible by Virtuoso and listed in DirsAllowed
- When using Docker, ensure the output directory is mounted and accessible inside the container
- The script automatically installs the required dump_nquads stored procedure
"""
    )

    parser.add_argument("-H", "--host", default=DEFAULT_VIRTUOSO_HOST,
                        help=f"Virtuoso server host (Default: {DEFAULT_VIRTUOSO_HOST})")
    parser.add_argument("-P", "--port", type=int, default=DEFAULT_VIRTUOSO_PORT,
                        help=f"Virtuoso server port (Default: {DEFAULT_VIRTUOSO_PORT})")
    parser.add_argument("-u", "--user", default=DEFAULT_VIRTUOSO_USER,
                        help=f"Virtuoso username (Default: {DEFAULT_VIRTUOSO_USER})")
    parser.add_argument("-k", "--password", default="dba",
                        help="Virtuoso password (Default: dba)")

    # Output parameters
    parser.add_argument("-o", "--output-dir", default=DEFAULT_OUTPUT_DIR,
                        help=f"Output directory for N-Quads files (Default: {DEFAULT_OUTPUT_DIR}). Must be accessible by Virtuoso and listed in DirsAllowed.")
    parser.add_argument("--file-length-limit", type=int, default=DEFAULT_FILE_LENGTH_LIMIT,
                        help=f"Maximum length of dump files in bytes (Default: {DEFAULT_FILE_LENGTH_LIMIT:,})")
    parser.add_argument("--no-compression", action="store_true",
                        help="Disable gzip compression (files will be .nq instead of .nq.gz)")

    docker_group = parser.add_argument_group('Docker Options')
    docker_group.add_argument("--docker-container",
                            help="Name or ID of the running Virtuoso Docker container")

    args = parser.parse_args()

    if args.file_length_limit <= 0:
        print("Error: --file-length-limit must be greater than 0", file=sys.stderr)
        sys.exit(1)

    args.compression = not args.no_compression

    args.isql_path = DEFAULT_ISQL_PATH_HOST
    args.docker_isql_path = DEFAULT_ISQL_PATH_DOCKER
    args.docker_path = DEFAULT_DOCKER_PATH

    args.output_dir = os.path.abspath(args.output_dir)

    print("-" * 70)
    print("Virtuoso N-Quads Dump Configuration:")
    print(f"  Host: {args.host}:{args.port}")
    print(f"  User: {args.user}")
    print(f"  Mode: {'Docker' if args.docker_container else 'Local'}")
    if args.docker_container:
        print(f"  Container: {args.docker_container}")
    print(f"  Output Dir: {args.output_dir}")
    print(f"  File Size Limit: {args.file_length_limit:,} bytes")
    print(f"  Compression: {'Enabled (.nq.gz)' if args.compression else 'Disabled (.nq)'}")
    print(f"  Method: Official dump_nquads stored procedure")
    print(f"  Output Format: N-Quads (preserves Named Graph information)")
    print("-" * 70)

    print("\nIMPORTANT: Ensure the output directory is:")
    print("  1. Accessible by the Virtuoso server process")
    print("  2. Listed in the 'DirsAllowed' parameter in virtuoso.ini")
    if args.docker_container:
        print("  3. Properly mounted and accessible inside the Docker container")
    print()

    print("Testing Virtuoso connection...")
    success, stdout, stderr = run_isql_command(args, sql_command="SELECT 'Connection test' as test;")
    if not success:
        print(f"Error: Could not connect to Virtuoso: {stderr}", file=sys.stderr)
        sys.exit(1)
    print("Connection successful!")

    start_time = time.time()
    success = dump_quadstore(args)
    end_time = time.time()
    
    if success:
        duration = end_time - start_time
        print(f"\nDump completed in {duration:.2f} seconds")
        sys.exit(0)
    else:
        print("Dump failed", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main() 