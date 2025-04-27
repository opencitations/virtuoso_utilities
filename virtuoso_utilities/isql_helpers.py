"""
Helper functions for executing ISQL commands and scripts against Virtuoso,
handling both direct execution and execution via Docker.
"""
import argparse
import os
import shlex
import subprocess
import sys


def _run_subprocess(
    command: list[str] | str,
    use_shell: bool = False,
    capture: bool = False,
    encoding: str = 'utf-8'
) -> tuple[int, str, str]:
    """Internal helper to run a subprocess command."""
    try:
        process = subprocess.run(
            command,
            shell=use_shell,
            capture_output=capture,
            text=True,
            check=False,
            encoding=encoding
        )
        stdout = process.stdout.strip() if process.stdout else ""
        stderr = process.stderr.strip() if process.stderr else ""
        return process.returncode, stdout, stderr
    except Exception as e:
        print(f"Subprocess execution failed: {e}", file=sys.stderr)
        print(f"Command: {command}", file=sys.stderr)
        return -1, "", str(e) # Use -1 to indicate internal error

def run_isql_command(
    args: argparse.Namespace,
    sql_command: str | None = None,
    script_path: str | None = None,
    capture: bool = False,
    ignore_errors: bool = False
) -> tuple[bool, str, str]:
    """
    Executes a SQL command or script using the 'isql' utility, either directly
    or via 'docker exec'.

    Exactly one of `sql_command` or `script_path` must be provided.

    Args:
        args (argparse.Namespace): Parsed command-line arguments containing
                                   connection details and paths. Must have
                                   attributes: docker_container, host, port,
                                   user, password. If docker_container
                                   is set, must also have docker_path and
                                   docker_isql_path. Otherwise, must have
                                   isql_path.
        sql_command (str | None): The SQL command string to execute.
        script_path (str | None): The path to the SQL script file to execute.
        capture (bool): If True, capture stdout and stderr.
        ignore_errors (bool): If True, print errors but return True anyway.

    Returns:
        tuple: (success_status, stdout, stderr)
               success_status is True if the command/script ran without error
               (exit code 0) or if ignore_errors is True.
               stdout and stderr contain the respective outputs.

    Raises:
        ValueError: If neither or both sql_command and script_path are provided.
    """
    if not ((sql_command is None) ^ (script_path is None)):
        raise ValueError("Exactly one of sql_command or script_path must be provided.")

    command_to_run: list[str] | str = []
    use_shell = False
    effective_isql_path_for_error = ""
    command_description = ""

    if args.docker_container:
        if not hasattr(args, 'docker_path') or not args.docker_path:
            print("Error: 'docker_path' argument missing for Docker execution.", file=sys.stderr)
            return False, "", "'docker_path' argument missing"
        if not hasattr(args, 'docker_isql_path') or not args.docker_isql_path:
            print("Error: 'docker_isql_path' argument missing for Docker execution.", file=sys.stderr)
            return False, "", "'docker_isql_path' argument missing"

        effective_isql_path_for_error = f"'{args.docker_isql_path}' inside container '{args.docker_container}' via '{args.docker_path}'"

        exec_content = ""
        if sql_command:
            exec_content = sql_command
            command_description = "ISQL command (Docker)"
        else:
            command_description = "ISQL script (Docker)"
            if not os.path.exists(script_path):
                 print(f"Error: Script file not found at '{script_path}'", file=sys.stderr)
                 return False, "", f"Script file not found: {script_path}"
            print("Reading script content for docker exec...", file=sys.stderr)
            try:
                with open(script_path, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                exec_content = sql_content.replace('\n', ' ').strip()
            except Exception as e:
                print(f"Error reading SQL script file '{script_path}': {e}", file=sys.stderr)
                return False, "", str(e)

        command_to_run = [
            args.docker_path,
            'exec',
            args.docker_container,
            args.docker_isql_path,
            f"{args.host}:{args.port}",
            args.user,
            args.password,
            f"exec={exec_content}"
        ]

    else:
        if not hasattr(args, 'isql_path') or not args.isql_path:
             print("Error: 'isql_path' argument missing for non-Docker execution.", file=sys.stderr)
             return False, "", "'isql_path' argument missing"

        effective_isql_path_for_error = f"'{args.isql_path}' on host"

        if sql_command:
            command_description = "ISQL command (Local)"
            command_to_run = [
                args.isql_path,
                f"{args.host}:{args.port}",
                args.user,
                args.password,
                f"exec={sql_command}"
            ]
        else:
            command_description = "ISQL script (Local)"
            if not os.path.exists(script_path):
                print(f"Error: Script file not found at '{script_path}'", file=sys.stderr)
                return False, "", f"Script file not found: {script_path}"

            effective_isql_path_for_error += " using shell redirection"
            use_shell = True

            base_command_list = [
                 args.isql_path,
                 f"{args.host}:{args.port}",
                 args.user,
                 args.password,
                 f"< {shlex.quote(script_path)}" # Use shlex.quote for safety
             ]
            command_to_run = " ".join(base_command_list)

    try:
        returncode, stdout, stderr = _run_subprocess(command_to_run, use_shell=use_shell, capture=capture)

        if returncode != 0:
            # Handle specific FileNotFoundError after subprocess call
            if ("No such file or directory" in stderr or "not found" in stderr or returncode == 127):
                # Distinguish between primary executable not found vs other issues
                missing_cmd = args.docker_path if args.docker_container else args.isql_path
                print(f"Error: Command '{missing_cmd}' or related component not found.", file=sys.stderr)
                if args.docker_container:
                    print(f"Make sure '{args.docker_path}' is installed and in your PATH, and the container/isql path is correct.", file=sys.stderr)
                else:
                    print(f"Make sure Virtuoso client tools (containing '{args.isql_path}') are installed and in your PATH.", file=sys.stderr)
                    if use_shell:
                         print(f"Check shell environment if using local script execution.", file=sys.stderr)
                return False, stdout, f"Executable or shell component not found: {missing_cmd}"

            # Handle other ISQL/execution errors
            print(f"Error executing {command_description}.", file=sys.stderr)
            print(f"Command: {command_to_run}", file=sys.stderr)
            print(f"Return Code: {returncode}", file=sys.stderr)
            if stderr:
                print(f"Stderr: {stderr}", file=sys.stderr)
            if stdout:
                print(f"Stdout: {stdout}", file=sys.stderr)
            return ignore_errors, stdout, stderr
        return True, stdout, stderr
    except Exception as e:
        # Catch unexpected errors *around* the subprocess call if any
        print(f"An unexpected error occurred preparing or handling {command_description}: {e}", file=sys.stderr)
        print(f"Command context: {command_to_run}", file=sys.stderr)
        return False, "", str(e) 