# Virtuoso Utilities

A collection of Python utilities for interacting with OpenLink Virtuoso.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Virtuoso Docker Launcher](#virtuoso-docker-launcher-launch_virtuosopy)
    - [Memory-Based Configuration](#memory-based-configuration)
  - [Parallel Bulk Loader](#parallel-bulk-loader-bulk_load_parallelpy)

## Features

*   **Parallel Bulk Loading:** The `bulk_load_parallel.py` script orchestrates Virtuoso's built-in bulk loading procedures (`ld_dir`/`ld_dir_all`, `rdf_loader_run`) to efficiently load RDF files into a Virtuoso instance. It manages parallel execution of `rdf_loader_run` using multiple `isql` processes.
*   **Docker Support:** Seamlessly integrates with Virtuoso running in a Docker container by executing `isql` commands via `docker exec`.
*   **Flexible Configuration:** Allows customization of Virtuoso connection details, number of parallel loaders, file patterns, target graphs, and paths to `isql` and `docker` executables.
*   **Virtuoso Docker Launcher:** The `launch_virtuoso.py` script provides a convenient way to launch a Virtuoso database using Docker with customizable configuration parameters, including memory settings and volume mounts.

## Installation

This project uses [Poetry](https://python-poetry.org/) for dependency management.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/opencitations/virtuoso_utilities.git
    cd virtuoso_utilities
    ```
2.  **Install dependencies:**
    ```bash
    poetry install
    ```

You also need the Virtuoso `isql` client installed on your host system or accessible within your Docker container.

## Usage

### Virtuoso Docker Launcher (`launch_virtuoso.py`)

The script provides a convenient way to launch a Virtuoso database using Docker with various configurable parameters.

**Basic Usage:**

```bash
poetry run python virtuoso_utilities/launch_virtuoso.py
```

This launches a Virtuoso container with default settings.

**Customized Usage:**

```bash
poetry run python virtuoso_utilities/launch_virtuoso.py \
    --name my-virtuoso \
    --http-port 8891 \
    --isql-port 1112 \
    --data-dir ./my-virtuoso-data \
    --dba-password mySafePassword \
    --mount-volume /path/on/host/with/rdf:/rdf-data-in-container \
    --detach \
    --wait-ready
```

**Arguments:**

Use `poetry run python virtuoso_utilities/launch_virtuoso.py --help` to see all available options:

*   `--name`: Name for the Docker container (Default: `virtuoso`).
*   `--image`: Docker image to use (Default: `openlink/virtuoso-opensource-7`).
*   `--version`: Version tag for the Virtuoso Docker image (Default: `latest`).
*   `--http-port`: HTTP port to expose Virtuoso on (Default: `8890`).
*   `--isql-port`: ISQL port to expose Virtuoso on (Default: `1111`).
*   `--data-dir`: Host directory to mount as Virtuoso data directory (Default: `./virtuoso-data`).
*   `--container-data-dir`: Path inside container where data will be stored (Default: `/opt/virtuoso-opensource/database`).
*   `--mount-volume HOST_PATH:CONTAINER_PATH`: Mount an additional host directory into the container. Format: `/path/on/host:/path/in/container`. Can be specified multiple times. Useful for making data files (e.g., RDF) available to the bulk loader.
*   `--memory`: Memory limit for the container (e.g., `2g`, `4g`). It defaults to approx. 2/3 of host RAM. You can manually override this calculated default.
*   `--cpu-limit`: CPU limit for the container, 0 means no limit (Default: `0`).
*   `--dba-password`: Password for the Virtuoso dba user (Default: `dba`).
*   `--max-rows`: ResultSet maximum number of rows (Default: `100000`).
*   `--max-dirty-buffers`: Maximum dirty buffers before checkpoint. Auto-calculated based on the final `--memory` value (either the default or the one you provided).
*   `--number-of-buffers`: Number of buffers. Auto-calculated based on the final `--memory` value (either the default or the one you provided).
*   `--wait-ready`: Wait until Virtuoso is ready to accept connections.
*   `--detach`: Run container in detached mode.
*   `--force-remove`: Force removal of existing container with the same name.

#### Memory-Based Configuration

> **Important Note on Docker Resource Limits:**
> By default, Docker Desktop runs with limited resources, especially RAM. Before allocating significant memory to the Virtuoso container using the `--memory` flag (e.g., more than a few gigabytes), ensure that Docker itself is configured to have access to at least that amount of RAM. You can usually adjust Docker's resource limits in its settings/preferences.
> If the Virtuoso container requests more memory than Docker can provide, the container may fail to start or crash unexpectedly (often with exit code 137).

The script aims to simplify memory configuration based on Virtuoso best practices:

1.  **Container Memory Limit (`--memory`):** The script automatically detects your host's total RAM and sets the *default* value for `--memory` to approximately 2/3 of that total (e.g., if you have 128GiB RAM, the default might become `85g`). This follows the Virtuoso guideline of allocating a significant portion of system RAM to the database process when dealing with large datasets. You can always explicitly set `--memory` to any value you prefer, overriding the automatic calculation.

2.  **Virtuoso Internal Buffers (`--number-of-buffers`, `--max-dirty-buffers`):** Based on the *final* memory limit set for the container (whether automatically calculated or manually specified via `--memory`), the script automatically calculates optimal values for Virtuoso's internal `NumberOfBuffers` and `MaxDirtyBuffers`. It uses the formula recommended in the [OpenLink Virtuoso Performance Tuning documentation](https://community.openlinksw.com/t/performance-tuning-virtuoso-for-rdf-queries-and-other-use/1692):
    ```
    NumberOfBuffers = (ContainerMemoryLimitInBytes * 0.66) / 8000
    MaxDirtyBuffers = NumberOfBuffers * 0.75
    ```
    This ensures Virtuoso's internal memory usage is tuned relative to the memory allocated to its container. You can still manually override these calculated values using the `--number-of-buffers` and `--max-dirty-buffers` arguments if needed for specific tuning requirements.

For more detailed information on Virtuoso performance tuning, refer to the [official OpenLink documentation](https://community.openlinksw.com/t/performance-tuning-virtuoso-for-rdf-queries-and-other-use/1692).

### Parallel Bulk Loader (`bulk_load_parallel.py`)

The script uses Virtuoso's standard bulk loading procedure to register and load RDF files from a specified directory in parallel. For detailed information on the underlying Virtuoso mechanism, refer to the [Virtuoso Bulk Loading RDF Source Files documentation](https://vos.openlinksw.com/owiki/wiki/VOS/VirtBulkRDFLoader).

**Important Prerequisites:**

*   **Server Access:** The directory containing the RDF files (`-d` or `--data-directory`) **must** be accessible by the Virtuoso server process itself.
*   **`DirsAllowed` Configuration:** This directory path **must** be listed in the `DirsAllowed` parameter within the `virtuoso.ini` file used by the server. If using Docker, the path specified by `--docker-data-mount-path` must be listed in the `DirsAllowed` of the container's `virtuoso.ini`.

**Basic Usage (Host Virtuoso):**

```bash
poetry run python virtuoso_utilities/bulk_load_parallel.py \
    -d /path/accessible/by/virtuoso/server \
    -k <your_virtuoso_password>
```

**Customized Usage (Host Virtuoso):**

```bash
poetry run python virtuoso_utilities/bulk_load_parallel.py \
    -d /path/accessible/by/virtuoso/server \
    -k <your_virtuoso_password> \
    --host <virtuoso_host> \
    --port <virtuoso_port> \
    --user <virtuoso_user> \
    --graph-uri <default_target_graph> \
    --num-processes 4 \
    --file-pattern "*.ttl" \
    --recursive
```

**Usage with Docker:**

```bash
# Example: Launch Virtuoso first using launch_virtuoso.py
poetry run python virtuoso_utilities/launch_virtuoso.py \
    --name my-virtuoso-loader \
    --isql-port 1112 \
    --data-dir ./virtuoso-loader-data \
    --mount-volume /home/user/my_rdf_data:/rdf_mount_in_container

# Then run the bulk loader
poetry run python virtuoso_utilities/bulk_load_parallel.py \
    -d /home/user/my_rdf_data \ # Path on HOST
    -k <your_virtuoso_password> \
    --port 1112 \ # Use the mapped ISQL port
    --docker-container my-virtuoso-loader \
    --docker-data-mount-path /rdf_mount_in_container \ # Path INSIDE CONTAINER
    --docker-isql-path /opt/virtuoso-opensource/bin/isql \ # Typical path in container
    --file-pattern "*.nq.gz" \
```

**Arguments:**

Use `poetry run python virtuoso_utilities/bulk_load_parallel.py --help` to see all available options:

*   `-d`, `--data-directory`: **Required.** Directory containing RDF files. **Crucially, this path must be accessible by the Virtuoso server process and listed in its `DirsAllowed` configuration.** When using Docker, provide the *host* path here; the script uses `--docker-data-mount-path` to tell Virtuoso where to find it *inside* the container.
*   `-k`, `--password`: **Required.** Virtuoso `dba` user password.
*   `-H`, `--host`: Virtuoso server host (Default: `localhost`).
*   `-P`, `--port`: Virtuoso server ISQL port (Default: `1111`). Use the *host* port if mapped via Docker.
*   `-u`, `--user`: Virtuoso username (Default: `dba`).
*   `-n`, `--num-processes`: Number of parallel `rdf_loader_run()` processes to launch (Default: calculated based on CPU cores / 2.5).
*   `-f`, `--file-pattern`: File pattern for Virtuoso's `ld_dir`/`ld_dir_all` function (e.g., `*.nq`, `*.ttl.gz`, Default: `*.*`). Virtuoso determines which files to load based on this pattern within the specified directory.
*   `-g`, `--graph-uri`: Default target graph URI if no `.graph` file is present alongside the data file (Default: `http://localhost:8890/DAV`). Quad formats (NQuads, TriG) typically ignore this as they define graphs internally.
*   `--recursive`: Use `ld_dir_all()` to load recursively from subdirectories (Default: uses `ld_dir()`).
*   `--log-enable`: `log_enable` mode for `rdf_loader_run()`. `2` (default) disables triggers for speed; `3` keeps triggers enabled.
*   `--checkpoint-interval`: Interval (seconds) for checkpointing after load (Default: `60`).
*   `--scheduler-interval`: Interval (seconds) for the scheduler after load (Default: `10`).
*   `--isql-path`: Path to `isql` on the host system (Default: `isql`). Used only if not in Docker mode.
*   `--docker-container`: Name or ID of the running Virtuoso Docker container.
*   `--docker-data-mount-path`: **Required with `--docker-container`**. The absolute path *inside* the container where the host data directory (`-d`) is mounted. This path must be listed in the container's `virtuoso.ini` `DirsAllowed`.
*   `--docker-isql-path`: Path to the `isql` executable *inside* the Docker container (Default: `isql`, often needs to be `/opt/virtuoso-opensource/bin/isql`).
*   `--docker-path`: Path to the `docker` executable on the host system (Default: `docker`).