# Virtuoso Utilities

A collection of Python utilities for interacting with OpenLink Virtuoso.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Virtuoso Docker Launcher](#virtuoso-docker-launcher-launch_virtuosopy)
    - [Memory-Based Configuration](#memory-based-configuration)
  - [Sequential Bulk Loader](#sequential-bulk-loader-bulk_loadpy)
    - [Performance Note: Why only `.nq.gz`?](#performance-note-why-only-nqgz)

## Features

*   **Sequential Bulk Loading:** The `bulk_load.py` script provides a sequential method to load N-Quads Gzipped files (`*.nq.gz`) into Virtuoso. It finds files within a specified directory (either locally or inside a Docker container) and uses the official Virtuoso `ld_dir`/`ld_dir_all` and `rdf_loader_run` methods for efficient loading.
*   **Docker Support:** Seamlessly integrates with Virtuoso running in a Docker container by executing `isql` commands via `docker exec`.
*   **Flexible Configuration:** Allows customization of Virtuoso connection details, file patterns, and paths to `isql` and `docker` executables.
*   **Virtuoso Docker Launcher:** The `launch_virtuoso.py` script provides a convenient way to launch a Virtuoso database using Docker with customizable configuration parameters, including automatic memory tuning and volume mounts. It also sets the `DirsAllowed` parameter in the container based on mounted volumes.

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
*   `--image`: Docker image to use (Default: `openlink/virtuoso-opensource-7@sha256:e07868a3db9090400332eaa8ee694b8cf9bf7eebc26db6bbdc3bb92fd30ed010`).
*   `--version`: Version tag for the Virtuoso Docker image (Default: `latest`).
*   `--http-port`: HTTP port to expose Virtuoso on (Default: `8890`).
*   `--isql-port`: ISQL port to expose Virtuoso on (Default: `1111`).
*   `--data-dir`: Host directory to mount as Virtuoso data directory (Default: `./virtuoso-data`). This directory is used to automatically calculate `MaxCheckpointRemap` if its size exceeds 1 GiB.
*   `--container-data-dir`: Path inside container where data will be stored (Default: `/opt/virtuoso-opensource/database`).
*   `--mount-volume HOST_PATH:CONTAINER_PATH`: Mount an additional host directory into the container. Format: `/path/on/host:/path/in/container`. Can be specified multiple times. Useful for making data files (e.g., RDF) available to the bulk loader. Paths mounted here are automatically added to the `DirsAllowed` setting in the container's environment.
*   `--memory`: Memory limit for the container (e.g., `2g`, `4g`). It defaults to approx. 2/3 of host RAM if `psutil` is installed, otherwise `2g`. You can manually override this calculated default.
*   `--cpu-limit`: CPU limit for the container, 0 means no limit (Default: `0`).
*   `--dba-password`: Password for the Virtuoso dba user (Default: `dba`).
*   `--max-rows`: ResultSet maximum number of rows (Default: `100000`).
*   `--max-dirty-buffers`: Maximum dirty buffers before checkpoint. Auto-calculated based on the final `--memory` value (either the default or the one you provided).
*   `--number-of-buffers`: Number of buffers. Auto-calculated based on the final `--memory` value (either the default or the one you provided).
*   `--estimated-db-size-gb`: Estimated database size in GB. If provided and >= 1 GB, `MaxCheckpointRemap` will be preconfigured via environment variables rather than measuring existing data. Useful for new deployments when you can estimate the final database size.
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
    These values are passed as environment variables (`VIRT_Parameters_NumberOfBuffers`, `VIRT_Parameters_MaxDirtyBuffers`) to the container. You can still manually override these calculated values using the `--number-of-buffers` and `--max-dirty-buffers` arguments if needed for specific tuning requirements.

3.  **Allowed Directories (`DirsAllowed`):** The script automatically constructs the `VIRT_Parameters_DirsAllowed` environment variable passed to the container. It includes the container data directory (`--container-data-dir`) and any paths specified via `--mount-volume`, along with Virtuoso's default required paths. This ensures that Virtuoso has permission to access the mounted data directories.

**Automatic `MaxCheckpointRemap` Configuration:**

For large databases (> 1 GiB), Virtuoso recommends tuning the `MaxCheckpointRemap` parameter. This script offers two methods to configure this important parameter:

1. **For existing databases (default behavior):**
   * Before starting the container, the script checks for an existing `virtuoso.ini` file within the host directory specified by `--data-dir`.
   * If the `virtuoso.ini` file is found and the total size of the `--data-dir` exceeds 1 GiB, the script calculates the recommended `MaxCheckpointRemap` value (1/4th of the total size in 8K pages).
   * It then reads the `virtuoso.ini` file and **directly modifies** the `MaxCheckpointRemap` value under both the `[Database]` and `[TempDatabase]` sections if the calculated value differs from the existing one.
   * The modified `virtuoso.ini` is saved back to the host directory.
   * The container then starts, reading the updated configuration from the file.

2. **For new deployments with known expected size (`--estimated-db-size-gb`):**
   * When starting a new Virtuoso instance where you can estimate the final database size, you can use the `--estimated-db-size-gb` parameter.
   * If the estimated size is >= 1 GB, the script calculates the appropriate `MaxCheckpointRemap` value (1/4th of the estimated size in 8K pages).
   * This value is then passed directly to the container via environment variables (`VIRT_Database_MaxCheckpointRemap` and `VIRT_TempDatabase_MaxCheckpointRemap`).
   * No direct file modification is needed, as Virtuoso will use these environment variables when initializing.
   * Example: `--estimated-db-size-gb 100` for an expected 100 GB database.

This automation simplifies tuning `MaxCheckpointRemap` based on the actual database size.

For more detailed information on Virtuoso performance tuning, refer to the [official OpenLink documentation](https://docs.openlinksw.com/virtuoso/rdfperformancetuning/).

### Sequential Bulk Loader (`bulk_load.py`)

This script offers a sequential method to load N-Quads Gzipped files (`*.nq.gz`) into a Virtuoso instance using the standard Virtuoso bulk loading procedure (`ld_dir`/`ld_dir_all` followed by `rdf_loader_run`).

#### Performance Note: Why only `.nq.gz`?

This script *only* processes files ending in `.nq.gz`. This restriction is intentional and provides significant performance advantages:

*   **Avoiding Artificial Throttling:** By reading compressed data, the load on Virtuoso's internal processing is more balanced. Loading many uncompressed files can sometimes overwhelm Virtuoso's internal mechanisms, causing it to introduce artificial delays to manage the workload. Using compressed files mitigates this effect.

Therefore, for optimal bulk loading performance with Virtuoso's `ld_dir`/`rdf_loader_run` mechanism, using `.nq.gz` files is strongly recommended.

**How it works:** It first registers files found in the specified directory using the `ld_dir` (or `ld_dir_all` for recursive loading) ISQL function, adding them to the `DB.DBA.load_list` queue. Then, it executes the `rdf_loader_run()` ISQL function once to process this queue sequentially. Progress and errors can be monitored by querying `DB.DBA.load_list`.

**Important Prerequisites:**

*   **Server Access & `DirsAllowed` Configuration:** The directory specified by `-d` (`--data-directory`) **must** be accessible by the Virtuoso server process itself. Crucially, this path **must** be listed in the `DirsAllowed` parameter within the Virtuoso INI file (or configured via environment variables if using the `launch_virtuoso.py` script).
    *   **When using Docker (`--docker-container`)**: `-d` specifies the **absolute path *inside* the container** (e.g., `/rdf_mount_in_container`). Ensure this path corresponds to a mounted volume and is included in the container\'s `DirsAllowed` configuration. Using `launch_virtuoso.py` with `--mount-volume` handles this automatically.
    *   **When *not* using Docker**: `-d` specifies the **path on the host system** accessible by the Virtuoso process. Ensure this host path is listed in the server\'s `virtuoso.ini` `DirsAllowed`.

**Basic Usage (Host Virtuoso):**

```bash
poetry run python virtuoso_utilities/bulk_load.py \\\\
    -d /path/accessible/by/virtuoso/server \\\\
    -k <your_virtuoso_password>
```

**Customized Usage (Host Virtuoso):**

```bash
poetry run python virtuoso_utilities/bulk_load.py \\\\
    -d /path/accessible/by/virtuoso/server \\\\
    -k <your_virtuoso_password> \\\\
    --host <virtuoso_host> \\\\
    --port <virtuoso_port> \\\\
    --user <virtuoso_user> \\\\
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

# Then run the bulk loader (note: no --file-pattern needed)
poetry run python virtuoso_utilities/bulk_load.py \
    -d /rdf_mount_in_container \\ # Path INSIDE CONTAINER
    -k <your_virtuoso_password> \\
    --port 1112 \\ # Use the mapped ISQL port
    --docker-container my-virtuoso-loader \\
    --docker-isql-path /opt/virtuoso-opensource/bin/isql \\ # Typical path in container
    --recursive
```

**Arguments:**

Use `poetry run python virtuoso_utilities/bulk_load.py --help` to see all available options:

*   `-d`, `--data-directory`: **Required.** Path where the script will search for N-Quads Gzipped (`.nq.gz`) files to register using `ld_dir` or `ld_dir_all`. **Meaning depends on context:**
    *   If using Docker (`--docker-container`): This must be the **absolute path inside the container** (e.g., `/rdf_mount_in_container`) accessible by Virtuoso.
    *   If *not* using Docker: This must be the **path on the host system** accessible by the Virtuoso server process.
    *   In either case, this path **must** be listed in the relevant `DirsAllowed` setting.
*   `-k`, `--password`: **Required.** Virtuoso `dba` user password.
*   `-H`, `--host`: Virtuoso server host (Default: `localhost`).
*   `-P`, `--port`: Virtuoso server ISQL port (Default: `1111`). Use the *host* port if mapped via Docker.
*   `-u`, `--user`: Virtuoso username (Default: `dba`).
*   `--recursive`: Search for `.nq.gz` files recursively within the data directory (uses `ld_dir_all` instead of `ld_dir`).
*   `--checkpoint-interval`: Interval (seconds) to set for Virtuoso checkpointing *after* the bulk load completes (Default: `60`).
*   `--scheduler-interval`: Interval (seconds) to set for the Virtuoso scheduler *after* the bulk load completes (Default: `10`).
*   `--isql-path`: Path to `isql` on the host system (Default: `isql`). Used only if not in Docker mode.
*   `--docker-container`: Name or ID of the running Virtuoso Docker container. If specified, `isql` commands are run via `docker exec`, and `-d` refers to the path *inside* the container.
*   `--docker-isql-path`: Path to the `isql` executable *inside* the Docker container (Default: `isql`, often needs to be `/opt/virtuoso-opensource/bin/isql`).
*   `--docker-path`: Path to the `docker` executable on the host system (Default: `docker`).