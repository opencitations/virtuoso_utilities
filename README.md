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
  - [Quadstore Dump Utility](#quadstore-dump-utility-dump_quadstorepy)
  - [Full-Text Index Rebuilder](#full-text-index-rebuilder-rebuild_fulltext_indexpy)

## Features

*   **Sequential Bulk Loading:** The [`bulk_load.py`](https://github.com/opencitations/virtuoso_utilities/blob/master/virtuoso_utilities/bulk_load.py) script provides a sequential method to load N-Quads Gzipped files (`*.nq.gz`) into Virtuoso. It finds files within a specified directory (either locally or inside a Docker container) and uses the official Virtuoso `ld_dir`/`ld_dir_all` and `rdf_loader_run` methods for efficient loading.
*   **Quadstore Export:** The [`dump_quadstore.py`](https://github.com/opencitations/virtuoso_utilities/blob/master/virtuoso_utilities/dump_quadstore.py) script provides a comprehensive solution to export the entire content of a Virtuoso quadstore using the official Virtuoso `dump_nquads` stored procedure. This procedure is specifically designed for N-Quads export and is documented in the [official OpenLink Virtuoso VOS documentation](https://vos.openlinksw.com/owiki/wiki/VOS/VirtRDFDumpNQuad).
*   **Docker Support:** Seamlessly integrates with Virtuoso running in a Docker container by executing `isql` commands via `docker exec`.
*   **Flexible Configuration:** Allows customization of Virtuoso connection details, file patterns, and paths to `isql` and `docker` executables.
*   **Virtuoso Docker Launcher:** The [`launch_virtuoso.py`](https://github.com/opencitations/virtuoso_utilities/blob/master/virtuoso_utilities/launch_virtuoso.py) script provides a convenient way to launch a Virtuoso database using Docker with customizable configuration parameters, including automatic memory tuning and volume mounts. It also sets the `DirsAllowed` parameter in the container based on mounted volumes.
*   **Full-Text Index Rebuilder:** The [`rebuild_fulltext_index.py`](https://github.com/opencitations/virtuoso_utilities/blob/master/virtuoso_utilities/rebuild_fulltext_index.py) script provides a utility to rebuild the Virtuoso full-text index, which is used for optimal querying of RDF object values using the `bif:contains` function in SPARQL queries.

## Installation

This package can be installed in two ways: globally using `pipx` for easy command-line access, or locally using Poetry for development.

### Global Installation with pipx (Recommended for end users)

[pipx](https://pypa.github.io/pipx/) is the recommended way to install Python CLI applications globally. It creates isolated environments for each package, avoiding dependency conflicts.

1.  **Install pipx** (if not already installed):
    ```bash
    # On Ubuntu/Debian
    sudo apt install pipx
    
    # On macOS
    brew install pipx
    
    # Or using pip
    pip install --user pipx
    pipx ensurepath
    ```

2.  **Install virtuoso-utilities globally**:
    ```bash
    # Install from PyPI
    pipx install virtuoso-utilities
    ```

3.  **Use the global commands**:
    ```bash
    # Launch Virtuoso with Docker
    virtuoso-launch --help
    
    # Bulk load data
    virtuoso-bulk-load --help
    
    # Dump quadstore
    virtuoso-dump --help
    
    # Rebuild full-text index
    virtuoso-rebuild-index --help
    ```

### Local Development Installation with Poetry

For development or if you prefer to use Poetry:

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/opencitations/virtuoso_utilities.git
    cd virtuoso_utilities
    ```
2.  **Install dependencies:**
    ```bash
    poetry install
    ```

3.  **Run scripts with Poetry:**
    ```bash
    poetry run python virtuoso_utilities/launch_virtuoso.py --help
    ```

**Prerequisites:**

You also need the Virtuoso `isql` client installed on your host system or accessible within your Docker container.

## Usage

### Virtuoso Docker Launcher ([`launch_virtuoso.py`](https://github.com/opencitations/virtuoso_utilities/blob/master/virtuoso_utilities/launch_virtuoso.py))

The script provides a convenient way to launch a Virtuoso database using Docker with various configurable parameters.

**Basic Usage:**

```bash
# With pipx (global installation)
virtuoso-launch

# With Poetry (development)
poetry run python virtuoso_utilities/launch_virtuoso.py
```

This launches a Virtuoso container with default settings.

**Customized Usage:**

```bash
# With pipx (global installation)
virtuoso-launch \
    --name my-virtuoso \
    --http-port 8891 \
    --isql-port 1112 \
    --data-dir ./my-virtuoso-data \
    --dba-password mySafePassword \
    --mount-volume /path/on/host/with/rdf:/rdf-data-in-container \
    --detach \
    --wait-ready

# With Poetry (development)
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

Use `virtuoso-launch --help` (or `poetry run python virtuoso_utilities/launch_virtuoso.py --help`) to see all available options:

*   `--name`: Name for the Docker container (Default: `virtuoso`).
*   `--http-port`: HTTP port to expose Virtuoso on (Default: `8890`).
*   `--isql-port`: ISQL port to expose Virtuoso on (Default: `1111`).
*   `--data-dir`: Host directory to mount as Virtuoso data directory (Default: `./virtuoso-data`). This directory is used to automatically calculate `MaxCheckpointRemap` if its size exceeds 1 GiB.
*   `--mount-volume HOST_PATH:CONTAINER_PATH`: Mount an additional host directory into the container. Format: `/path/on/host:/path/in/container`. Can be specified multiple times. Useful for making data files (e.g., RDF) available to the bulk loader. Paths mounted here are automatically added to the `DirsAllowed` setting in the container's environment.
*   `--memory`: Memory limit for the container (e.g., `2g`, `4g`). It defaults to approx. 2/3 of host RAM if `psutil` is installed, otherwise `2g`. You can manually override this calculated default.
*   `--cpu-limit`: CPU limit for the container, 0 means no limit (Default: `0`).
*   `--dba-password`: Password for the Virtuoso dba user (Default: `dba`).
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

3.  **Allowed Directories (`DirsAllowed`):** The script automatically constructs the `VIRT_Parameters_DirsAllowed` environment variable passed to the container. It includes the container data directory (hardcoded to `/opt/virtuoso-opensource/database`) and any paths specified via `--mount-volume`, along with Virtuoso's default required paths. This ensures that Virtuoso has permission to access the mounted data directories.

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

### Sequential Bulk Loader ([`bulk_load.py`](https://github.com/opencitations/virtuoso_utilities/blob/master/virtuoso_utilities/bulk_load.py))

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
# With pipx (global installation)
virtuoso-bulk-load \
    -d /path/accessible/by/virtuoso/server \
    -k <your_virtuoso_password>

# With Poetry (development)
poetry run python virtuoso_utilities/bulk_load.py \
    -d /path/accessible/by/virtuoso/server \
    -k <your_virtuoso_password>
```

**Customized Usage (Host Virtuoso):**

```bash
# With pipx (global installation)
virtuoso-bulk-load \
    -d /path/accessible/by/virtuoso/server \
    -k <your_virtuoso_password> \
    --host <virtuoso_host> \
    --port <virtuoso_port> \
    --user <virtuoso_user> \
    --recursive

# With Poetry (development)
poetry run python virtuoso_utilities/bulk_load.py \
    -d /path/accessible/by/virtuoso/server \
    -k <your_virtuoso_password> \
    --host <virtuoso_host> \
    --port <virtuoso_port> \
    --user <virtuoso_user> \
    --recursive
```

**Usage with Docker:**

```bash
# Example: Launch Virtuoso first (with pipx)
virtuoso-launch \
    --name my-virtuoso-loader \
    --isql-port 1112 \
    --data-dir ./virtuoso-loader-data \
    --mount-volume /home/user/my_rdf_data:/rdf_mount_in_container

# Then run the bulk loader (with pipx)
virtuoso-bulk-load \
    -d /rdf_mount_in_container \
    -k <your_virtuoso_password> \
    --port 1112 \
    --docker-container my-virtuoso-loader \
    --docker-isql-path /opt/virtuoso-opensource/bin/isql \
    --recursive
```

**Arguments:**

Use `virtuoso-bulk-load --help` (or `poetry run python virtuoso_utilities/bulk_load.py --help`) to see all available options:

*   `-d`, `--data-directory`: **Required.** Path where the script will search for N-Quads Gzipped (`.nq.gz`) files to register using `ld_dir` or `ld_dir_all`. **Meaning depends on context:**
    *   If using Docker (`--docker-container`): This must be the **absolute path inside the container** (e.g., `/rdf_mount_in_container`) accessible by Virtuoso.
    *   If *not* using Docker: This must be the **path on the host system** accessible by the Virtuoso server process.
    *   In either case, this path **must** be listed in the relevant `DirsAllowed` setting.
*   `-k`, `--password`: **Required.** Virtuoso `dba` user password (Default: `dba`).
*   `-H`, `--host`: Virtuoso server host (Default: `localhost`).
*   `-P`, `--port`: Virtuoso server ISQL port (Default: `1111`). Use the *host* port if mapped via Docker.
*   `-u`, `--user`: Virtuoso username (Default: `dba`).
*   `--recursive`: Search for `.nq.gz` files recursively within the data directory (uses `ld_dir_all` instead of `ld_dir`).
*   `--checkpoint-interval`: Interval (seconds) to set for Virtuoso checkpointing *after* the bulk load completes (Default: `60`).
*   `--scheduler-interval`: Interval (seconds) to set for the Virtuoso scheduler *after* the bulk load completes (Default: `10`).
*   `--isql-path`: Path to `isql` on the host system (Default: `isql`). Used only if not in Docker mode.

### Quadstore Dump Utility ([`dump_quadstore.py`](https://github.com/opencitations/virtuoso_utilities/blob/master/virtuoso_utilities/dump_quadstore.py))

This script provides a comprehensive solution to export the entire content of a Virtuoso quadstore using the official Virtuoso `dump_nquads` stored procedure. This procedure is specifically designed for N-Quads export and is documented in the [official OpenLink Virtuoso VOS documentation](https://vos.openlinksw.com/owiki/wiki/VOS/VirtRDFDumpNQuad).

**Key Features:**

*   **Official Virtuoso Procedure:** Uses the optimized `dump_nquads` stored procedure specifically designed for N-Quads export with Named Graph information preservation.
*   **N-Quads Format:** Outputs data in N-Quads format, which preserves Named Graph IRI information and provides significant value for data partitioning applications.
*   **Automatic Procedure Installation:** The script automatically installs the required `dump_nquads` stored procedure in Virtuoso before performing the dump.
*   **Automatic Graph Filtering:** The procedure automatically excludes internal `virtrdf:` graphs while including all user data graphs.
*   **Automatic Compression:** Output files are automatically compressed as .nq.gz files for space efficiency (can be disabled).
*   **Configurable File Size Limits:** Control the maximum size of individual dump files to prevent excessively large files.
*   **Sequential File Numbering:** Files are numbered sequentially (output000001.nq.gz, output000002.nq.gz, etc.) with configurable starting numbers.
*   **Docker Integration:** Full support for Docker-based Virtuoso instances.

**Important Prerequisites:**

*   **DirsAllowed Configuration:** The output directory **must** be accessible by the Virtuoso server process and listed in the `DirsAllowed` parameter within the Virtuoso INI file.
*   **File Permissions:** When using Docker, ensure the output directory is properly mounted and accessible inside the container.

**Basic Usage (Local Virtuoso):**

```bash
# With pipx (global installation)
virtuoso-dump \
    --password <your_virtuoso_password> \
    --output-dir ./virtuoso_dump

# With Poetry (development)
poetry run python virtuoso_utilities/dump_quadstore.py \
    --password <your_virtuoso_password> \
    --output-dir ./virtuoso_dump
```

**Export with Custom File Size Limits (50MB per file):**

```bash
# With pipx (global installation)
virtuoso-dump \
    --password <your_virtuoso_password> \
    --output-dir ./virtuoso_dump \
    --file-length-limit 50000000

# With Poetry (development)
poetry run python virtuoso_utilities/dump_quadstore.py \
    --password <your_virtuoso_password> \
    --output-dir ./virtuoso_dump \
    --file-length-limit 50000000
```

**Export Uncompressed Files Starting from output000005.nq:**

```bash
# With pipx (global installation)
virtuoso-dump \
    --password <your_virtuoso_password> \
    --output-dir ./virtuoso_dump \
    --no-compression \
    --start-from 5

# With Poetry (development)
poetry run python virtuoso_utilities/dump_quadstore.py \
    --password <your_virtuoso_password> \
    --output-dir ./virtuoso_dump \
    --no-compression \
    --start-from 5
```

**Usage with Docker:**

```bash
# Example: First launch Virtuoso (with pipx)
virtuoso-launch \
    --name my-virtuoso-dump \
    --isql-port 1112 \
    --data-dir ./virtuoso-data \
    --mount-volume ./dump_output:/dumps

# Then dump the quadstore (with pipx)
virtuoso-dump \
    --password <your_virtuoso_password> \
    --port 1112 \
    --docker-container my-virtuoso-dump \
    --output-dir /dumps
```

**Arguments:**

Use `virtuoso-dump --help` (or `poetry run python virtuoso_utilities/dump_quadstore.py --help`) to see all available options:

**Connection Parameters:**
*   `-H`, `--host`: Virtuoso server host (Default: `localhost`).
*   `-P`, `--port`: Virtuoso server ISQL port (Default: `1111`). Use the *host* port if mapped via Docker.
*   `-u`, `--user`: Virtuoso username (Default: `dba`).
*   `-k`, `--password`: **Required.** Virtuoso password (Default: `dba`).

**Output Parameters:**
*   `-o`, `--output-dir`: Output directory for N-Quads files (Default: `./virtuoso_dump`).
    - **With Docker:** this must be an already existing directory mounted inside the container, accessible by Virtuoso and present in DirsAllowed. The script does not create it on the host.
    - **Without Docker:** the directory will be created automatically if it does not exist.
*   `--file-length-limit`: Maximum length of dump files in bytes (Default: `100,000,000` - 100MB).
*   `--no-compression`: Disable gzip compression (files will be .nq instead of .nq.gz).

**Docker Parameters:**
*   `--docker-container`: Name or ID of the running Virtuoso Docker container.

**Output Format:**

The script outputs data in **N-Quads (.nq)** format, which is the native format used by Virtuoso's `dump_nquads` procedure. Files are automatically compressed to `.nq.gz` format unless `--no-compression` is specified. The naming pattern is:
- `output000001.nq.gz`, `output000002.nq.gz`, etc. for compressed files
- `output000001.nq`, `output000002.nq`, etc. for uncompressed files

### Full-Text Index Rebuilder ([`rebuild_fulltext_index.py`](https://github.com/opencitations/virtuoso_utilities/blob/master/virtuoso_utilities/rebuild_fulltext_index.py))

This script provides a utility to rebuild the Virtuoso full-text index, which is essential for optimal querying of RDF object values using the `bif:contains` function in SPARQL queries. The implementation is based on the official [OpenLink Software documentation](https://community.openlinksw.com/t/how-to-rebuild-virtuoso-full-text-index/2697).

**Why rebuild the Full-Text index?**

In some cases, the Full-Text index may need to be recreated if unexpected results are returned when using the `bif:contains` Full-Text index search function in SPARQL queries. The Virtuoso RDF Quad store supports optional Full-Text indexing of RDF object values, providing much better performance compared to the SPARQL `regex` feature, which is very inefficient in most cases.

**IMPORTANT:** Always ensure you have a full backup of your database before running this script, as it involves dropping and recreating database tables.

**What the script does:**

1. Drops the existing full-text index tables
2. Recreates the index structure
3. Refills the index with data

**Note:** After this process completes, the Virtuoso database **MUST** be restarted for the index rebuild to take effect.

**Basic Usage:**

```bash
# With pipx (global installation)
virtuoso-rebuild-index --password <your_virtuoso_password>

# With Poetry (development)
poetry run python virtuoso_utilities/rebuild_fulltext_index.py --password <your_virtuoso_password>
```

**Usage with Docker:**

```bash
# With pipx (global installation)
virtuoso-rebuild-index \
    --password <your_virtuoso_password> \
    --docker-container my-virtuoso

# With Poetry (development)
poetry run python virtuoso_utilities/rebuild_fulltext_index.py \
    --password <your_virtuoso_password> \
    --docker-container my-virtuoso
```

**Arguments:**

Use `virtuoso-rebuild-index --help` (or `poetry run python virtuoso_utilities/rebuild_fulltext_index.py --help`) to see all available options:

*   `--host`: Virtuoso host (Default: `localhost`).
*   `--port`: Virtuoso ISQL port (Default: `1111`).
*   `--user`: Virtuoso username (Default: `dba`).
*   `--password`: Virtuoso password (Default: `dba`).
*   `--docker-container`: Name or ID of the running Virtuoso Docker container. If specified, `isql` commands are run via `docker exec`, and `-d` refers to the path *inside* the container.
*   `--docker-isql-path`: Path to the `isql` executable *inside* the Docker container (Default: `isql`, often needs to be `/opt/virtuoso-opensource/bin/isql`).
*   `--docker-path`: Path to the `docker` executable on the host system (Default: `docker`).