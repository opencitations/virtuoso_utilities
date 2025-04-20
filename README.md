# Virtuoso Utilities

A collection of Python utilities for interacting with OpenLink Virtuoso.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Virtuoso Docker Launcher](#virtuoso-docker-launcher-launch_virtuosopy)
    - [Memory-Based Configuration](#memory-based-configuration)
  - [Parallel Bulk Loader](#parallel-bulk-loader-bulk_load_parallelpy)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Features

*   **Parallel Bulk Loading:** The `bulk_load_parallel.py` script allows for loading multiple RDF files (TTL, RDF/XML, NQuads, etc.) into a Virtuoso instance concurrently using multiprocessing.
*   **Docker Support:** Seamlessly integrates with Virtuoso running in a Docker container by executing the `isql` command via `docker exec`.
*   **Flexible Configuration:** Allows customization of Virtuoso connection details, target graph, number of parallel processes, file patterns, and paths to `isql` and `docker` executables.
*   **Virtuoso Docker Launcher:** The `launch_virtuoso.py` script provides a convenient way to launch a Virtuoso database using Docker with customizable configuration parameters.

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
    --memory 4g \
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
*   `--memory`: Memory limit for the container (Default: `2g`, Maximum: `16g`).
*   `--cpu-limit`: CPU limit for the container, 0 means no limit (Default: `0`).
*   `--dba-password`: Password for the Virtuoso dba user (Default: `dba`).
*   `--max-rows`: ResultSet maximum number of rows (Default: `100000`).
*   `--max-dirty-buffers`: Maximum dirty buffers before checkpoint (Auto-calculated based on available system memory).
*   `--number-of-buffers`: Number of buffers (Auto-calculated based on available system memory).
*   `--wait-ready`: Wait until Virtuoso is ready to accept connections.
*   `--detach`: Run container in detached mode.
*   `--force-remove`: Force removal of existing container with the same name.

**Memory-Based Configuration:**

The script automatically calculates optimal values for `--number-of-buffers` and `--max-dirty-buffers` based on the specified container memory limit (`--memory` parameter), following OpenLink's recommendations.

> **IMPORTANT**: Memory allocation is limited to a maximum of 16GB to prevent container crashes. If you specify a higher value, it will automatically be reduced to 16GB with a warning message. This limitation exists because Docker containers with Virtuoso tend to be unstable with larger memory allocations, resulting in exit code 137 (out of memory) errors.

This ensures optimal performance based on the memory allocated to the container. The auto-calculated values follow these general guidelines:

| Container Memory | NumberOfBuffers | MaxDirtyBuffers |
|------------------|-----------------|-----------------|
| 2 GB             | 170000          | 130000          |
| 4 GB             | 340000          | 250000          |
| 8 GB             | 680000          | 500000          |
| 16 GB            | 1360000         | 1000000         |

You can still manually override the buffer values if needed for specific use cases, but the memory limit of 16GB cannot be exceeded.

For more information on Virtuoso performance tuning, refer to the [official OpenLink documentation on performance tuning](https://community.openlinksw.com/t/performance-tuning-virtuoso-for-rdf-queries-and-other-use/1692).

### Parallel Bulk Loader (`bulk_load_parallel.py`)

The script searches a specified directory for RDF files and loads them in parallel.

**Basic Usage (Host Virtuoso, Automatic File Detection):**

```bash
poetry run python bulk_load_parallel.py \
    -d /path/to/your/rdf/data \
    -k <your_virtuoso_password> \
```

**Usage with Specific Pattern (Host Virtuoso):**

```bash
poetry run python bulk_load_parallel.py \
    -d /path/to/your/rdf/data \
    -k <your_virtuoso_password> \
    --host <virtuoso_host> \
    --port <virtuoso_port> \
    --user <virtuoso_user> \
    --graph-uri <target_graph_uri> \
    --num-processes 4 \
    --file-pattern "*.ttl" \
```

**Usage with Docker:**

Ensure the directory specified with `-d` is mounted into your Virtuoso Docker container.

```bash
poetry run virtuoso_utilities/python bulk_load_parallel.py \
    -d /path/on/host/to/rdf/data \
    -k <your_virtuoso_password> \
    --docker-container <your_virtuoso_container_name_or_id> \
    --docker-data-mount-path /path/inside/container/where/data/is/mounted \
    --host <virtuoso_host_reachable_from_container> \ # Often the container name itself
    --port <virtuoso_port> \
    --user <virtuoso_user> \
    --graph-uri <target_graph_uri> \
    --num-processes 4 \
```

**Arguments:**

Use `poetry run virtuoso_utilities/python bulk_load_parallel.py --help` to see all available options:

*   `-d`, `--data-directory`: **Required.** Directory containing RDF files.
*   `-k`, `--password`: **Required.** Virtuoso password.
*   `-H`, `--host`: Virtuoso server host (Default: `localhost`).
*   `-P`, `--port`: Virtuoso server port (Default: `1111`).
*   `-u`, `--user`: Virtuoso username (Default: `dba`).
*   `-n`, `--num-processes`: Number of parallel processes (Default: CPU count).
*   `-f`, `--file-pattern`: Glob pattern to match files (e.g., `*.nq`, `prefix_*.ttl`). If omitted, the script automatically searches for files with common RDF extensions: `.ttl`, `.rdf`, `.owl`, `.nq`, `.trig`, `.trix`, `.xml`.
*   `-g`, `--graph-uri`: Target graph URI for triple formats (Default: `http://localhost:8890/DAV`). **Note:** This is ignored for quad formats (NQuads, TriG, Trix), as they define their own graph URIs within the file content.
*   `--isql-path`: Path to `isql` on the host (Used only if not using Docker).
*   `--docker-container`: Name/ID of the Virtuoso Docker container.
*   `--docker-data-mount-path`: **Required with `--docker-container`**. Path inside the container where the host data directory is mounted.

## Development

To run tests:

```bash
poetry install --with dev
poetry run pytest
```

(Note: Tests might require a running Virtuoso instance, potentially managed via `pytest-docker`.)

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the ISC License. See the [LICENSE](LICENSE) file for details. 