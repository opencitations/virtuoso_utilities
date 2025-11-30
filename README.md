# Virtuoso Utilities

[![Tests](https://github.com/opencitations/virtuoso_utilities/actions/workflows/test.yml/badge.svg)](https://github.com/opencitations/virtuoso_utilities/actions/workflows/test.yml)
[![Coverage](https://byob.yarr.is/arcangelo7/badges/opencitations-virtuoso-utilities-coverage-master)](https://opencitations.github.io/virtuoso_utilities/coverage/)
[![Python Versions](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://github.com/opencitations/virtuoso_utilities)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![License](https://img.shields.io/badge/license-ISC-green)](https://github.com/opencitations/virtuoso_utilities/blob/master/LICENSE)

A collection of Python CLI utilities for interacting with OpenLink Virtuoso.

## Documentation

Full documentation is available at: **https://opencitations.github.io/virtuoso_utilities/**

## Quick start

```bash
# Install
pipx install virtuoso-utilities

# Launch Virtuoso with Docker
virtuoso-launch --name my-virtuoso --memory 8g --mount-volume /data/rdf:/rdf --detach --wait-ready

# Bulk load RDF data
virtuoso-bulk-load -d /rdf -k dba --docker-container my-virtuoso --recursive

# Dump quadstore
virtuoso-dump -k dba --docker-container my-virtuoso -o /dumps

# Rebuild full-text index
virtuoso-rebuild-index --password dba --docker-container my-virtuoso
```

### Programmatic usage

```python
from virtuoso_utilities.launch_virtuoso import launch_virtuoso
from virtuoso_utilities.bulk_load import bulk_load

launch_virtuoso(
    name="my-virtuoso",
    memory="8g",
    extra_volumes=["/data/rdf:/rdf"],
    detach=True,
    wait_ready=True,
)

bulk_load(
    data_directory="/rdf",
    password="dba",
    docker_container="my-virtuoso",
    recursive=True,
)
```

## License

ISC
