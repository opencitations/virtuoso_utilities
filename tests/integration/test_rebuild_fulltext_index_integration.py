import subprocess
import time

from virtuoso_utilities.bulk_load import bulk_load
from virtuoso_utilities.rebuild_fulltext_index import (
    drop_fulltext_tables,
    enable_batch_update,
    rebuild_fulltext_index,
    recreate_fulltext_index,
    refill_fulltext_index,
)

DBA_PASSWORD = "dba"
HOST_PORT = 11115


def parse_sparql_count(result: str) -> int:
    """Parse COUNT result from SPARQL query output."""
    lines = result.strip().splitlines()
    for line in lines:
        line = line.strip()
        if line and line.isdigit():
            return int(line)
    raise ValueError(f"Could not parse count from result: {result}")


def verify_fulltext_search(container_name: str, search_term: str) -> int:
    """Run fulltext search and return count of matching results."""
    sql = f"SELECT COUNT(*) FROM DB.DBA.RDF_OBJ WHERE contains(RO_FLAGS, '{search_term}');"
    cmd = [
        "docker", "exec", container_name,
        "/opt/virtuoso-opensource/bin/isql",
        "localhost:1111",
        "dba", DBA_PASSWORD,
        f"exec={sql}"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Fulltext query failed: {result.stderr}", flush=True)
        return -1
    try:
        return parse_sparql_count(result.stdout)
    except ValueError:
        print(f"Could not parse: {result.stdout}", flush=True)
        return -1


def test_rebuild_on_empty_database(clean_virtuoso, rebuild_args):
    """Rebuild index on empty database, verify no errors."""
    result = rebuild_fulltext_index(rebuild_args)
    assert result is True


def test_rebuild_after_loading_data(
    clean_virtuoso, sample_nquads_file, test_data_dir, rebuild_args
):
    """Load RDF data, rebuild index, verify success."""
    bulk_load(
        data_directory=str(test_data_dir),
        password=DBA_PASSWORD,
        host="localhost",
        port=HOST_PORT,
        user="dba",
        recursive=False,
        docker_container=clean_virtuoso,
        container_data_directory="/database/test_data"
    )

    result = rebuild_fulltext_index(rebuild_args)
    assert result is True


def test_rebuild_with_restart_container(
    clean_virtuoso, sample_nquads_with_text, test_data_dir, rebuild_args
):
    """Verify rebuild with container restart completes successfully."""
    bulk_load(
        data_directory=str(test_data_dir),
        password=DBA_PASSWORD,
        host="localhost",
        port=HOST_PORT,
        user="dba",
        recursive=False,
        docker_container=clean_virtuoso,
        container_data_directory="/database/test_data"
    )

    rebuild_args.restart_container = True
    result = rebuild_fulltext_index(rebuild_args)
    assert result is True

    time.sleep(5)

    count = verify_fulltext_search(clean_virtuoso, "fox")
    assert count >= 0, f"Fulltext search query failed: {count}"


def test_drop_fulltext_tables(clean_virtuoso, rebuild_args):
    """Test drop tables function directly."""
    success, _, _ = drop_fulltext_tables(rebuild_args)
    assert success is True


def test_recreate_fulltext_index(clean_virtuoso, rebuild_args):
    """Test recreate function directly."""
    drop_fulltext_tables(rebuild_args)

    success, _, _ = recreate_fulltext_index(rebuild_args)
    assert success is True


def test_enable_batch_update(clean_virtuoso, rebuild_args):
    """Test enable batch update function."""
    drop_fulltext_tables(rebuild_args)
    recreate_fulltext_index(rebuild_args)

    success, _, _ = enable_batch_update(rebuild_args)
    assert success is True


def test_refill_fulltext_index(clean_virtuoso, rebuild_args):
    """Test refill function directly."""
    drop_fulltext_tables(rebuild_args)
    recreate_fulltext_index(rebuild_args)
    enable_batch_update(rebuild_args)

    success, _, _ = refill_fulltext_index(rebuild_args)
    assert success is True
