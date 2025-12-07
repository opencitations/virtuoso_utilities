import gzip
import subprocess
from pathlib import Path

from virtuoso_utilities.bulk_load import bulk_load
from virtuoso_utilities.dump_quadstore import (
    dump_quadstore,
    install_dump_procedure,
    list_output_files,
)

DBA_PASSWORD = "dba"
HOST_PORT = 11115


def read_nquads_from_dump(output_dir: Path, compressed: bool = True) -> list[str]:
    """Read and parse all N-Quads lines from dump output files."""
    lines = []
    pattern = "*.nq.gz" if compressed else "*.nq"
    for file_path in sorted(output_dir.glob(pattern)):
        if compressed:
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        lines.append(line)
        else:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        lines.append(line)
    return lines


def run_sparql_query(container_name: str, query: str) -> str:
    """Execute a SPARQL query against the test Virtuoso instance."""
    escaped_query = query.replace('"', '\\"')
    cmd = [
        "docker", "exec", container_name,
        "/opt/virtuoso-opensource/bin/isql",
        "-U", "dba", "-P", DBA_PASSWORD,
        f"exec=SPARQL {escaped_query};"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout


def parse_sparql_count(result: str) -> int:
    """Parse COUNT result from SPARQL query output."""
    lines = result.strip().splitlines()
    for line in lines:
        line = line.strip()
        if line and line.isdigit():
            return int(line)
    raise ValueError(f"Could not parse count from result: {result}")


def test_dump_empty_database(clean_virtuoso, dump_args, dump_output_dir):
    """Dump empty database, verify no data files or empty output."""
    result = dump_quadstore(dump_args)
    assert result is True

    output_files = list_output_files(str(dump_output_dir), compressed=True)
    if output_files:
        lines = read_nquads_from_dump(dump_output_dir, compressed=True)
        assert len(lines) == 0, f"Expected no triples in dump, got {len(lines)}"


def test_dump_with_triples_compressed(
    clean_virtuoso, sample_nquads_file, test_data_dir, dump_args, dump_output_dir
):
    """Load data, dump compressed, verify .nq.gz files created."""
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

    dump_args.compression = True
    result = dump_quadstore(dump_args)
    assert result is True

    output_files = list(dump_output_dir.glob("*.nq.gz"))
    assert len(output_files) > 0, "Expected at least one .nq.gz file"

    uncompressed_files = list(dump_output_dir.glob("*.nq"))
    assert len(uncompressed_files) == 0, "Expected no uncompressed .nq files"


def test_dump_with_triples_uncompressed(
    clean_virtuoso, sample_nquads_file, test_data_dir, dump_args, dump_output_dir
):
    """Load data, dump uncompressed, verify .nq files created."""
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

    dump_args.compression = False
    result = dump_quadstore(dump_args)
    assert result is True

    output_files = list(dump_output_dir.glob("*.nq"))
    assert len(output_files) > 0, "Expected at least one .nq file"


def test_dump_content_matches_database(
    clean_virtuoso, sample_nquads_file, test_data_dir, dump_args, dump_output_dir
):
    """Verify dumped content matches loaded triples."""
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

    query = "SELECT (COUNT(*) as ?count) WHERE { GRAPH ?g { ?s ?p ?o } FILTER(!STRSTARTS(STR(?g), 'http://www.openlinksw.com/')) }"
    result = run_sparql_query(clean_virtuoso, query)
    db_count = parse_sparql_count(result)

    dump_args.compression = True
    dump_result = dump_quadstore(dump_args)
    assert dump_result is True

    lines = read_nquads_from_dump(dump_output_dir, compressed=True)
    assert len(lines) == db_count, f"Expected {db_count} triples, got {len(lines)}"


def test_dump_preserves_named_graphs(
    clean_virtuoso, sample_nquads_file, test_data_dir, dump_args, dump_output_dir
):
    """Verify graph URIs preserved in output."""
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

    dump_args.compression = True
    result = dump_quadstore(dump_args)
    assert result is True

    lines = read_nquads_from_dump(dump_output_dir, compressed=True)

    graph1_found = any("http://example.org/graph1" in line for line in lines)
    graph2_found = any("http://example.org/graph2" in line for line in lines)

    assert graph1_found, "Expected graph1 URI in dump output"
    assert graph2_found, "Expected graph2 URI in dump output"


def test_install_dump_procedure(clean_virtuoso, dump_args):
    """Verify stored procedure installation."""
    result = install_dump_procedure(dump_args)
    assert result is True

    sql = "dump_nquads('/tmp', 1, 1000000, 0);"
    cmd = [
        "docker", "exec", clean_virtuoso,
        "/opt/virtuoso-opensource/bin/isql",
        "-U", "dba", "-P", DBA_PASSWORD,
        f"exec={sql}"
    ]
    proc_result = subprocess.run(cmd, capture_output=True, text=True)
    assert proc_result.returncode == 0, f"dump_nquads procedure call failed: {proc_result.stderr}"
