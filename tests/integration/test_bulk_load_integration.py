import re
import subprocess

import pytest

from virtuoso_utilities.bulk_load import bulk_load

CONTAINER_NAME = "virtuoso-utilities-test"
DBA_PASSWORD = "dba"
HOST_PORT = 11115


def parse_sparql_count(result):
    """
    Parse COUNT result from SPARQL query output.

    Extracts the integer count from ISQL output, skipping version and header lines.
    """
    lines = result.strip().splitlines()
    for line in lines:
        line = line.strip()
        if line and line.isdigit():
            return int(line)
    raise ValueError(f"Could not parse count from result: {result}")


def query_triple_count(container_name, graph=None):
    """
    Count triples in Virtuoso, optionally filtered by graph.

    Only counts triples with test subjects (subject1, subject2, subject3, root1, sub1).

    Args:
        container_name: Docker container name
        graph: Optional graph URI to filter by

    Returns:
        Integer count of triples
    """
    test_subjects = [
        "http://example.org/subject1",
        "http://example.org/subject2",
        "http://example.org/subject3",
        "http://example.org/root1",
        "http://example.org/sub1"
    ]

    values_clause = " ".join([f"<{s}>" for s in test_subjects])

    if graph:
        query = f'SELECT (COUNT(*) as ?count) WHERE {{ VALUES ?s {{ {values_clause} }} GRAPH <{graph}> {{ ?s ?p ?o }} }}'
    else:
        query = f'SELECT (COUNT(*) as ?count) WHERE {{ VALUES ?s {{ {values_clause} }} GRAPH ?g {{ ?s ?p ?o }} }}'

    result = run_sparql_query(container_name, query)
    return parse_sparql_count(result)


def query_subjects(container_name):
    """
    Get list of test subject URIs in Virtuoso.

    Only returns specific test subjects.

    Args:
        container_name: Docker container name

    Returns:
        List of subject URI strings
    """
    test_subjects = [
        "http://example.org/subject1",
        "http://example.org/subject2",
        "http://example.org/subject3",
        "http://example.org/root1",
        "http://example.org/sub1"
    ]

    values_clause = " ".join([f"<{s}>" for s in test_subjects])
    query = f'SELECT DISTINCT ?s WHERE {{ VALUES ?s {{ {values_clause} }} GRAPH ?g {{ ?s ?p ?o }} }} ORDER BY ?s'
    result = run_sparql_query(container_name, query)

    subjects = []
    for line in result.splitlines():
        stripped = line.strip()
        if stripped.startswith('http://example.org/'):
            subjects.append(stripped)
    return subjects


def query_graphs(container_name):
    """
    Get list of test named graphs in Virtuoso.

    Only returns test graphs (graph1, graph2).

    Args:
        container_name: Docker container name

    Returns:
        List of graph URI strings
    """
    test_graphs = [
        "http://example.org/graph1",
        "http://example.org/graph2"
    ]

    values_clause = " ".join([f"<{g}>" for g in test_graphs])
    query = f'SELECT DISTINCT ?g WHERE {{ VALUES ?g {{ {values_clause} }} GRAPH ?g {{ ?s ?p ?o }} }} ORDER BY ?g'
    result = run_sparql_query(container_name, query)

    graphs = []
    for line in result.splitlines():
        stripped = line.strip()
        if stripped.startswith('http://example.org/'):
            graphs.append(stripped)
    return graphs


def test_bulk_load_success(clean_virtuoso, sample_nquads_file, test_data_dir):
    """
    Test successful bulk loading of a single .nq.gz file.

    Verifies that:
    1. The bulk_load function completes without errors
    2. Data is actually loaded into Virtuoso
    3. Data can be queried via SPARQL
    """
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

    subjects = query_subjects(clean_virtuoso)
    assert len(subjects) == 3, f"Expected exactly 3 test subjects, got {len(subjects)}: {subjects}"
    assert "http://example.org/subject1" in subjects, f"Missing subject1 in {subjects}"
    assert "http://example.org/subject2" in subjects, f"Missing subject2 in {subjects}"
    assert "http://example.org/subject3" in subjects, f"Missing subject3 in {subjects}"


def test_bulk_load_recursive(clean_virtuoso, sample_nquads_files_recursive, test_data_dir):
    """
    Test recursive bulk loading from subdirectories.

    Verifies that files in subdirectories are loaded when recursive=True.
    """
    bulk_load(
        data_directory=str(test_data_dir),
        password=DBA_PASSWORD,
        host="localhost",
        port=HOST_PORT,
        user="dba",
        recursive=True,
        docker_container=clean_virtuoso,
        container_data_directory="/database/test_data"
    )

    subjects = query_subjects(clean_virtuoso)
    assert len(subjects) == 2, f"Expected exactly 2 test subjects from recursive loading, got {len(subjects)}: {subjects}"
    assert "http://example.org/root1" in subjects, f"Missing root1 from root directory in {subjects}"
    assert "http://example.org/sub1" in subjects, f"Missing sub1 from subdirectory in {subjects}"


def test_bulk_load_no_files(clean_virtuoso, test_data_dir):
    """
    Test bulk_load with an empty directory.

    Should return gracefully without errors.
    """
    empty_dir = test_data_dir / "empty"
    empty_dir.mkdir(exist_ok=True)

    try:
        bulk_load(
            data_directory=str(empty_dir),
            password=DBA_PASSWORD,
            host="localhost",
            port=HOST_PORT,
            user="dba",
            recursive=False,
            docker_container=clean_virtuoso,
            container_data_directory="/database/test_data/empty"
        )

        subjects = query_subjects(clean_virtuoso)
        assert len(subjects) == 0, f"Expected 0 test subjects with empty directory, got {len(subjects)}: {subjects}"
    finally:
        if empty_dir.exists():
            empty_dir.rmdir()


def test_bulk_load_dirs_not_allowed(clean_virtuoso, sample_nquads_file, test_data_dir):
    """
    Test that bulk_load fails when the directory is not in DirsAllowed.

    This simulates the common configuration error where the data directory
    is not listed in virtuoso.ini DirsAllowed parameter.
    """
    forbidden_dir = "/tmp/forbidden_test_data"

    with pytest.raises(RuntimeError) as exc_info:
        bulk_load(
            data_directory=str(test_data_dir),
            password=DBA_PASSWORD,
            host="localhost",
            port=HOST_PORT,
            user="dba",
            recursive=False,
            docker_container=clean_virtuoso,
            container_data_directory=forbidden_dir
        )

    assert "unable to list files" in str(exc_info.value).lower() or "fa020" in str(exc_info.value).lower(), \
        f"Expected 'unable to list files' error, got: {exc_info.value}"


def test_bulk_load_verify_graphs(clean_virtuoso, sample_nquads_file, test_data_dir):
    """
    Test that data is loaded into the correct named graphs.

    Verifies graph structure is preserved from N-Quads.
    """
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

    graphs = query_graphs(clean_virtuoso)
    assert len(graphs) == 2, f"Expected exactly 2 named graphs, got {len(graphs)}: {graphs}"
    assert "http://example.org/graph1" in graphs, f"Missing graph1 in {graphs}"
    assert "http://example.org/graph2" in graphs, f"Missing graph2 in {graphs}"

    count_graph1 = query_triple_count(clean_virtuoso, "http://example.org/graph1")
    assert count_graph1 == 2, f"Expected 2 triples in graph1, got {count_graph1}"

    count_graph2 = query_triple_count(clean_virtuoso, "http://example.org/graph2")
    assert count_graph2 == 1, f"Expected 1 triple in graph2, got {count_graph2}"


def test_bulk_load_cleanup_load_list(clean_virtuoso, sample_nquads_file, test_data_dir):
    """
    Test that DB.DBA.load_list table is cleaned after successful bulk load.

    Verifies that successfully loaded files (ll_state = 2) are removed
    from the load_list table after completion.
    """
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

    sql = "SELECT COUNT(*) FROM DB.DBA.load_list WHERE ll_state = 2;"
    cmd = [
        "docker", "exec", clean_virtuoso,
        "/opt/virtuoso-opensource/bin/isql",
        "-U", "dba", "-P", DBA_PASSWORD,
        f"exec={sql}"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    count = parse_sparql_count(result.stdout)
    assert count == 0, f"Expected 0 loaded files in load_list after cleanup, got {count}"


def run_sparql_query(container_name, query):
    """
    Execute a SPARQL query against the test Virtuoso instance.

    Args:
        container_name: Docker container name
        query: SPARQL query string

    Returns:
        Query result as string
    """
    escaped_query = query.replace('"', '\\"')
    cmd = [
        "docker", "exec", container_name,
        "/opt/virtuoso-opensource/bin/isql",
        "-U", "dba", "-P", DBA_PASSWORD,
        f"exec=SPARQL {escaped_query};"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return result.stdout
