import argparse
import gzip
import shutil
import subprocess
import time
from pathlib import Path

import pytest

CONTAINER_NAME = "virtuoso-utilities-test"
VIRTUOSO_IMAGE = "openlink/virtuoso-opensource-7:7.2.15"
VIRTUOSO_PORT = 1111
VIRTUOSO_HTTP_PORT = 8890
HOST_PORT = 11115
HOST_HTTP_PORT = 8895
DBA_PASSWORD = "dba"


@pytest.fixture(scope="session")
def test_dir():
    """Return the tests directory path."""
    return Path(__file__).parent


@pytest.fixture(scope="session")
def virtuoso_db_dir(test_dir):
    """Create and return the test Virtuoso database directory."""
    db_dir = test_dir / "test_virtuoso_db"
    db_dir.mkdir(exist_ok=True)
    return db_dir


@pytest.fixture(scope="session")
def test_data_dir(test_dir):
    """Create and return the test data directory for .nq.gz files."""
    data_dir = test_dir / "test_data"
    data_dir.mkdir(exist_ok=True)
    return data_dir


@pytest.fixture(scope="session")
def virtuoso_container(test_dir, virtuoso_db_dir, test_data_dir):
    """
    Start a Virtuoso Docker container for the entire test session.

    Yields container name when ready, then stops and removes container on cleanup.
    """
    config_template = test_dir / "virtuoso_config_template" / "virtuoso.ini"
    config_dest = virtuoso_db_dir / "virtuoso.ini"

    subprocess.run(["cp", str(config_template), str(config_dest)], check=True)

    print(f"\nRemoving existing {CONTAINER_NAME} container if present...")
    subprocess.run(
        ["docker", "rm", "-f", CONTAINER_NAME],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    print(f"Starting {CONTAINER_NAME} container...")
    cmd = [
        "docker", "run", "-d",
        "--name", CONTAINER_NAME,
        "-p", f"{HOST_HTTP_PORT}:{VIRTUOSO_HTTP_PORT}",
        "-p", f"{HOST_PORT}:{VIRTUOSO_PORT}",
        "-e", f"DBA_PASSWORD={DBA_PASSWORD}",
        "-e", "SPARQL_UPDATE=true",
        "-v", f"{virtuoso_db_dir}:/database",
        "-v", f"{test_data_dir}:/database/test_data",
        VIRTUOSO_IMAGE
    ]

    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    container_id = result.stdout.strip()

    print(f"Container started with ID: {container_id}")
    print("Waiting for Virtuoso to start (approx. 30 seconds)...")
    time.sleep(30)

    check_cmd = ["docker", "ps", "-q", "-f", f"name={CONTAINER_NAME}"]
    result = subprocess.run(check_cmd, capture_output=True, text=True)
    if not result.stdout.strip():
        subprocess.run(["docker", "logs", CONTAINER_NAME])
        raise RuntimeError(f"{CONTAINER_NAME} failed to start")

    print("Granting SPARQL_UPDATE role...")
    isql_cmd = [
        "docker", "exec", CONTAINER_NAME,
        "/opt/virtuoso-opensource/bin/isql",
        "-U", "dba", "-P", DBA_PASSWORD,
        "exec=DB.DBA.USER_GRANT_ROLE ('SPARQL', 'SPARQL_UPDATE');"
    ]
    subprocess.run(isql_cmd, check=True, stdout=subprocess.DEVNULL)

    print("Setting default RDF user permissions...")
    isql_cmd = [
        "docker", "exec", CONTAINER_NAME,
        "/opt/virtuoso-opensource/bin/isql",
        "-U", "dba", "-P", DBA_PASSWORD,
        "exec=DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);"
    ]
    subprocess.run(isql_cmd, check=True, stdout=subprocess.DEVNULL)

    print(f"Virtuoso test container ready at localhost:{HOST_PORT}")
    print(f"SPARQL endpoint: http://localhost:{HOST_HTTP_PORT}/sparql")

    yield CONTAINER_NAME

    print(f"\nStopping and removing {CONTAINER_NAME} container...")
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], check=True)
    print("Container removed.")


@pytest.fixture(scope="function")
def clean_virtuoso(virtuoso_container):
    """
    Clean Virtuoso database before each test.

    Removes all RDF data and clears bulk loader tracking table.
    """
    cleanup_sql = "log_enable(3,1); RDF_GLOBAL_RESET(); DELETE FROM DB.DBA.load_list;"

    cleanup_cmd = [
        "docker", "exec", virtuoso_container,
        "/opt/virtuoso-opensource/bin/isql",
        "-U", "dba", "-P", DBA_PASSWORD,
        f"exec={cleanup_sql}"
    ]

    subprocess.run(cleanup_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    yield virtuoso_container


@pytest.fixture
def sample_nquads_file(test_data_dir, request):
    """
    Create a sample .nq.gz file with test RDF data.

    Returns the path to the created file.
    """
    test_name = request.node.name
    file_path = test_data_dir / f"{test_name}_data.nq.gz"

    nquads_data = """
<http://example.org/subject1> <http://example.org/predicate1> "Object 1" <http://example.org/graph1> .
<http://example.org/subject2> <http://example.org/predicate2> "Object 2" <http://example.org/graph1> .
<http://example.org/subject3> <http://example.org/predicate3> "Object 3" <http://example.org/graph2> .
""".strip()

    with gzip.open(file_path, 'wt', encoding='utf-8') as f:
        f.write(nquads_data)

    yield file_path

    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def sample_nquads_files_recursive(test_data_dir, request):
    """
    Create multiple .nq.gz files in subdirectories for recursive loading tests.

    Returns a list of created file paths.
    """
    test_name = request.node.name
    subdir = test_data_dir / f"{test_name}_subdir"
    subdir.mkdir(exist_ok=True)

    file1 = test_data_dir / f"{test_name}_data1.nq.gz"
    file2 = subdir / f"{test_name}_data2.nq.gz"

    nquads1 = """
<http://example.org/root1> <http://example.org/pred1> "Root data" <http://example.org/graph1> .
""".strip()

    nquads2 = """
<http://example.org/sub1> <http://example.org/pred2> "Subdir data" <http://example.org/graph2> .
""".strip()

    with gzip.open(file1, 'wt', encoding='utf-8') as f:
        f.write(nquads1)

    with gzip.open(file2, 'wt', encoding='utf-8') as f:
        f.write(nquads2)

    yield [file1, file2]

    if file1.exists():
        file1.unlink()
    if file2.exists():
        file2.unlink()
    if subdir.exists():
        subdir.rmdir()


@pytest.fixture
def dump_output_dir(test_data_dir, request):
    """Create temporary directory for dump output."""
    output_dir = test_data_dir / f"{request.node.name}_dump"
    output_dir.mkdir(exist_ok=True)
    yield output_dir
    shutil.rmtree(output_dir, ignore_errors=True)


@pytest.fixture
def sample_nquads_with_text(test_data_dir, request):
    """Create .nq.gz file with text content for fulltext search testing."""
    test_name = request.node.name
    file_path = test_data_dir / f"{test_name}_text.nq.gz"

    nquads_data = """
<http://example.org/doc1> <http://purl.org/dc/elements/1.1/title> "The quick brown fox jumps over the lazy dog" <http://example.org/textgraph> .
<http://example.org/doc2> <http://purl.org/dc/elements/1.1/title> "A lazy cat sleeps on the mat" <http://example.org/textgraph> .
<http://example.org/doc3> <http://purl.org/dc/elements/1.1/description> "Fox and dog are common animals" <http://example.org/textgraph> .
""".strip()

    with gzip.open(file_path, 'wt', encoding='utf-8') as f:
        f.write(nquads_data)

    yield file_path

    if file_path.exists():
        file_path.unlink()


@pytest.fixture
def dump_args(virtuoso_container, dump_output_dir):
    """Create argparse.Namespace for dump_quadstore tests."""
    container_output_dir = f"/database/test_data/{dump_output_dir.name}"
    return argparse.Namespace(
        host="localhost",
        port=HOST_PORT,
        user="dba",
        password=DBA_PASSWORD,
        docker_container=virtuoso_container,
        docker_path="docker",
        docker_isql_path="isql",
        isql_path="isql",
        output_dir=container_output_dir,
        file_length_limit=100000000,
        compression=True,
    )


@pytest.fixture
def rebuild_args(virtuoso_container):
    """Create argparse.Namespace for rebuild_fulltext_index tests."""
    return argparse.Namespace(
        host="localhost",
        port=HOST_PORT,
        user="dba",
        password=DBA_PASSWORD,
        docker_container=virtuoso_container,
        docker_path="docker",
        docker_isql_path="isql",
        isql_path="isql",
        restart_container=False,
    )


