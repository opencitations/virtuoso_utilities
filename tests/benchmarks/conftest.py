import json
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pytest
import requests

from .sample_data import SAMPLE_BR_URIS, SAMPLE_DOIS, SAMPLE_VVIS

PARALLELISM_LEVELS = [1] + [int((os.cpu_count() or 1) * f) for f in [0.25, 0.5, 0.75, 1.0]]
FIXED_QUERY_COUNT = 1000

OUTPUT_DIR = Path("benchmark_results")

SPARQL_ENDPOINT = "http://localhost:8790/sparql"
DATACITE = "http://purl.org/spar/datacite/"
LITERAL_REIFICATION = "http://www.essepuntato.it/2010/06/literalreification/"
FABIO = "http://purl.org/spar/fabio/"
FRBR = "http://purl.org/vocab/frbr/core#"
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"

QUERY_TIMEOUT = 60


def execute_query(session: requests.Session, query: str, timeout: int = QUERY_TIMEOUT) -> dict:
    response = session.post(
        SPARQL_ENDPOINT,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def execute_query_in_process(query: str, timeout: int = QUERY_TIMEOUT) -> dict:
    session = requests.Session()
    try:
        return execute_query(session, query, timeout)
    finally:
        session.close()


def build_spo_query(uris: list) -> str:
    values = " ".join([f"<{uri}>" for uri in uris])
    return f"""
        SELECT ?s ?p ?o WHERE {{
            VALUES ?s {{ {values} }}
            ?s ?p ?o .
        }}
    """


def build_identifier_lookup_query(scheme: str, literal: str) -> str:
    escaped = literal.replace("\\", "\\\\").replace('"', '\\"')
    return f"""
        SELECT ?s WHERE {{
            {{
                ?id <{LITERAL_REIFICATION}hasLiteralValue> "{escaped}" .
            }}
            UNION
            {{
                ?id <{LITERAL_REIFICATION}hasLiteralValue> "{escaped}"^^<{XSD_STRING}> .
            }}
            ?id <{DATACITE}usesIdentifierScheme> <{DATACITE}{scheme}> .
            ?s <{DATACITE}hasIdentifier> ?id .
        }}
    """


def build_vvi_query(venue_uri: str, volume: str, issue: str) -> str:
    escaped_volume = volume.replace("\\", "\\\\").replace('"', '\\"')
    escaped_issue = issue.replace("\\", "\\\\").replace('"', '\\"')
    return f"""
        SELECT ?s WHERE {{
            {{
                ?volume a <{FABIO}JournalVolume> ;
                    <{FRBR}partOf> <{venue_uri}> ;
                    <{FABIO}hasSequenceIdentifier> "{escaped_volume}" .
                ?s a <{FABIO}JournalIssue> ;
                    <{FRBR}partOf> ?volume ;
                    <{FABIO}hasSequenceIdentifier> "{escaped_issue}" .
            }}
            UNION
            {{
                ?volume a <{FABIO}JournalVolume> ;
                    <{FRBR}partOf> <{venue_uri}> ;
                    <{FABIO}hasSequenceIdentifier> "{escaped_volume}"^^<{XSD_STRING}> .
                ?s a <{FABIO}JournalIssue> ;
                    <{FRBR}partOf> ?volume ;
                    <{FABIO}hasSequenceIdentifier> "{escaped_issue}" .
            }}
            UNION
            {{
                ?volume a <{FABIO}JournalVolume> ;
                    <{FRBR}partOf> <{venue_uri}> ;
                    <{FABIO}hasSequenceIdentifier> "{escaped_volume}" .
                ?s a <{FABIO}JournalIssue> ;
                    <{FRBR}partOf> ?volume ;
                    <{FABIO}hasSequenceIdentifier> "{escaped_issue}"^^<{XSD_STRING}> .
            }}
            UNION
            {{
                ?volume a <{FABIO}JournalVolume> ;
                    <{FRBR}partOf> <{venue_uri}> ;
                    <{FABIO}hasSequenceIdentifier> "{escaped_volume}"^^<{XSD_STRING}> .
                ?s a <{FABIO}JournalIssue> ;
                    <{FRBR}partOf> ?volume ;
                    <{FABIO}hasSequenceIdentifier> "{escaped_issue}"^^<{XSD_STRING}> .
            }}
        }}
    """


def pytest_configure(config):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


@pytest.fixture(scope="session", autouse=True)
def check_endpoint_available():
    """Verify SPARQL endpoint is reachable before running benchmarks."""
    try:
        requests.get(SPARQL_ENDPOINT, timeout=5)
    except requests.exceptions.ConnectionError:
        pytest.fail(f"SPARQL endpoint not reachable: {SPARQL_ENDPOINT}")


def pytest_sessionfinish(session, exitstatus):
    json_path = Path(".benchmarks/baseline.json")
    if json_path.exists():
        _generate_plots(json_path, OUTPUT_DIR)


def _generate_plots(json_path: Path, output_dir: Path):
    with open(json_path) as f:
        data = json.load(f)

    benchmarks = data.get("benchmarks", [])
    if not benchmarks:
        return

    groups = _group_benchmarks(benchmarks)
    parallel_groups = {
        k: v for k, v in groups.items()
        if "parallel" in k or "mixed" in k or "sustained" in k or "fixed" in k
    }

    if not parallel_groups:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    _plot_total_time(parallel_groups, ax)
    plt.tight_layout()
    plt.savefig(output_dir / "benchmark_results.png", dpi=150)
    plt.close()


def _group_benchmarks(benchmarks: list) -> dict:
    groups = {}
    for bench in benchmarks:
        name = bench["name"]
        if "[" in name:
            base_name = name.split("[")[0]
            param = name.split("[")[1].rstrip("]")
        else:
            base_name = name
            param = "single"

        if base_name not in groups:
            groups[base_name] = []
        groups[base_name].append({
            "param": param,
            "stats": bench["stats"],
            "extra_info": bench.get("extra_info", {}),
        })
    return groups


def _plot_total_time(parallel_groups: dict, ax):
    for name, results in parallel_groups.items():
        workers = []
        times = []
        for r in sorted(results, key=lambda x: int(x["param"]) if x["param"].isdigit() else 0):
            if r["param"].isdigit():
                workers.append(int(r["param"]))
                times.append(r["stats"]["mean"])
        if workers:
            ax.plot(workers, times, marker="o", label=name.replace("test_benchmark_", "").replace("fixed_", "query_"))

    ax.set_xlabel("Number of workers")
    ax.set_ylabel("Total time (seconds)")
    ax.set_title(f"Time to complete {FIXED_QUERY_COUNT} queries")
    ax.legend()
    ax.grid(alpha=0.3)
