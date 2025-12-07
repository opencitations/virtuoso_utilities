from concurrent.futures import ProcessPoolExecutor, as_completed

import pytest

from .conftest import (
    FIXED_QUERY_COUNT,
    PARALLELISM_LEVELS,
    QUERY_TIMEOUT,
    SAMPLE_BR_URIS,
    SAMPLE_DOIS,
    SAMPLE_VVIS,
    build_identifier_lookup_query,
    build_spo_query,
    build_vvi_query,
    execute_query_in_process,
)


def run_fixed_queries(queries: list, num_workers: int) -> dict:
    results = []
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(execute_query_in_process, q): i for i, q in enumerate(queries)}
        for future in as_completed(futures):
            result = future.result(timeout=QUERY_TIMEOUT)
            results.append(result)
    return {"results": results}


@pytest.mark.benchmark(group="fixed_spo")
@pytest.mark.parametrize("num_workers", PARALLELISM_LEVELS)
def test_benchmark_fixed_spo(benchmark, num_workers):
    uris = SAMPLE_BR_URIS[:FIXED_QUERY_COUNT]
    queries = [build_spo_query([uri]) for uri in uris]

    result = benchmark(run_fixed_queries, queries, num_workers)
    benchmark.extra_info["num_workers"] = num_workers
    benchmark.extra_info["total_queries"] = FIXED_QUERY_COUNT
    benchmark.extra_info["success_count"] = len(result["results"])


@pytest.mark.benchmark(group="fixed_doi")
@pytest.mark.parametrize("num_workers", PARALLELISM_LEVELS)
def test_benchmark_fixed_doi(benchmark, num_workers):
    dois = SAMPLE_DOIS[:FIXED_QUERY_COUNT]
    queries = [build_identifier_lookup_query("doi", doi) for doi in dois]

    result = benchmark(run_fixed_queries, queries, num_workers)
    benchmark.extra_info["num_workers"] = num_workers
    benchmark.extra_info["total_queries"] = FIXED_QUERY_COUNT
    benchmark.extra_info["success_count"] = len(result["results"])


@pytest.mark.benchmark(group="fixed_vvi")
@pytest.mark.parametrize("num_workers", PARALLELISM_LEVELS)
def test_benchmark_fixed_vvi(benchmark, num_workers):
    vvis = SAMPLE_VVIS[:FIXED_QUERY_COUNT]
    queries = [build_vvi_query(venue, volume, issue) for venue, volume, issue in vvis]

    result = benchmark(run_fixed_queries, queries, num_workers)
    benchmark.extra_info["num_workers"] = num_workers
    benchmark.extra_info["total_queries"] = FIXED_QUERY_COUNT
    benchmark.extra_info["success_count"] = len(result["results"])


@pytest.mark.benchmark(group="fixed_mixed")
@pytest.mark.parametrize("num_workers", PARALLELISM_LEVELS)
def test_benchmark_fixed_mixed(benchmark, num_workers):
    queries = []
    spo_count = FIXED_QUERY_COUNT // 3
    doi_count = FIXED_QUERY_COUNT // 3
    vvi_count = FIXED_QUERY_COUNT - spo_count - doi_count

    for uri in SAMPLE_BR_URIS[:spo_count]:
        queries.append(build_spo_query([uri]))
    for doi in SAMPLE_DOIS[:doi_count]:
        queries.append(build_identifier_lookup_query("doi", doi))
    for vvi in SAMPLE_VVIS[:vvi_count]:
        queries.append(build_vvi_query(*vvi))

    result = benchmark(run_fixed_queries, queries, num_workers)
    benchmark.extra_info["num_workers"] = num_workers
    benchmark.extra_info["total_queries"] = FIXED_QUERY_COUNT
    benchmark.extra_info["success_count"] = len(result["results"])
