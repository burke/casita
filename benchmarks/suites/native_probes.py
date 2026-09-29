"""Discoverable, fresh-process state and catalog maintenance probes."""
from __future__ import annotations
import argparse
import json
import os
import pathlib
import re
import tempfile
from benchmarks.suites import repository as common
from benchmarks.suites.pack.catalog import build_probe_binary

PROBES = {
    "state-publication": "metadata::benchmarks::benchmark_state_publication",
    "metadata-durability": "metadata::benchmarks::benchmark_metadata_commit_durability",
    "deletion-ordering": "repository::collection_benchmark::benchmark_collection_deletion_ordering",
    "catalog-maintenance": "blob::pack::benchmarks::benchmark_catalog_reclaim_marker_probe",
    "catalog-durability": "blob::pack::benchmarks::benchmark_local_catalog_durable_publication",
    "logical-state": "metadata::wal3_shard::tests::benchmark_logical_state_shards_scale",
}

# Probes outside the default suites of their name prefix.
SUITES = {"deletion-ordering": "collection-and-fsck"}

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", choices=PROBES, default="state-publication")
    parser.add_argument("--probe-binary", type=pathlib.Path)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--entries", type=int, default=65536)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    if min(args.iterations, args.entries, args.repetitions) < 1 or args.writers < 2:
        parser.error("positive counts and at least two writers are required")
    binary = (args.probe_binary or build_probe_binary()).resolve()
    env = {**os.environ, "CASITA_STATE_BENCH_ITERATIONS": str(args.iterations),
        "CASITA_STATE_BENCH_WRITERS": str(args.writers),
        "CASITA_METADATA_DURABILITY_BENCH_ITERATIONS": str(args.iterations),
        "CASITA_DELETION_ORDERING_BENCH_ITERATIONS": str(args.iterations),
        "CASITA_CATALOG_MARKER_BENCH_ITERATIONS": str(args.iterations),
        "CASITA_CATALOG_DURABILITY_BENCH_ITERATIONS": str(args.iterations),
        "CASITA_LOGICAL_STATE_BENCH_ENTRIES": str(args.entries)}
    samples = []
    with tempfile.TemporaryDirectory(prefix="casita-native-probes-") as temporary:
        root = pathlib.Path(temporary)
        environment = common.environment_metadata(root)
        for repetition in range(1, args.repetitions + 1):
            stdout, stderr = root / "stdout", root / "stderr"
            timing = common.measured_command(common.CommandSpec(
                [[str(binary), PROBES[args.probe], "--exact", "--ignored", "--nocapture"]], root, env), stdout, stderr)
            output = stdout.read_text()
            if "1 passed" not in output:
                raise common.BenchmarkError("probe did not execute exactly one passing test")
            metrics = {key: int(value) for key, value in re.findall(r"(?:^|\s)([a-z][a-z0-9_]+) (\d+)(?=\s|$)", output)}
            if not metrics:
                raise common.BenchmarkError("probe emitted no metrics")
            samples.append({"status": "ok", "implementation": "casita", "operation": args.probe,
                "repetition": repetition, **timing, "metrics": metrics, "stdout": output})
    common.write_atomic(args.output, json.dumps({"schema_version": 1,
        "result_schema": "casita.native-probes.v1",
        "suite_id": SUITES.get(args.probe, "blob-backends" if args.probe.startswith("catalog-") else "state-and-publication"),
        "environment": environment,
        "configuration": vars(args) | {"probe_binary": str(binary), "output": str(args.output)},
        "samples": samples}, indent=2) + "\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
