"""Vector workload: drives real internal/ann.HNSW replicas (via cmd/vectortorture), kept
in sync through a real internal/raft.Cluster, under a seeded fault schedule -- the vector
counterpart to register.py's real Cluster-driven register workload.

Before this existed, run() checked a fixed, hardcoded three-element ID list, independent
of the seed, the nemesis schedule, or the real Go HNSW implementation at all -- a passing
run proved only that ["a", "b", "c"] has no duplicates, which is always true.
ann.ReplicatedIndex (the existing in-memory Raft+HNSW composition) could not be reused
directly to fix this: its commit() always uses Cluster.Propose, which always delivers
every message, and it errors if any replica is missing a just-proposed entry -- exactly
what a fault schedule needs to be able to happen. cmd/vectortorture drives
internal/raft.Cluster and internal/ann.HNSW directly instead, the same layer cmd/torture
already works at.

What this checks, and what it does not: after the run, every replica that applied the
same number of committed mutations must have a byte-identical graph snapshot (the
replication-determinism property ann.ReplicatedIndex's own tests prove without chaos, now
proven under real partitions and crashes too), and no replica's graph may contain a
duplicate ID. It does not check recall or search quality -- that is what
harness/bench/run_recall_benchmark.py is for, and conflating the two would misrepresent
what either proves (see docs/correctness.md's "Claims discipline" section).
"""

import json
import subprocess
from pathlib import Path

from harness.torture.checker.invariant import no_duplicate_ids
from harness.torture.nemesis import schedule

REPO_ROOT = Path(__file__).resolve().parents[3]
_BINARY_PATH: Path | None = None


def _binary() -> Path:
    """Builds cmd/vectortorture once per process and reuses it, mirroring
    register.py's _binary()."""
    global _BINARY_PATH
    if _BINARY_PATH is None:
        out = REPO_ROOT / "harness" / "torture" / ".vectortorture-bin"
        result = subprocess.run(
            ["go", "build", "-o", str(out), "./cmd/vectortorture"],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"building cmd/vectortorture failed:\n{result.stderr}")
        _BINARY_PATH = out
    return _BINARY_PATH


def run(seed: int = 0, nemeses: list[str] | None = None, steps: int = 20, nodes: int = 3) -> bool:
    """Drives real HNSW replicas under a seeded fault schedule and checks that every
    replica which applied the same number of mutations ended up with an identical graph,
    and that no replica's graph has a duplicate ID.
    """
    faults = schedule(seed, nemeses or [], steps, nodes)
    faults_json = json.dumps([f.__dict__ for f in faults])

    result = subprocess.run(
        [str(_binary()), "-nodes", str(nodes), "-rounds", str(steps)],
        input=faults_json, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"cmd/vectortorture failed for seed={seed} nemeses={nemeses}:\n{result.stderr}")

    report = json.loads(result.stdout)
    by_applied: dict[int, list[dict]] = {}
    for replica in report["replicas"]:
        if not no_duplicate_ids(replica["ids"]):
            return False
        by_applied.setdefault(replica["applied"], []).append(replica)

    for replicas in by_applied.values():
        snapshots = {r["snapshot"] for r in replicas}
        if len(snapshots) != 1:
            return False
    return True
