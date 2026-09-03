"""Doctors-on-call workload: drives a real 3-node kv.DurableRange group through
internal/txn.Coordinator (via cmd/doctortorture) under a seeded fault schedule, running
repeated write-skew-shaped transactions, and checks that the invariant this codebase's
write-skew defense exists to protect -- at least one doctor always on call -- never breaks.

Unlike register.py/vector.py, this workload does not check linearizability or replica
convergence -- it checks a domain invariant (see checker/invariant.py's
doctors_always_on_call) against real committed state read back from a real Raft group under
real chaos, the textbook write-skew scenario docs/notes/14-serializable.md and
internal/txn/serializable_test.go's TestWriteIntentRejectsWriteSkew both use, run here at
volume instead of by hand.

Nemesis note: kv.DurableRange (real TCP transport, no message-filtering hook) cannot be
partitioned the way internal/raft.Cluster can for register.py/vector.py -- cmd/doctortorture
treats "partition" and "crash" identically (a real close+reopen of the target node), the
same documented simplification cmd/torture itself uses for the same reason. "clock-skew" is
a new nemesis kind this workload introduces: it perturbs cmd/doctortorture's coordinator
clock by a bounded, seeded offset, exercising DurableStore.ReadAtTimestamp's uncertainty
window (see docs/notes/14-serializable.md).
"""

import json
import subprocess
from pathlib import Path

from harness.torture.checker.invariant import doctors_always_on_call
from harness.torture.nemesis import schedule

REPO_ROOT = Path(__file__).resolve().parents[3]
_BINARY_PATH: Path | None = None


def _binary() -> Path:
    """Builds cmd/doctortorture once per process and reuses it, mirroring
    register.py's/vector.py's own _binary()."""
    global _BINARY_PATH
    if _BINARY_PATH is None:
        out = REPO_ROOT / "harness" / "torture" / ".doctortorture-bin"
        result = subprocess.run(
            ["go", "build", "-o", str(out), "./cmd/doctortorture"],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"building cmd/doctortorture failed:\n{result.stderr}")
        _BINARY_PATH = out
    return _BINARY_PATH


def run(seed: int = 0, nemeses: list[str] | None = None, steps: int = 30, nodes: int = 3, doctors: int = 5) -> bool:
    """Drives real doctors-on-call transactions under a seeded fault schedule and checks
    that the on-call invariant never breaks. The same seed and nemeses always produce the
    same fault schedule (nemesis.schedule is a pure function of them) and the same PRNG
    seed drives cmd/doctortorture's own doctor-pair selection and clock-skew magnitude, so a
    failing seed is reproducible.
    """
    faults = schedule(seed, nemeses or [], steps, nodes)
    faults_json = json.dumps([f.__dict__ for f in faults])

    result = subprocess.run(
        [str(_binary()), "-nodes", str(nodes), "-rounds", str(steps), "-doctors", str(doctors), "-seed", str(seed)],
        input=faults_json, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"cmd/doctortorture failed for seed={seed} nemeses={nemeses}:\n{result.stderr}")

    report = json.loads(result.stdout)
    return doctors_always_on_call(report["violations"])
