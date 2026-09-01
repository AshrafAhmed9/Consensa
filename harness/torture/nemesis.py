"""Seeded fault schedule generation independent from the system under test."""

from dataclasses import dataclass
import random


@dataclass(frozen=True)
class Fault:
    step: int
    kind: str
    target: int | None = None


# internal/raft/cluster.go sets ElectionTick to 3 + a per-node stagger (position), so the
# shortest fuse on any node is 3 ticks with no leader contact. A fault window shorter than
# that can never let a node's own election machinery activate at all -- see
# docs/notes/06-torture.md's account of the Figure-8/pre-vote regression tests that a
# single-round fault model could not catch regardless of seed count. MIN_WINDOW is set
# above that floor so a generated window is long enough, by construction, to matter.
MIN_WINDOW = 4
MAX_WINDOW = 10


def schedule(seed: int, kinds: list[str], steps: int, nodes: int = 3) -> list[Fault]:
    """Generates the same fault decisions for the same seed and configuration.

    Faults are emitted as sustained windows -- one target isolated for
    MIN_WINDOW..MAX_WINDOW consecutive steps -- rather than independent single-step
    events, because a one-round isolation reconnects before any node's election timer
    can fire and so can never exercise election-safety code paths (Figure-8, pre-vote).
    """
    rng = random.Random(seed)
    if not kinds:
        return []

    faults: list[Fault] = []
    step = 0
    while step < steps:
        if rng.random() < 0.25:
            kind = rng.choice(kinds)
            target = rng.randrange(nodes)
            window = min(rng.randint(MIN_WINDOW, MAX_WINDOW), steps - step)
            for offset in range(window):
                faults.append(Fault(step + offset, kind, target))
            step += window
        else:
            step += 1
    return faults
