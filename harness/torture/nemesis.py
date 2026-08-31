"""Seeded fault schedule generation independent from the system under test."""

from dataclasses import dataclass
import random


@dataclass(frozen=True)
class Fault:
    step: int
    kind: str
    target: int | None = None


def schedule(seed: int, kinds: list[str], steps: int, nodes: int = 3) -> list[Fault]:
    """Generates the same fault decisions for the same seed and configuration."""
    rng = random.Random(seed)
    return [Fault(step, rng.choice(kinds), rng.randrange(nodes)) for step in range(steps) if kinds and rng.random() < .25]
