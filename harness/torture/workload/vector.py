"""Placeholder vector workload: checks a fixed, hardcoded ID list, independent of any seed,
nemesis, or the real Go ANN implementation.

This is NOT yet a real chaos-tested workload the way harness/torture/workload/register.py
is (see that file's docstring for the register/cmd/torture pattern this should eventually
follow). Wiring a real one needs a Go driver that inserts/deletes vectors into a real
ann.ReplicatedIndex under a fault-injected raft.Cluster and reports the resulting graph
state -- ReplicatedIndex.commit() currently has no fault-injection hook at all (it always
uses Cluster's always-delivers Propose internally), so that driver does not exist yet.
Until it does, a passing run() here proves nothing about Consensa's vector replication
under partitions or crashes -- it only proves this fixed three-element list has no
duplicates, which is always true.
"""

from harness.torture.checker.invariant import no_duplicate_ids


def run() -> bool:
    return no_duplicate_ids(["a", "b", "c"])
