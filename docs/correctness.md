# Correctness status

Consensa uses deterministic schedules so a failing seed can be recorded and replayed.
The register checker tests small histories for linearizability. The ANN checker measures
recall separately: approximate search is not, and must not be described as, linearizable.

Current checks cover unit-level Raft election/replication properties, WAL recovery,
canonical ANN snapshots, and deterministic harness schedules. They do not yet prove an
end-to-end distributed cluster; that claim will be added only after the harness drives
real replicated API workloads across the full fault matrix.
