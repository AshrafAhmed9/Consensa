# Phase 9: leases and follower reads

## Why does this exist?

Leader-only reads turn every lookup into a single-replica bottleneck. A closed timestamp
lets caught-up followers safely serve reads that are explicitly old enough.

## How does it work?

A lease names an authorized holder and validity interval. A follower read additionally
requires the local applied index to reach the closed timestamp’s index and the requested
timestamp not to exceed its promise.

## What alternatives existed?

ReadIndex quorum confirmation is stronger without clock assumptions, but costs a quorum
round trip on every read. Leases make the clock assumption explicit.

## What tradeoff was made?

Only bounded-staleness reads are authorized. This avoids claiming linearizability for a
follower response that has not performed a leader round trip.

## What can fail?

Clock skew beyond the configured bound invalidates the lease argument. The production
assembly must revoke leases conservatively on uncertainty or leader changes.
