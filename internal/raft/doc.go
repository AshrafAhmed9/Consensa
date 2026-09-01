// Package raft implements one deterministic Raft replica following Ongaro and Ousterhout.
// A Node consumes messages and explicit ticks, returning persistence, application, and
// networking work in Ready; it never starts a goroutine or performs I/O itself. Voters in
// the configured peer universe can be changed through log-replicated joint consensus.
package raft
