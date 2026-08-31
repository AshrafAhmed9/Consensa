// Package raft implements one deterministic Raft replica following Ongaro and Ousterhout.
// A Node consumes messages and explicit ticks, returning persistence, application, and
// networking work in Ready; it never starts a goroutine or performs I/O itself. This
// package deliberately supports one fixed voter set only—membership changes are later work.
package raft
