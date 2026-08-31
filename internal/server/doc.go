// Package server exposes Consensa's client-facing gRPC service. It validates untrusted
// requests before passing them to the index and returns explicit gRPC errors rather than
// allowing malformed vectors to reach storage or consensus code.
package server
