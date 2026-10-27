// Package server is the gRPC service clients talk to. It checks each request (IDs,
// vector length, no NaN) and returns proper gRPC errors before anything reaches the
// index or Raft.
package server
