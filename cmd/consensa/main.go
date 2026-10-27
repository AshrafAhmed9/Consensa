// Command consensa starts one replica of the vector store: a Raft host over TCP, backed by
// an on-disk storage engine, with its own HNSW graph (internal/ann.DurableNode) applying
// committed mutations. Every node in a deployment runs this same binary with the same
// --peers list and a different --id.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/server"
	"google.golang.org/grpc"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	fatal := func(message string, args ...any) {
		slog.Error(message, args...)
		os.Exit(1)
	}
	id := flag.Uint64("id", 0, "this node's Raft ID (must be a key in --peers)")
	peersFlag := flag.String("peers", "", `Raft group members and their addresses, e.g. "1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003" -- identical on every node`)
	dataDir := flag.String("data-dir", "", "on-disk directory for this node's storage engine and Raft log (required, unique per node)")
	grpcListen := flag.String("grpc-listen", ":8080", "client-facing gRPC listen address")
	dimension := flag.Int("dimension", 3, "fixed vector dimension")
	tickInterval := flag.Duration("tick-interval", 50*time.Millisecond, "how often this node advances its Raft clock")
	flag.Parse()

	if *id == 0 {
		fatal("invalid startup configuration", "reason", "--id is required and must be nonzero")
	}
	if *dataDir == "" {
		fatal("invalid startup configuration", "reason", "--data-dir is required")
	}
	allPeers, err := parsePeers(*peersFlag)
	if err != nil {
		fatal("invalid peer configuration", "error", err)
	}
	selfID := raft.NodeID(*id)
	selfAddr, ok := allPeers[selfID]
	if !ok {
		fatal("node absent from peer configuration", "node_id", selfID)
	}
	groupPeers := make([]raft.NodeID, 0, len(allPeers))
	transportPeers := make(map[raft.NodeID]string, len(allPeers)-1)
	for peerID, addr := range allPeers {
		groupPeers = append(groupPeers, peerID)
		if peerID != selfID {
			transportPeers[peerID] = addr
		}
	}
	sort.Slice(groupPeers, func(i, j int) bool { return groupPeers[i] < groupPeers[j] })

	node, err := ann.NewDurableNode(ann.DurableNodeConfig{
		ID: selfID, GroupPeers: groupPeers, ListenAddress: selfAddr, TransportPeers: transportPeers,
		StorageDir: *dataDir, Index: ann.Config{Dimension: *dimension, M: 16, EFConstruction: 64, EFSearch: 64, Seed: 1},
	})
	if err != nil {
		fatal("starting durable node", "error", err)
	}
	defer func() {
		if err := node.Close(); err != nil {
			slog.Error("closing durable node", "error", err)
		}
	}()

	stopTicking := make(chan struct{})
	defer close(stopTicking)
	go func() {
		ticker := time.NewTicker(*tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopTicking:
				return
			case <-ticker.C:
				_ = node.Tick()
			}
		}
	}()

	listener, err := net.Listen("tcp", *grpcListen)
	if err != nil {
		fatal("starting gRPC listener", "error", err, "address", *grpcListen)
	}
	grpcServer := grpc.NewServer()
	consensav1.RegisterConsensaServer(grpcServer, server.NewService(node))
	slog.Info("consensa node started", "node_id", selfID, "raft_address", selfAddr, "grpc_address", *grpcListen, "data_dir", *dataDir)
	if err := grpcServer.Serve(listener); err != nil {
		fatal("gRPC server stopped", "error", err)
	}
}

func parsePeers(raw string) (map[raft.NodeID]string, error) {
	if raw == "" {
		return nil, fmt.Errorf("must not be empty")
	}
	peers := map[raft.NodeID]string{}
	for _, entry := range strings.Split(raw, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("malformed entry %q, want id=host:port", entry)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("malformed node ID in %q: %v", entry, err)
		}
		peers[raft.NodeID(id)] = parts[1]
	}
	return peers, nil
}
