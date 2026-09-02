// Command consensa-cli is the operator tool PLAN.md names ("admin CLI: range inspection,
// membership ops") and, until now, left unbuilt. This first cut implements exactly one
// subcommand, join, closing the specific gap docs/adr/010-learners.md stated plainly:
// "Bootstrap ... remain[s] separate, undone work." Range inspection and the other
// membership operations PLAN.md lists (remove-replica) remain future work, stated here
// rather than implied.
//
// What "bootstrap" actually meant, precisely: internal/raft/new_node_join_test.go and
// internal/server/admin_service_test.go already proved the underlying mechanism works --
// a genuinely new process can join a live Raft group as a learner and be promoted to a
// full voter with zero downtime, over real gRPC. What was missing was purely the
// operator-tooling side: nothing automated the actual sequence (AddReplica against EVERY
// existing replica, then PromoteReplica retried against whichever one is currently
// leader) -- a human had to script that by hand, one gRPC call at a time, against
// whichever addresses they happened to already know. join automates exactly that
// sequence.
//
// join does NOT discover cluster membership on its own -- the operator still supplies
// every existing replica's ConsensaAdmin address, the same way --peers is supplied to
// cmd/consensa itself at deployment time. That is a deliberate, stated scope boundary,
// not an oversight: a new process still needs to be told at least the existing group's
// addresses from somewhere, and inventing a service-discovery mechanism (DNS, gossip, a
// registry) is real, separate, unbuilt work this command does not attempt.
//
// join also only operates on ONE named Raft range/group at a time (--range-id), matching
// AddReplica/PromoteReplica's own per-range scope. A real physical process joining
// cmd/consensa's multi-group deployment (the vector index plus however many KV ranges
// currently exist, including any live-split children) needs join run once per group --
// this command does not attempt to enumerate or join "the whole deployment" in one call,
// since nothing in this project exposes a list of a deployment's current range IDs to
// discover them from either.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: consensa-cli <join> [flags]")
	}
	switch os.Args[1] {
	case "join":
		runJoin(os.Args[2:])
	default:
		log.Fatalf("unknown subcommand %q (only \"join\" exists so far)", os.Args[1])
	}
}

func runJoin(args []string) {
	fs := flag.NewFlagSet("join", flag.ExitOnError)
	rangeID := fs.Uint64("range-id", 0, "the Raft range/group ID the new node is joining (required)")
	newID := fs.Uint64("new-id", 0, "the new node's Raft ID (required)")
	newAddr := fs.String("new-addr", "", "the new node's Raft transport address for this range, e.g. 127.0.0.1:9004 (required)")
	existing := fs.String("existing", "", `every EXISTING replica's ConsensaAdmin gRPC address, as "id=host:port" pairs separated by commas, e.g. "1=127.0.0.1:8081,2=127.0.0.1:8082,3=127.0.0.1:8083" (required) -- AddReplica must reach every one of these (see AdminService.AddReplica's own doc comment for why: it is local, per-replica bookkeeping, not something Raft's own commit protocol replicates)`)
	authToken := fs.String("auth-token", "", "bearer token to present on every call, if the target cluster was started with --admin-auth-token (or --auth-token, if no separate admin token was configured)")
	timeout := fs.Duration("timeout", 30*time.Second, "how long to keep retrying PromoteReplica across --existing addresses before giving up (AddReplica itself is not retried this way -- see runJoin's own comment)")
	if err := fs.Parse(args); err != nil {
		log.Fatal(err)
	}
	if *rangeID == 0 || *newID == 0 || *newAddr == "" || *existing == "" {
		log.Fatal("--range-id, --new-id, --new-addr, and --existing are all required")
	}

	peers, err := parseIDAddrList(*existing)
	if err != nil {
		log.Fatalf("--existing: %v", err)
	}

	voterIDs := make([]uint64, 0, len(peers)+1)
	for id := range peers {
		voterIDs = append(voterIDs, id)
	}
	voterIDs = append(voterIDs, *newID)

	// AddReplica must reach EVERY existing replica -- unlike PromoteReplica, it is not a
	// "retry until you hit the leader" call, since it is local, per-replica bookkeeping
	// that never touches Raft's own commit protocol (AdminService.AddReplica's own doc
	// comment). A replica this fails against would be permanently unable to route to the
	// new node once it eventually becomes a voter, so a partial success here is reported
	// as a hard failure rather than silently continuing to PromoteReplica.
	for id, addr := range peers {
		if err := addReplicaWithRetry(addr, *rangeID, *newID, *newAddr, *authToken, *timeout); err != nil {
			log.Fatalf("AddReplica against node %d (%s): %v", id, addr, err)
		}
		fmt.Printf("AddReplica accepted by node %d (%s)\n", id, addr)
	}

	// PromoteReplica only succeeds against the range's CURRENT leader -- retried across
	// every known address until one accepts or timeout elapses, the same "route to
	// whoever's in charge" contract this project's own TransactionalPut/upsert clients
	// already use (cmd/consensa/main_e2e_test.go's transactionalPutUntilAccepted/
	// upsertUntilAccepted).
	if err := promoteReplicaAnyLeader(peers, *rangeID, voterIDs, *authToken, *timeout); err != nil {
		log.Fatalf("PromoteReplica: %v", err)
	}
	fmt.Printf("node %d promoted to a full voter on range %d (new voter set: %v)\n", *newID, *rangeID, voterIDs)
}

func dialAdmin(addr, token string) (consensav1.ConsensaAdminClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(auth.NewBearerCredentials(token)),
	)
	if err != nil {
		return nil, nil, err
	}
	return consensav1.NewConsensaAdminClient(conn), conn, nil
}

// addReplicaWithRetry retries a single AddReplica call against ONE specific address
// (transient dial/connection issues only -- this is not the multi-address "any leader"
// retry PromoteReplica needs, since AddReplica succeeds on any replica and there is
// nothing to route around here).
func addReplicaWithRetry(addr string, rangeID, newID uint64, newAddr, token string, deadline time.Duration) error {
	client, conn, err := dialAdmin(addr, token)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := client.AddReplica(ctx, &consensav1.AddReplicaRequest{RangeId: rangeID, NodeId: newID, Address: newAddr})
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	return lastErr
}

// promoteReplicaAnyLeader mirrors admin_service_test.go's own retry loop exactly.
func promoteReplicaAnyLeader(peers map[uint64]string, rangeID uint64, voterIDs []uint64, token string, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		for id, addr := range peers {
			client, conn, err := dialAdmin(addr, token)
			if err != nil {
				lastErr = err
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = client.PromoteReplica(ctx, &consensav1.PromoteReplicaRequest{RangeId: rangeID, VoterIds: voterIDs})
			cancel()
			_ = conn.Close()
			if err == nil {
				return nil
			}
			lastErr = fmt.Errorf("node %d (%s): %w", id, addr, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("no replica accepted PromoteReplica within %s: %w", deadline, lastErr)
}

// parseIDAddrList parses "id=host:port,id=host:port,..." -- the same convention
// cmd/consensa's own --peers flag uses (parsePeers, cmd/consensa/main.go), reused here
// rather than imported since cmd/consensa is package main and not importable, and
// duplicating this ~15-line parser is cheaper and clearer than factoring out a shared
// internal package for one small helper both binaries would otherwise need to depend on.
func parseIDAddrList(raw string) (map[uint64]string, error) {
	out := map[uint64]string{}
	for _, entry := range strings.Split(raw, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("malformed entry %q, want id=host:port", entry)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("malformed node ID in %q: %v", entry, err)
		}
		out[id] = parts[1]
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("must name at least one existing node")
	}
	return out, nil
}
