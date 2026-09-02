// Command democlient is a small, real gRPC client used only by demo.sh: it upserts a few
// vectors, searches for the nearest neighbor, and commits a cross-range transactional
// write, against whichever running consensa node it's given. It is not a general-purpose
// CLI (see PLAN.md's own cmd/consensa-cli, which remains unbuilt) -- it exists so the demo
// script can show real client traffic without requiring grpcurl or a hand-written protobuf
// call to be installed on whatever machine runs the demo.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addrs := flag.String("addrs", "", "comma-separated gRPC addresses to try, in order, e.g. 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083")
	action := flag.String("action", "upsert-and-search", "upsert-and-search | search-only | txn")
	timeout := flag.Duration("timeout", 15*time.Second, "how long to keep retrying across addrs before giving up")
	flag.Parse()

	if *addrs == "" {
		log.Fatal("--addrs is required")
	}
	addrList := strings.Split(*addrs, ",")

	switch *action {
	case "upsert-and-search":
		upsertAndSearch(addrList, *timeout)
	case "search-only":
		searchOnly(addrList, *timeout)
	case "txn":
		transactionalPut(addrList, *timeout)
	default:
		log.Fatalf("unknown --action %q", *action)
	}
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// eachAddr retries fn against every address in order, in a loop, until it succeeds or
// deadline expires -- exactly the pattern a real client needs, since a write only ever
// succeeds against the current leader and this demo does not know in advance which node
// that is (see docs/notes/05-api.md: this project deliberately does not implement
// server-side leader forwarding).
func eachAddr(addrs []string, deadline time.Duration, fn func(consensav1.ConsensaClient) error) error {
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		for _, addr := range addrs {
			conn, err := dial(addr)
			if err != nil {
				lastErr = err
				continue
			}
			err = fn(consensav1.NewConsensaClient(conn))
			_ = conn.Close()
			if err == nil {
				return nil
			}
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("no node succeeded within %s: %w", deadline, lastErr)
}

func upsertAndSearch(addrs []string, deadline time.Duration) {
	vectors := map[string][]float32{
		"cat":   {1, 0, 0, 0},
		"dog":   {0.9, 0.1, 0, 0},
		"truck": {0, 0, 1, 0},
	}
	for id, v := range vectors {
		id, v := id, v
		err := eachAddr(addrs, deadline, func(c consensav1.ConsensaClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, err := c.Upsert(ctx)
			if err != nil {
				return err
			}
			if err := stream.Send(&consensav1.UpsertRequest{Id: id, Vector: &consensav1.Vector{Values: v}}); err != nil {
				return err
			}
			_, err = stream.CloseAndRecv()
			return err
		})
		if err != nil {
			log.Fatalf("upsert %q: %v", id, err)
		}
		fmt.Printf("  upserted %-6s %v\n", id, v)
	}

	// Search immediately after upsert can legitimately reach a replica that has not yet
	// applied every write -- Get/Search here are bounded-staleness reads by design (see
	// DurableNode.Search's own doc comment), not linearizable ones. printSearch retries
	// until the expected nearest neighbor actually appears, matching what a real client
	// polling for its own recent write would do, rather than printing whatever a single
	// early, possibly-stale attempt happened to return.
	fmt.Println("\nsearching for the 2 nearest neighbors of (1,0,0,0) -- a \"cat\"-like query:")
	printSearch(addrs, deadline, []float32{1, 0, 0, 0}, 2, "cat")
}

func searchOnly(addrs []string, deadline time.Duration) {
	fmt.Println("searching for the 2 nearest neighbors of (1,0,0,0):")
	printSearch(addrs, deadline, []float32{1, 0, 0, 0}, 2, "cat")
}

// printSearch retries until the top result's ID matches want, or deadline passes -- see
// upsertAndSearch's own comment on why this waits rather than printing a possibly-stale
// first answer.
func printSearch(addrs []string, deadline time.Duration, query []float32, k int, want string) {
	end := time.Now().Add(deadline)
	var results []*consensav1.SearchResult
	var lastErr error
	for time.Now().Before(end) {
		results = nil
		lastErr = eachAddr(addrs, 3*time.Second, func(c consensav1.ConsensaClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, err := c.Search(ctx, &consensav1.SearchRequest{Query: &consensav1.Vector{Values: query}, K: uint32(k)})
			if err != nil {
				return err
			}
			for {
				r, err := stream.Recv()
				if err != nil {
					break
				}
				results = append(results, r)
			}
			if len(results) == 0 {
				return fmt.Errorf("no results")
			}
			return nil
		})
		if lastErr == nil && len(results) > 0 && results[0].Id == want {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil && len(results) == 0 {
		log.Fatalf("search: %v", lastErr)
	}
	for _, r := range results {
		fmt.Printf("  %-6s distance=%.4f\n", r.Id, r.Distance)
	}
}

func transactionalPut(addrs []string, deadline time.Duration) {
	end := time.Now().Add(deadline)
	var lastErr error
	for time.Now().Before(end) {
		for _, addr := range addrs {
			conn, err := dial(addr)
			if err != nil {
				lastErr = err
				continue
			}
			client := consensav1.NewConsensaKVClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err = client.TransactionalPut(ctx, &consensav1.TransactionalPutRequest{
				TxnId: fmt.Sprintf("demo-%d", time.Now().UnixNano()),
				Writes: map[string][]byte{
					"apple": []byte("fruit"),  // sorts before the default "m" split key
					"zebra": []byte("animal"), // sorts after it -- proves two real ranges committed atomically
				},
			})
			cancel()
			_ = conn.Close()
			if err == nil {
				fmt.Println("  committed a transaction spanning two independent Raft-replicated ranges:")
				fmt.Println("    apple -> fruit  (range [\"\", \"m\"))")
				fmt.Println("    zebra -> animal (range [\"m\", \"\"))")
				return
			}
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Fprintf(os.Stderr, "transactional put never committed: %v\n", lastErr)
	os.Exit(1)
}
