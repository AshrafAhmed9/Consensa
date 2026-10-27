// Command democlient is a small gRPC client used by demo.sh. Writes only succeed on the
// Raft leader, so every command is given all node addresses and tries them in turn.
//
//	democlient status -addrs a,b,c
//	democlient upsert -addrs a,b,c -id cat -vec 1,0,0,0
//	democlient search -addr a -vec 1,0,0,0 -k 3 [-expect cat]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		fatal("usage: democlient status|upsert|search [flags]")
	}
	switch os.Args[1] {
	case "status":
		status(os.Args[2:])
	case "upsert":
		upsert(os.Args[2:])
	case "search":
		search(os.Args[2:])
	default:
		fatal("unknown command " + os.Args[1])
	}
}

// dial opens a connection to one node.
func dial(addr string) consensav1.ConsensaClient {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err.Error())
	}
	return consensav1.NewConsensaClient(conn)
}

// parseVector turns "1,0,0,0" into a vector.
func parseVector(s string) []float32 {
	var out []float32
	for _, part := range strings.Split(s, ",") {
		f, err := strconv.ParseFloat(strings.TrimSpace(part), 32)
		if err != nil {
			fatal("bad vector: " + s)
		}
		out = append(out, float32(f))
	}
	return out
}

// status prints each node's role.
func status(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	addrs := fs.String("addrs", "", "comma-separated gRPC addresses")
	_ = fs.Parse(args)
	for _, addr := range strings.Split(*addrs, ",") {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := dial(addr).Status(ctx, &consensav1.StatusRequest{})
		cancel()
		if err != nil {
			fmt.Printf("  %s  down\n", addr)
			continue
		}
		fmt.Printf("  %s  %s (term %d)\n", addr, resp.Role, resp.Term)
	}
}

// upsert tries every node until the leader accepts the vector.
func upsert(args []string) {
	fs := flag.NewFlagSet("upsert", flag.ExitOnError)
	addrs := fs.String("addrs", "", "comma-separated gRPC addresses")
	id := fs.String("id", "", "vector ID")
	vec := fs.String("vec", "", "vector, e.g. 1,0,0,0")
	timeout := fs.Duration("timeout", 15*time.Second, "give up after this long")
	_ = fs.Parse(args)
	values := parseVector(*vec)
	deadline := time.Now().Add(*timeout)
	for time.Now().Before(deadline) {
		for _, addr := range strings.Split(*addrs, ",") {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			stream, err := dial(addr).Upsert(ctx)
			if err == nil {
				err = stream.Send(&consensav1.UpsertRequest{Id: *id, Vector: &consensav1.Vector{Values: values}})
			}
			if err == nil {
				_, err = stream.CloseAndRecv()
			}
			cancel()
			if err == nil {
				fmt.Printf("  stored %q via %s\n", *id, addr)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	fatal("no node accepted the write")
}

// search asks one node for the k nearest vectors. With -expect, it keeps asking until
// the nearest result is the expected ID, which lets a node that is catching up finish.
func search(args []string) {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	addr := fs.String("addr", "", "gRPC address of the node to ask")
	vec := fs.String("vec", "", "query vector, e.g. 1,0,0,0")
	k := fs.Int("k", 3, "how many results")
	expect := fs.String("expect", "", "wait until this ID is the nearest result")
	timeout := fs.Duration("timeout", 15*time.Second, "give up after this long (with -expect)")
	_ = fs.Parse(args)
	query := parseVector(*vec)
	deadline := time.Now().Add(*timeout)
	for {
		results := runSearch(*addr, query, *k)
		if *expect == "" || (len(results) > 0 && results[0].Id == *expect) {
			for _, r := range results {
				fmt.Printf("  %-8s distance %.3f\n", r.Id, r.Distance)
			}
			return
		}
		if time.Now().After(deadline) {
			fatal(fmt.Sprintf("node %s never returned %q as nearest", *addr, *expect))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// runSearch runs one search and returns the results (empty on any error).
func runSearch(addr string, query []float32, k int) []*consensav1.SearchResult {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := dial(addr).Search(ctx, &consensav1.SearchRequest{Query: &consensav1.Vector{Values: query}, K: uint32(k), Ef: 20})
	if err != nil {
		return nil
	}
	var out []*consensav1.SearchResult
	for {
		r, err := stream.Recv()
		if err != nil {
			return out
		}
		out = append(out, r)
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "democlient:", msg)
	os.Exit(1)
}
