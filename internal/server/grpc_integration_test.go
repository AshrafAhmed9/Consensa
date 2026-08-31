package server

import (
	"context"
	"net"
	"testing"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/ann"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// TestGRPCUpsertThenSearch verifies the generated public contract, streaming service, and
// ANN implementation compose correctly without relying on a network listener or timing.
func TestGRPCUpsertThenSearch(t *testing.T) {
	index, err := ann.NewHNSW(ann.Config{Dimension: 2, M: 4, EFConstruction: 8, EFSearch: 8, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	consensav1.RegisterConsensaServer(server, NewService(index))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := consensav1.NewConsensaClient(conn)
	upsert, err := client.Upsert(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := upsert.Send(&consensav1.UpsertRequest{Id: "near", Vector: &consensav1.Vector{Values: []float32{1, 1}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := upsert.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	upsert, err = client.Upsert(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := upsert.Send(&consensav1.UpsertRequest{Id: "near", Vector: &consensav1.Vector{Values: []float32{4, 4}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := upsert.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	search, err := client.Search(context.Background(), &consensav1.SearchRequest{Query: &consensav1.Vector{Values: []float32{1.1, 1.1}}, K: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err := search.Recv()
	if err != nil || got.Id != "near" || got.Distance < 10 {
		t.Fatalf("Search = %#v, %v", got, err)
	}
	if _, err := client.Delete(context.Background(), &consensav1.DeleteRequest{Id: "near"}); err != nil {
		t.Fatal(err)
	}
	search, err = client.Search(context.Background(), &consensav1.SearchRequest{Query: &consensav1.Vector{Values: []float32{1.1, 1.1}}, K: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := search.Recv(); err == nil {
		t.Fatal("deleted vector remained searchable")
	}
}
