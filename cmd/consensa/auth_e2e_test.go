package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestConsensaBinaryEnforcesAuthTokenWhenConfigured proves --auth-token is real
// enforcement inside the actual shipped binary, not just internal/auth's own unit and
// bufconn-level tests: a single real consensa process started with --auth-token rejects
// every RPC missing or presenting the wrong bearer token, and accepts the correct one --
// over a real TCP gRPC connection, through the real interceptor chain cmd/consensa/main.go
// wires (grpc.ChainUnaryInterceptor(tokenAuth.UnaryInterceptor)). A single-node "cluster"
// is enough here: this test is about the auth layer, not about leader election, and
// Service.Status answers regardless of leadership.
func TestConsensaBinaryEnforcesAuthTokenWhenConfigured(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs a real OS process; skipped in -short mode")
	}

	binDir := t.TempDir()
	binary := binPath(t, binDir)

	raftAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	node := &e2eNode{
		id: 1, raftAddr: raftAddr,
		grpcAddr:   fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		metricAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		dataDir:    t.TempDir(),
		binary:     binary,
		peersFlag:  fmt.Sprintf("1=%s", raftAddr),
		extraArgs:  []string{"-auth-token", "s3cret-e2e-token"},
	}
	node.start(t)
	defer node.kill(t)
	waitForListening(t, node.grpcAddr, 10*time.Second)

	dialUnauthenticated := func(t *testing.T) consensav1.ConsensaClient {
		t.Helper()
		conn, err := grpc.NewClient(node.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return consensav1.NewConsensaClient(conn)
	}
	dialWithToken := func(t *testing.T, token string) consensav1.ConsensaClient {
		t.Helper()
		conn, err := grpc.NewClient(node.grpcAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(auth.NewBearerCredentials(token)),
		)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return consensav1.NewConsensaClient(conn)
	}

	statusCall := func(t *testing.T, client consensav1.ConsensaClient) error {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := client.Status(ctx, &consensav1.StatusRequest{})
		return err
	}

	t.Run("no credentials rejected", func(t *testing.T) {
		err := statusCall(t, dialUnauthenticated(t))
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("Status without credentials: code = %v, want Unauthenticated (err: %v)", status.Code(err), err)
		}
	})

	t.Run("wrong token rejected", func(t *testing.T) {
		err := statusCall(t, dialWithToken(t, "wrong-token"))
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("Status with wrong token: code = %v, want Unauthenticated (err: %v)", status.Code(err), err)
		}
	})

	t.Run("correct token accepted", func(t *testing.T) {
		if err := statusCall(t, dialWithToken(t, "s3cret-e2e-token")); err != nil {
			t.Fatalf("Status with correct token: %v", err)
		}
	})
}

// TestConsensaBinaryScopesAdminTokenIndependently proves --admin-auth-token, configured
// separately from --auth-token, actually separates ConsensaAdmin's credential from the
// data plane's inside the real shipped binary: a client holding only the data-plane token
// can call Status but is rejected calling AddReplica, and vice versa for the admin token
// -- the real security property this scoping exists for (a leaked data-plane credential
// cannot be used to drive membership changes).
func TestConsensaBinaryScopesAdminTokenIndependently(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs a real OS process; skipped in -short mode")
	}

	binDir := t.TempDir()
	binary := binPath(t, binDir)

	raftAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	node := &e2eNode{
		id: 1, raftAddr: raftAddr,
		grpcAddr:   fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		metricAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		dataDir:    t.TempDir(),
		binary:     binary,
		peersFlag:  fmt.Sprintf("1=%s", raftAddr),
		extraArgs:  []string{"-auth-token", "data-secret", "-admin-auth-token", "admin-secret"},
	}
	node.start(t)
	defer node.kill(t)
	waitForListening(t, node.grpcAddr, 10*time.Second)

	dialWith := func(t *testing.T, token string) *grpc.ClientConn {
		t.Helper()
		conn, err := grpc.NewClient(node.grpcAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(auth.NewBearerCredentials(token)),
		)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	addReplica := func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := consensav1.NewConsensaAdminClient(conn).AddReplica(ctx, &consensav1.AddReplicaRequest{RangeId: 1, NodeId: 99, Address: "127.0.0.1:1"})
		return err
	}

	t.Run("data token calls Status but not AddReplica", func(t *testing.T) {
		conn := dialWith(t, "data-secret")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := consensav1.NewConsensaClient(conn).Status(ctx, &consensav1.StatusRequest{}); err != nil {
			t.Fatalf("Status with data token: %v", err)
		}
		if err := addReplica(ctx, conn); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("AddReplica with data token: code = %v, want Unauthenticated (err: %v)", status.Code(err), err)
		}
	})

	t.Run("admin token calls AddReplica but not Status", func(t *testing.T) {
		conn := dialWith(t, "admin-secret")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// AddReplica may still fail for an ordinary business reason (a bogus range ID
		// here) -- the point is it must NOT fail with Unauthenticated, proving the admin
		// token really was accepted by the auth layer before reaching the handler.
		if err := addReplica(ctx, conn); status.Code(err) == codes.Unauthenticated {
			t.Fatalf("AddReplica with admin token was rejected as Unauthenticated: %v", err)
		}
		if _, err := consensav1.NewConsensaClient(conn).Status(ctx, &consensav1.StatusRequest{}); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("Status with admin token: code = %v, want Unauthenticated (err: %v)", status.Code(err), err)
		}
	})
}

// TestConsensaBinaryWithoutAuthTokenAllowsUnauthenticatedCalls proves the default,
// backward-compatible behavior still holds inside the real binary: a node started WITHOUT
// --auth-token accepts calls carrying no credentials at all, exactly as every existing
// e2e test and the demo client already assume.
func TestConsensaBinaryWithoutAuthTokenAllowsUnauthenticatedCalls(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs a real OS process; skipped in -short mode")
	}

	binDir := t.TempDir()
	binary := binPath(t, binDir)

	raftAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	node := &e2eNode{
		id: 1, raftAddr: raftAddr,
		grpcAddr:   fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		metricAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		dataDir:    t.TempDir(),
		binary:     binary,
		peersFlag:  fmt.Sprintf("1=%s", raftAddr),
	}
	node.start(t)
	defer node.kill(t)
	waitForListening(t, node.grpcAddr, 10*time.Second)

	client := dialNode(t, node.grpcAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Status(ctx, &consensav1.StatusRequest{}); err != nil {
		t.Fatalf("Status with no --auth-token configured: %v", err)
	}
}
