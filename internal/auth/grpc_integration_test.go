package auth

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// pingServer is the smallest real gRPC service available in this module's dependency
// tree (grpc_health_v1, already pulled in transitively by google.golang.org/grpc itself)
// -- using it instead of Consensa's own generated services keeps this package's tests
// from depending on api/consensa/v1, which would be a real import-cycle-shaped layering
// violation (internal/auth is meant to be usable by, not dependent on, the API layer).
type pingServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (pingServer) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// dialWithAuth starts a real bufconn gRPC server with tokenAuth's interceptors installed
// -- proving the actual wire path (UnaryServerInterceptor wired via grpc.NewServer,
// exactly as cmd/consensa/main.go wires it), not just calling authorize directly the way
// token_test.go's unit tests do.
func dialWithAuth(t *testing.T, tokenAuth *TokenAuth, dialOpts ...grpc.DialOption) (client grpc_health_v1.HealthClient, closeAll func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(tokenAuth.UnaryInterceptor),
		grpc.ChainStreamInterceptor(tokenAuth.StreamInterceptor),
	)
	grpc_health_v1.RegisterHealthServer(grpcServer, pingServer{})
	go func() { _ = grpcServer.Serve(listener) }()

	opts := append([]grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, dialOpts...)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatal(err)
	}
	return grpc_health_v1.NewHealthClient(conn), func() {
		_ = conn.Close()
		grpcServer.Stop()
	}
}

// TestUnaryInterceptorRejectsMissingToken proves a real gRPC call over a real server with
// TokenAuth's interceptor installed is actually rejected -- the end-to-end path
// token_test.go's authorize-only tests cannot exercise, since they never construct a real
// grpc.Server or send a real request over the wire.
func TestUnaryInterceptorRejectsMissingToken(t *testing.T) {
	client, closeAll := dialWithAuth(t, NewTokenAuth("s3cret", ""))
	defer closeAll()

	_, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err == nil {
		t.Fatal("expected the call to be rejected without a token")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("error code = %v, want Unauthenticated", status.Code(err))
	}
}

// TestUnaryInterceptorAcceptsBearerCredentials proves the client-side BearerCredentials
// helper and the server-side TokenAuth interceptor actually interoperate over a real
// connection, end to end.
func TestUnaryInterceptorAcceptsBearerCredentials(t *testing.T) {
	client, closeAll := dialWithAuth(t, NewTokenAuth("s3cret", ""), grpc.WithPerRPCCredentials(NewBearerCredentials("s3cret")))
	defer closeAll()

	resp, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check with correct credentials: %v", err)
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("Status = %v, want SERVING", resp.Status)
	}
}

// TestUnaryInterceptorRejectsWrongBearerCredentials proves a client presenting the wrong
// token is rejected exactly like one presenting none at all -- Enabled auth does not
// silently accept an arbitrary credential merely because one was attached.
func TestUnaryInterceptorRejectsWrongBearerCredentials(t *testing.T) {
	client, closeAll := dialWithAuth(t, NewTokenAuth("s3cret", ""), grpc.WithPerRPCCredentials(NewBearerCredentials("wrong")))
	defer closeAll()

	_, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("error code = %v, want Unauthenticated", status.Code(err))
	}
}

// TestUnaryInterceptorDisabledAllowsUnauthenticatedCall proves the default, disabled
// configuration (empty token) never rejects a real call -- the deployment-compatibility
// guarantee NewTokenAuth's own doc comment states.
func TestUnaryInterceptorDisabledAllowsUnauthenticatedCall(t *testing.T) {
	client, closeAll := dialWithAuth(t, NewTokenAuth("", ""))
	defer closeAll()

	if _, err := client.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("disabled auth rejected an unauthenticated call: %v", err)
	}
}
