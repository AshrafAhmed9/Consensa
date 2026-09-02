package auth

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// dataMethod and adminMethod are realistic gRPC full method names, matching what
// grpc.UnaryServerInfo/StreamServerInfo actually carry, used to exercise authorize's
// per-service token selection without needing a real gRPC server (grpc_integration_test.go
// covers that end to end).
const (
	dataMethod  = "/consensa.v1.Consensa/Status"
	adminMethod = adminMethodPrefix + "AddReplica"
)

func withToken(token string) context.Context {
	md := metadata.MD{}
	if token != "" {
		md.Set(metadataKey, token)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

// TestDisabledAuthAllowsEverything proves an empty token (the default, matching this
// project's previous unauthenticated behavior) never rejects a call, even one with no
// metadata at all -- existing deployments/tests/demos that never learned about auth must
// keep working unmodified.
func TestDisabledAuthAllowsEverything(t *testing.T) {
	a := NewTokenAuth("", "")
	if a.Enabled() {
		t.Fatal("empty tokens must report Enabled() == false")
	}
	if err := a.authorize(context.Background(), dataMethod); err != nil {
		t.Fatalf("disabled auth rejected a bare context: %v", err)
	}
	if err := a.authorize(withToken("Bearer wrong"), adminMethod); err != nil {
		t.Fatalf("disabled auth rejected a call carrying an unrelated token: %v", err)
	}
}

// TestEnabledAuthRequiresCorrectBearerToken proves the real enforcement path: missing
// metadata, a missing/malformed Authorization value, and a wrong token are all rejected
// with Unauthenticated, and only the exact configured token, in "Bearer <token>" form, is
// accepted. Single-token mode: adminToken empty falls back to dataToken, so both a
// data-plane and an admin method require the same secret.
func TestEnabledAuthRequiresCorrectBearerToken(t *testing.T) {
	a := NewTokenAuth("s3cret", "")
	if !a.Enabled() {
		t.Fatal("non-empty token must report Enabled() == true")
	}

	cases := []struct {
		name    string
		ctx     context.Context
		method  string
		wantErr bool
	}{
		{"no metadata at all", context.Background(), dataMethod, true},
		{"missing authorization key", withToken(""), dataMethod, true},
		{"not a bearer token", metadata.NewIncomingContext(context.Background(), metadata.Pairs(metadataKey, "s3cret")), dataMethod, true},
		{"wrong token", withToken("Bearer nope"), dataMethod, true},
		{"empty bearer token", withToken("Bearer "), dataMethod, true},
		{"correct token, data method", withToken("Bearer s3cret"), dataMethod, false},
		{"correct token, admin method falls back to the same secret", withToken("Bearer s3cret"), adminMethod, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := a.authorize(c.ctx, c.method)
			if c.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				if status.Code(err) != codes.Unauthenticated {
					t.Fatalf("error code = %v, want Unauthenticated", status.Code(err))
				}
				return
			}
			if err != nil {
				t.Fatalf("expected the correct token to be accepted, got: %v", err)
			}
		})
	}
}

// TestAdminTokenScopedIndependently proves --admin-auth-token actually separates
// ConsensaAdmin's required credential from the data plane's: a token valid for
// Consensa/ConsensaKV must NOT be accepted for ConsensaAdmin once a distinct admin token
// is configured, and vice versa -- the real security property this scoping exists for
// (a leaked data-plane credential cannot be used to call membership-change RPCs).
func TestAdminTokenScopedIndependently(t *testing.T) {
	a := NewTokenAuth("data-secret", "admin-secret")

	if err := a.authorize(withToken("Bearer data-secret"), dataMethod); err != nil {
		t.Fatalf("data token rejected for a data-plane method: %v", err)
	}
	if err := a.authorize(withToken("Bearer admin-secret"), adminMethod); err != nil {
		t.Fatalf("admin token rejected for an admin method: %v", err)
	}
	if err := a.authorize(withToken("Bearer data-secret"), adminMethod); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("data token accepted for an admin method: code = %v, want Unauthenticated", status.Code(err))
	}
	if err := a.authorize(withToken("Bearer admin-secret"), dataMethod); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("admin token accepted for a data-plane method: code = %v, want Unauthenticated", status.Code(err))
	}
}

// TestBearerCredentialsRoundTrip proves the client-side helper produces exactly the
// metadata value TokenAuth's server-side check expects, so a client configured with
// NewBearerCredentials(token) and a server configured with NewTokenAuth(token, "")
// actually interoperate rather than merely both compiling.
func TestBearerCredentialsRoundTrip(t *testing.T) {
	creds := NewBearerCredentials("s3cret")
	got, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata: %v", err)
	}
	want := "Bearer s3cret"
	if got[metadataKey] != want {
		t.Fatalf("metadata[%q] = %q, want %q", metadataKey, got[metadataKey], want)
	}
	if creds.RequireTransportSecurity() {
		t.Fatal("RequireTransportSecurity must be false -- see its own doc comment for why")
	}

	a := NewTokenAuth("s3cret", "")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(metadataKey, got[metadataKey]))
	if err := a.authorize(ctx, dataMethod); err != nil {
		t.Fatalf("server rejected a token produced by the client helper: %v", err)
	}
}
