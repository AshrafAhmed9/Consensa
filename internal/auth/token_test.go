package auth

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	a := NewTokenAuth("")
	if a.Enabled() {
		t.Fatal("empty token must report Enabled() == false")
	}
	if err := a.authorize(context.Background()); err != nil {
		t.Fatalf("disabled auth rejected a bare context: %v", err)
	}
	if err := a.authorize(withToken("Bearer wrong")); err != nil {
		t.Fatalf("disabled auth rejected a call carrying an unrelated token: %v", err)
	}
}

// TestEnabledAuthRequiresCorrectBearerToken proves the real enforcement path: missing
// metadata, a missing/malformed Authorization value, and a wrong token are all rejected
// with Unauthenticated, and only the exact configured token, in "Bearer <token>" form, is
// accepted.
func TestEnabledAuthRequiresCorrectBearerToken(t *testing.T) {
	a := NewTokenAuth("s3cret")
	if !a.Enabled() {
		t.Fatal("non-empty token must report Enabled() == true")
	}

	cases := []struct {
		name    string
		ctx     context.Context
		wantErr bool
	}{
		{"no metadata at all", context.Background(), true},
		{"missing authorization key", withToken(""), true},
		{"not a bearer token", metadata.NewIncomingContext(context.Background(), metadata.Pairs(metadataKey, "s3cret")), true},
		{"wrong token", withToken("Bearer nope"), true},
		{"empty bearer token", withToken("Bearer "), true},
		{"correct token", withToken("Bearer s3cret"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := a.authorize(c.ctx)
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

// TestBearerCredentialsRoundTrip proves the client-side helper produces exactly the
// metadata value TokenAuth's server-side check expects, so a client configured with
// NewBearerCredentials(token) and a server configured with NewTokenAuth(token) actually
// interoperate rather than merely both compiling.
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

	a := NewTokenAuth("s3cret")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(metadataKey, got[metadataKey]))
	if err := a.authorize(ctx); err != nil {
		t.Fatalf("server rejected a token produced by the client helper: %v", err)
	}
}
