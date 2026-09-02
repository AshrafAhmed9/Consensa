// Package auth is a deliberately minimal shared-secret bearer-token layer for Consensa's
// gRPC surface -- every RPC in this project (Consensa, ConsensaKV, ConsensaAdmin) was
// previously, honestly, documented as unauthenticated (see api/consensa/v1/consensa.proto
// and docs/adr/010-learners.md). This closes that gap with the simplest mechanism that is
// still a real access control, not a full identity system: up to two operator-configured
// secrets -- one for the data plane (Consensa, ConsensaKV), one for ConsensaAdmin's
// membership-change surface -- checked on every RPC via a gRPC interceptor, in constant
// time to avoid a timing side channel on the comparison itself.
//
// What this deliberately does NOT provide, stated plainly rather than implied: there is
// no per-user identity, no per-RPC-method scoping within a plane (a valid data-plane
// token can call every Consensa/ConsensaKV RPC; a valid admin token can call every
// ConsensaAdmin RPC), no token rotation or expiry, and no transport encryption --
// RequireTransportSecurity returns false because nothing in this project's deployment
// story (docker-compose, the e2e tests, the demo) terminates TLS today, so requiring it
// here would just make the token layer unusable rather than more secure. A real
// production deployment MUST put this behind TLS (a reverse proxy or gRPC's own
// credentials.NewTLS) before either token stops traveling in cleartext -- this package
// only prevents an unauthenticated caller from reaching the API; it does not prevent
// network eavesdropping.
package auth

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// metadataKey is the incoming-metadata key a client's bearer token is read from, matching
// HTTP's own conventional header name so a caller (or a future HTTP/gRPC-gateway bridge)
// needs no Consensa-specific convention to learn.
const metadataKey = "authorization"

// bearerPrefix precedes the token value, again matching the HTTP Authorization header's
// own "Bearer <token>" convention rather than inventing a new one.
const bearerPrefix = "Bearer "

// adminMethodPrefix identifies a call as belonging to ConsensaAdmin by its gRPC full
// method name (e.g. "/consensa.v1.ConsensaAdmin/AddReplica") -- grpc.UnaryServerInfo and
// grpc.StreamServerInfo both carry this on every call, which is what lets one shared
// interceptor pair apply a DIFFERENT required token to ConsensaAdmin than to the other
// two services registered on the same grpc.Server, without ConsensaAdmin's own handlers
// needing to know anything about auth at all.
const adminMethodPrefix = "/consensa.v1.ConsensaAdmin/"

// TokenAuth checks every incoming RPC against one of up to two operator-configured
// shared secrets, chosen by which service the call belongs to.
type TokenAuth struct {
	dataToken, adminToken string
}

// NewTokenAuth builds a TokenAuth. dataToken gates Consensa and ConsensaKV; adminToken
// gates ConsensaAdmin. An empty adminToken falls back to dataToken -- the single-secret
// mode this package originally shipped with, and still the right default for a
// deployment that has no reason to separate the two -- so configuring only --auth-token
// keeps protecting every RPC, including admin ones, exactly as before. A deployment that
// wants ConsensaAdmin locked down independently of (or even when) the data plane is open
// sets --admin-auth-token separately; a real, intentional use case, not just a fallback
// path, since an operator's own tooling calling membership-change RPCs is a genuinely
// different trust boundary than an application's ordinary read/write traffic.
//
// An empty dataToken alone (adminToken also empty) disables auth entirely -- Enabled
// reports false and both interceptors below become no-ops -- so a deployment that never
// sets either flag keeps today's default behavior exactly, and every existing e2e test
// and demo client that never learned about auth keeps working unmodified.
func NewTokenAuth(dataToken, adminToken string) *TokenAuth {
	if adminToken == "" {
		adminToken = dataToken
	}
	return &TokenAuth{dataToken: dataToken, adminToken: adminToken}
}

// Enabled reports whether this TokenAuth actually checks anything, on either plane.
func (a *TokenAuth) Enabled() bool { return a.dataToken != "" || a.adminToken != "" }

// requiredToken picks the token a call must present, based on which service fullMethod
// names -- see adminMethodPrefix's own doc comment for why the method name alone is
// enough to decide this without any per-RPC configuration.
func (a *TokenAuth) requiredToken(fullMethod string) string {
	if strings.HasPrefix(fullMethod, adminMethodPrefix) {
		return a.adminToken
	}
	return a.dataToken
}

// authorize is the shared check both interceptors below delegate to: extract the token
// from incoming metadata and compare it against the token fullMethod's service requires.
//
// subtle.ConstantTimeCompare, not ==: a plain string comparison exits as soon as it finds
// the first mismatched byte, so a network attacker who can measure response latency
// precisely enough can recover the token one byte at a time by trying every possible next
// byte and watching which one takes marginally longer to reject -- a real, well-known
// timing side channel for exactly this kind of secret comparison. ConstantTimeCompare
// always examines every byte regardless of where the first mismatch is, closing that
// channel. It still requires equal-length inputs to be meaningful, so the length check
// happens first and is itself safe to short-circuit (byte length is not a secret in
// scope: an attacker capable of running enough timing trials to matter here already knows
// this is a bearer token, and length disclosure alone does not shrink the search space in
// a way that speeds up recovering token bytes -- ConstantTimeCompare's own guarantee is
// only meaningful for comparing content of a KNOWN length, which is exactly this case).
func (a *TokenAuth) authorize(ctx context.Context, fullMethod string) error {
	required := a.requiredToken(fullMethod)
	if required == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "auth: missing metadata")
	}
	values := md.Get(metadataKey)
	if len(values) == 0 {
		return status.Error(codes.Unauthenticated, "auth: missing authorization metadata")
	}
	presented := values[0]
	if len(presented) < len(bearerPrefix) || presented[:len(bearerPrefix)] != bearerPrefix {
		return status.Error(codes.Unauthenticated, "auth: authorization metadata must be a Bearer token")
	}
	presentedToken := presented[len(bearerPrefix):]
	if len(presentedToken) != len(required) || subtle.ConstantTimeCompare([]byte(presentedToken), []byte(required)) != 1 {
		return status.Error(codes.Unauthenticated, "auth: invalid token")
	}
	return nil
}

// UnaryInterceptor is a grpc.UnaryServerInterceptor enforcing authorize on every unary
// call (Delete, BatchGet, Status, TransactionalPut, AddReplica, PromoteReplica).
func (a *TokenAuth) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := a.authorize(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamInterceptor is a grpc.StreamServerInterceptor enforcing authorize on every
// streaming call (Upsert, Search) before the handler ever sees a single message --
// checked once at stream setup, not per-message, since the token is a connection-level
// credential, not a per-request one.
func (a *TokenAuth) StreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := a.authorize(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, ss)
}
