package auth

import "context"

// BearerCredentials implements credentials.PerRPCCredentials, letting a gRPC client
// attach a token to every call by passing grpc.WithPerRPCCredentials(NewBearerCredentials(token))
// once at dial time, rather than every call site individually threading a token through
// metadata.AppendToOutgoingContext -- the same "configure once at the client" shape
// TokenAuth gives the server side.
type BearerCredentials struct {
	token string
}

// NewBearerCredentials builds a BearerCredentials for token. An empty token still
// produces a valid credentials.PerRPCCredentials that attaches an empty Authorization
// value -- harmless against a server with auth disabled (TokenAuth.Enabled false skips
// the check entirely) and correctly rejected as "invalid token" against one that isn't,
// which is the right failure mode for a client that forgot to configure a token.
func NewBearerCredentials(token string) BearerCredentials { return BearerCredentials{token: token} }

// GetRequestMetadata attaches this credential's bearer token to every outgoing call.
func (c BearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{metadataKey: bearerPrefix + c.token}, nil
}

// RequireTransportSecurity returns false, matching TokenAuth's own doc comment: this
// project's deployment story does not terminate TLS anywhere today, so requiring it here
// would make the credential unusable rather than safer. A real deployment needs TLS
// configured at the transport level (grpc.WithTransportCredentials) independently of this
// type; RequireTransportSecurity existing at all is gRPC's own hook for a credential that
// should refuse to run over a plaintext connection, which this one deliberately does not
// enforce, consistent with the rest of this project's own transport choices.
func (c BearerCredentials) RequireTransportSecurity() bool { return false }
