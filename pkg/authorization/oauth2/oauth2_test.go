package oauth2

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const keyID = "key-1"

func newKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return key
}

// newJWKSServer serves the public key as a JWK set.
func newJWKSServer(t *testing.T, key *rsa.PrivateKey) *httptest.Server {
	t.Helper()
	jwks := map[string]any{"keys": []map[string]any{{
		"kty": "RSA",
		"kid": keyID,
		"alg": "RS256",
		"use": "sig",
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	t.Cleanup(server.Close)
	return server
}

func sign(t *testing.T, key *rsa.PrivateKey, method jwt.SigningMethod, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(method, claims)
	token.Header["kid"] = keyID
	signed, err := token.SignedString(key)
	require.NoError(t, err)
	return signed
}

func newAuthorization(t *testing.T, key *rsa.PrivateKey, options ...Option) *ClientCredentialsAuthorization {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	authorization, err := NewClientCredentialsAuthorization(ctx, newJWKSServer(t, key).URL, options...)
	require.NoError(t, err)
	return authorization
}

func Test_ExtractCallerID(t *testing.T) {
	key := newKey(t)
	authorization := newAuthorization(t, key)
	assert.Equal(t, Type, authorization.Type())
	future := time.Now().Add(time.Hour).Unix()

	id, err := authorization.ExtractCallerID(context.Background(), "Bearer "+sign(t, key, jwt.SigningMethodRS256, jwt.MapClaims{"sub": "cp-1", "exp": future}))
	require.NoError(t, err)
	assert.Equal(t, "cp-1", id)

	otherKey := newKey(t)
	invalid := map[string]string{
		"wrong key":          "Bearer " + sign(t, otherKey, jwt.SigningMethodRS256, jwt.MapClaims{"sub": "cp-1", "exp": future}),
		"expired":            "Bearer " + sign(t, key, jwt.SigningMethodRS256, jwt.MapClaims{"sub": "cp-1", "exp": time.Now().Add(-time.Minute).Unix()}),
		"no expiry":          "Bearer " + sign(t, key, jwt.SigningMethodRS256, jwt.MapClaims{"sub": "cp-1"}),
		"missing sub":        "Bearer " + sign(t, key, jwt.SigningMethodRS256, jwt.MapClaims{"exp": future}),
		"not a bearer":       "Basic dXNlcjpwYXNz",
		"empty bearer":       "Bearer ",
		"garbage":            "Bearer not-a-jwt",
		"alg none":           "Bearer " + unsignedToken(t, jwt.MapClaims{"sub": "cp-1", "exp": future}),
		"disallowed alg":     "Bearer " + sign(t, key, jwt.SigningMethodRS256, jwt.MapClaims{"sub": "cp-1", "exp": future}),
		"not yet valid":      "Bearer " + sign(t, key, jwt.SigningMethodRS256, jwt.MapClaims{"sub": "cp-1", "exp": future, "nbf": future}),
		"missing kid in set": "Bearer " + signWithKID(t, key, "unknown", jwt.MapClaims{"sub": "cp-1", "exp": future}),
	}
	restricted := newAuthorization(t, key, WithAlgorithms("ES256"))
	for name, header := range invalid {
		t.Run(name, func(t *testing.T) {
			target := authorization
			if name == "disallowed alg" {
				target = restricted
			}
			_, err := target.ExtractCallerID(context.Background(), header)
			assert.ErrorIs(t, err, dsdk.ErrUnauthorized)
		})
	}
}

func Test_ExtractCallerID_IssuerAndAudience(t *testing.T) {
	key := newKey(t)
	authorization := newAuthorization(t, key, WithIssuer("https://idp.example.com"), WithAudience("dataplane"))
	future := time.Now().Add(time.Hour).Unix()

	valid := sign(t, key, jwt.SigningMethodRS256, jwt.MapClaims{"sub": "cp-1", "exp": future, "iss": "https://idp.example.com", "aud": "dataplane"})
	id, err := authorization.ExtractCallerID(context.Background(), "Bearer "+valid)
	require.NoError(t, err)
	assert.Equal(t, "cp-1", id)

	for _, claims := range []jwt.MapClaims{
		{"sub": "cp-1", "exp": future, "iss": "https://other.example.com", "aud": "dataplane"},
		{"sub": "cp-1", "exp": future, "iss": "https://idp.example.com", "aud": "other"},
		{"sub": "cp-1", "exp": future},
	} {
		_, err := authorization.ExtractCallerID(context.Background(), "Bearer "+sign(t, key, jwt.SigningMethodRS256, claims))
		assert.ErrorIs(t, err, dsdk.ErrUnauthorized)
	}
}

func Test_New_RequiresKeySource(t *testing.T) {
	_, err := NewClientCredentialsAuthorization(context.Background(), "")
	assert.Error(t, err)
}

// --- outbound ---

type tokenServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []url.Values
}

func newTokenServer(t *testing.T, expiresIn int) *tokenServer {
	t.Helper()
	ts := &tokenServer{}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		ts.mu.Lock()
		ts.requests = append(ts.requests, r.PostForm)
		n := len(ts.requests)
		ts.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "token-" + string(rune('0'+n)),
			"token_type":   "Bearer",
			"expires_in":   expiresIn,
		})
	}))
	t.Cleanup(ts.Close)
	return ts
}

func (ts *tokenServer) received() []url.Values {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return append([]url.Values(nil), ts.requests...)
}

func entry(tokenEndpoint string, extra map[string]any) dsdk.AuthorizationEntry {
	properties := map[string]any{
		PropertyTokenEndpoint: tokenEndpoint,
		PropertyClientID:      "client-1",
		PropertyClientSecret:  "s3cr3t",
	}
	for k, v := range extra {
		properties[k] = v
	}
	return dsdk.AuthorizationEntry{Type: Type, Properties: properties}
}

func Test_AuthorizationHeader(t *testing.T) {
	tokens := newTokenServer(t, 3600)
	authorization := newAuthorization(t, newKey(t))

	header, err := authorization.AuthorizationHeader(context.Background(), entry(tokens.URL, map[string]any{PropertyScope: "signaling admin"}))
	require.NoError(t, err)
	assert.Equal(t, "Bearer token-1", header)

	requests := tokens.received()
	require.Len(t, requests, 1)
	assert.Equal(t, "client_credentials", requests[0].Get("grant_type"))
	assert.Equal(t, "client-1", requests[0].Get("client_id"))
	assert.Equal(t, "s3cr3t", requests[0].Get("client_secret"))
	assert.Equal(t, "signaling admin", requests[0].Get("scope"))

	// the token is cached until it expires
	header, err = authorization.AuthorizationHeader(context.Background(), entry(tokens.URL, map[string]any{PropertyScope: "signaling admin"}))
	require.NoError(t, err)
	assert.Equal(t, "Bearer token-1", header)
	assert.Len(t, tokens.received(), 1)

	// different credentials get their own token
	other := entry(tokens.URL, nil)
	other.Properties[PropertyClientID] = "client-2"
	header, err = authorization.AuthorizationHeader(context.Background(), other)
	require.NoError(t, err)
	assert.Equal(t, "Bearer token-2", header)
	assert.Equal(t, "client-2", tokens.received()[1].Get("client_id"))
}

func Test_AuthorizationHeader_RefreshesExpiredTokens(t *testing.T) {
	tokens := newTokenServer(t, 1) // expires within the token source's expiry delta, so it is never reused
	authorization := newAuthorization(t, newKey(t))
	for range 2 {
		_, err := authorization.AuthorizationHeader(context.Background(), entry(tokens.URL, nil))
		require.NoError(t, err)
	}
	assert.Len(t, tokens.received(), 2)
}

func Test_AuthorizationHeader_Errors(t *testing.T) {
	authorization := newAuthorization(t, newKey(t))

	incomplete := entry("https://idp.example.com/token", nil)
	delete(incomplete.Properties, PropertyClientSecret)
	_, err := authorization.AuthorizationHeader(context.Background(), incomplete)
	assert.ErrorIs(t, err, dsdk.ErrInvalidInput)
	assert.ErrorContains(t, err, PropertyClientSecret)

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error": "invalid_client"}`))
	}))
	defer failing.Close()
	_, err = authorization.AuthorizationHeader(context.Background(), entry(failing.URL, nil))
	assert.ErrorContains(t, err, "invalid_client")
}

// Test_WithSDK checks the authorization end to end through the SDK's handler.
func Test_WithSDK(t *testing.T) {
	key := newKey(t)
	authorization := newAuthorization(t, key)
	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(&nopStore{}),
		dsdk.WithTransactionContext(nopTrx{}),
		dsdk.WithAuthorization(authorization),
	)
	require.NoError(t, err)
	handler := dsdk.NewHandler(sdk)

	request := func(header string) int {
		req := httptest.NewRequest(http.MethodGet, "/v1/dataflows/flow/status", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		return rr.Code
	}
	valid := sign(t, key, jwt.SigningMethodRS256, jwt.MapClaims{"sub": "cp-1", "exp": time.Now().Add(time.Hour).Unix()})
	assert.Equal(t, http.StatusNotFound, request("Bearer "+valid), "authenticated; the flow does not exist")
	assert.Equal(t, http.StatusUnauthorized, request(""))
	assert.Equal(t, http.StatusUnauthorized, request("Bearer "+sign(t, newKey(t), jwt.SigningMethodRS256, jwt.MapClaims{"sub": "cp-1", "exp": time.Now().Add(time.Hour).Unix()})))
}

func unsignedToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	token.Header["kid"] = keyID
	signed, err := token.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)
	return signed
}

func signWithKID(t *testing.T, key *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString(key)
	require.NoError(t, err)
	return signed
}

type nopStore struct{}

func (nopStore) FindById(context.Context, string) (*dsdk.DataFlow, error) {
	return nil, dsdk.ErrNotFound
}
func (nopStore) Create(context.Context, *dsdk.DataFlow) error { return nil }
func (nopStore) Save(context.Context, *dsdk.DataFlow) error   { return nil }
func (nopStore) Delete(context.Context, string) error         { return nil }

type nopTrx struct{}

func (nopTrx) Execute(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }
