package oauth2

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/golang-jwt/jwt/v5"
	xoauth2 "golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// Type is the authorization profile type.
const Type = "oauth2_client_credentials"

const bearerPrefix = "Bearer "

// Authorization entry properties used for outbound requests.
const (
	PropertyTokenEndpoint = "tokenEndpoint"
	PropertyClientID      = "clientId"
	PropertyClientSecret  = "clientSecret"
	PropertyScope         = "scope"
)

// defaultAlgorithms are the accepted JWT signature algorithms; "none" and HMAC are never accepted.
var defaultAlgorithms = []string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA"}

// ClientCredentialsAuthorization is the dsdk.Authorization for the "oauth2_client_credentials" profile.
type ClientCredentialsAuthorization struct {
	keyfunc    jwt.Keyfunc
	parserOpts []jwt.ParserOption
	httpClient *http.Client

	mu      sync.Mutex
	sources map[tokenKey]xoauth2.TokenSource
}

type tokenKey struct {
	endpoint, clientID, clientSecret, scope string
}

type config struct {
	httpClient *http.Client
	keyfunc    jwt.Keyfunc
	issuer     string
	audience   string
	leeway     time.Duration
	algorithms []string
}

// Option configures a ClientCredentialsAuthorization.
type Option func(*config)

// WithHTTPClient sets the client used for the token and JWKS endpoints.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) { c.httpClient = client }
}

// WithIssuer requires inbound tokens to carry this "iss" claim.
func WithIssuer(issuer string) Option {
	return func(c *config) { c.issuer = issuer }
}

// WithAudience requires inbound tokens to carry this "aud" claim.
func WithAudience(audience string) Option {
	return func(c *config) { c.audience = audience }
}

// WithLeeway allows for clock skew when validating exp, nbf and iat.
func WithLeeway(leeway time.Duration) Option {
	return func(c *config) { c.leeway = leeway }
}

// WithAlgorithms restricts the accepted JWT signature algorithms (default: RSA, RSA-PSS, ECDSA and EdDSA).
func WithAlgorithms(algorithms ...string) Option {
	return func(c *config) { c.algorithms = algorithms }
}

// WithKeyfunc verifies inbound tokens with the given key function instead of a JWKS endpoint, e.g. for a static key
// set (keyfunc.NewJWKSetJSON).
func WithKeyfunc(keyfunc jwt.Keyfunc) Option {
	return func(c *config) { c.keyfunc = keyfunc }
}

// NewClientCredentialsAuthorization creates the authorization. Inbound tokens are verified against the keys of the
// JWKS endpoint at jwksURL, which are refreshed in the background until ctx is done. jwksURL may be empty if
// WithKeyfunc is given.
func NewClientCredentialsAuthorization(ctx context.Context, jwksURL string, options ...Option) (*ClientCredentialsAuthorization, error) {
	cfg := config{httpClient: &http.Client{Timeout: 30 * time.Second}, algorithms: defaultAlgorithms}
	for _, opt := range options {
		opt(&cfg)
	}
	if cfg.keyfunc == nil {
		if jwksURL == "" {
			return nil, errors.New("a JWKS URL is required to verify inbound tokens")
		}
		jwks, err := keyfunc.NewDefaultOverrideCtx(ctx, []string{jwksURL}, keyfunc.Override{Client: cfg.httpClient})
		if err != nil {
			return nil, fmt.Errorf("creating JWKS key function for %s: %w", jwksURL, err)
		}
		cfg.keyfunc = jwks.Keyfunc
	}

	parserOpts := []jwt.ParserOption{
		jwt.WithValidMethods(cfg.algorithms),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(cfg.leeway),
	}
	if cfg.issuer != "" {
		parserOpts = append(parserOpts, jwt.WithIssuer(cfg.issuer))
	}
	if cfg.audience != "" {
		parserOpts = append(parserOpts, jwt.WithAudience(cfg.audience))
	}
	return &ClientCredentialsAuthorization{
		keyfunc:    cfg.keyfunc,
		parserOpts: parserOpts,
		httpClient: cfg.httpClient,
		sources:    map[tokenKey]xoauth2.TokenSource{},
	}, nil
}

func (a *ClientCredentialsAuthorization) Type() string {
	return Type
}

// AuthorizationHeader returns "Bearer <access token>" for the control plane's client credentials. Tokens are reused
// until they expire.
func (a *ClientCredentialsAuthorization) AuthorizationHeader(ctx context.Context, entry dsdk.AuthorizationEntry) (string, error) {
	key := tokenKey{}
	var missing []string
	for name, target := range map[string]*string{PropertyTokenEndpoint: &key.endpoint, PropertyClientID: &key.clientID, PropertyClientSecret: &key.clientSecret} {
		value, ok := entry.Property(name)
		if !ok || value == "" {
			missing = append(missing, name)
		}
		*target = value
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("%w: authorization entry lacks %s", dsdk.ErrInvalidInput, strings.Join(missing, ", "))
	}
	key.scope, _ = entry.Property(PropertyScope)

	token, err := a.tokenSource(key).Token()
	if err != nil {
		return "", fmt.Errorf("obtaining access token from %s: %w", key.endpoint, err)
	}
	return bearerPrefix + token.AccessToken, nil
}

func (a *ClientCredentialsAuthorization) tokenSource(key tokenKey) xoauth2.TokenSource {
	a.mu.Lock()
	defer a.mu.Unlock()
	if source, ok := a.sources[key]; ok {
		return source
	}
	cfg := clientcredentials.Config{
		ClientID:     key.clientID,
		ClientSecret: key.clientSecret,
		TokenURL:     key.endpoint,
		// the client credentials are sent in the form body, like the Java SDK does
		AuthStyle: xoauth2.AuthStyleInParams,
	}
	if key.scope != "" {
		cfg.Scopes = strings.Fields(key.scope)
	}
	// the token source outlives the request, so it gets its own context carrying only the HTTP client
	ctx := context.WithValue(context.Background(), xoauth2.HTTPClient, a.httpClient)
	source := cfg.TokenSource(ctx) // caches the token until it expires
	a.sources[key] = source
	return source
}

// ExtractCallerID verifies the bearer JWT and returns its "sub" claim.
func (a *ClientCredentialsAuthorization) ExtractCallerID(_ context.Context, authorizationHeader string) (string, error) {
	raw, ok := strings.CutPrefix(authorizationHeader, bearerPrefix)
	if !ok || raw == "" {
		return "", fmt.Errorf("%w: expected a bearer token", dsdk.ErrUnauthorized)
	}
	token, err := jwt.Parse(raw, a.keyfunc, a.parserOpts...)
	if err != nil {
		return "", fmt.Errorf("%w: invalid token: %w", dsdk.ErrUnauthorized, err)
	}
	subject, err := token.Claims.GetSubject()
	if err != nil || subject == "" {
		return "", fmt.Errorf("%w: token has no sub claim", dsdk.ErrUnauthorized)
	}
	return subject, nil
}
