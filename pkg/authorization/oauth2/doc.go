// Package oauth2 implements the "oauth2_client_credentials" authorization profile of the Data Plane Signaling
// specification as a dsdk.Authorization.
//
// Outbound, the data plane obtains an access token from the token endpoint in the control plane's authorization entry
// (properties tokenEndpoint, clientId, clientSecret and optionally scope) with the client credentials grant and sends
// it as "Authorization: Bearer <token>". Tokens are cached until they expire.
//
// Inbound, the bearer token sent by a control plane must be a JWT signed by a key of the configured JWKS endpoint; its
// "sub" claim is the control plane id.
package oauth2
