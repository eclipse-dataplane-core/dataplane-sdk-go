package dsdk

import (
	"context"
	"fmt"
	"strings"
)

const (
	testAuthorizationType = "test"
	// testCaller is the control plane id that serve() authenticates as.
	testCaller = "test-cp"
	// testPrincipal is the identity the data plane presents to control planes in the tests.
	testPrincipal = "test-dp"
)

// testAuthorization is a trivial Authorization: the header "Test <id>" authenticates the caller <id>, and outbound
// requests carry "Test <principal>" with the principal from the authorization entry.
type testAuthorization struct{}

func (testAuthorization) Type() string { return testAuthorizationType }

func (testAuthorization) AuthorizationHeader(_ context.Context, entry AuthorizationEntry) (string, error) {
	principal, ok := entry.Property("principal")
	if !ok {
		return "", fmt.Errorf("%w: no principal", ErrUnauthorized)
	}
	return "Test " + principal, nil
}

func (testAuthorization) ExtractCallerID(_ context.Context, header string) (string, error) {
	id, ok := strings.CutPrefix(header, "Test ")
	if !ok || id == "" {
		return "", fmt.Errorf("%w: not a test header", ErrUnauthorized)
	}
	return id, nil
}

func testEntry() AuthorizationEntry {
	return AuthorizationEntry{Type: testAuthorizationType, Properties: map[string]any{"principal": testPrincipal}}
}
