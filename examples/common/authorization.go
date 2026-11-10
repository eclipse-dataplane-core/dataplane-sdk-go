//  Copyright (c) 2025 Metaform Systems, Inc
//
//  This program and the accompanying materials are made available under the
//  terms of the Apache License, Version 2.0 which is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
//       Metaform Systems, Inc. - initial API and implementation
//

package common

import (
	"context"
	"fmt"
	"strings"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
)

const (
	// DemoAuthorizationType is the authorization type of DemoAuthorization.
	DemoAuthorizationType = "demo"
	// SampleControlPlaneID is the id the sample control plane authenticates with.
	SampleControlPlaneID = "sample-control-plane"

	demoPrefix            = "Demo "
	demoPrincipalProperty = "principal"
)

// DemoAuthorization is a SAMPLE ONLY authorization profile for the examples: the header "Demo <id>" identifies the
// caller <id> without any secret or signature, so anybody can impersonate anybody. Real deployments use a profile
// such as oauth2_client_credentials (pkg/authorization/oauth2).
type DemoAuthorization struct{}

func (DemoAuthorization) Type() string {
	return DemoAuthorizationType
}

// AuthorizationHeader returns "Demo <principal>" with the principal of the control plane's authorization entry.
func (DemoAuthorization) AuthorizationHeader(_ context.Context, entry dsdk.AuthorizationEntry) (string, error) {
	principal, ok := entry.Property(demoPrincipalProperty)
	if !ok || principal == "" {
		return "", fmt.Errorf("%w: demo authorization entry has no principal", dsdk.ErrInvalidInput)
	}
	return DemoHeader(principal), nil
}

// ExtractCallerID returns <id> from "Demo <id>".
func (DemoAuthorization) ExtractCallerID(_ context.Context, header string) (string, error) {
	id, ok := strings.CutPrefix(header, demoPrefix)
	if !ok || id == "" {
		return "", fmt.Errorf("%w: not a demo authorization header", dsdk.ErrUnauthorized)
	}
	return id, nil
}

// DemoEntry returns an authorization entry telling the other side to authenticate as principal.
func DemoEntry(principal string) dsdk.AuthorizationEntry {
	return dsdk.AuthorizationEntry{Type: DemoAuthorizationType, Properties: map[string]any{demoPrincipalProperty: principal}}
}

// DemoHeader returns the Authorization header value that authenticates as id.
func DemoHeader(id string) string {
	return demoPrefix + id
}
