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
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
)

const (
	ControlPlanePort      = 7070
	ProviderSignalingPort = 8080
	ProviderDataPort      = 8181
	ConsumerSignalingPort = 9090
	ConsumerDataPort      = 9191

	// ProviderDataPlaneID and ConsumerDataPlaneID identify the sample data planes towards the control plane.
	ProviderDataPlaneID = "provider-dataplane"
	ConsumerDataPlaneID = "consumer-dataplane"

	bearerPrefix = "Bearer "
)

type TokenResponse struct {
	Token    string `json:"token"`
	Endpoint string `json:"url"`
}

// StartSignalingServer starts an HTTP server on the port that serves the data plane signaling endpoints of the SDK
// (dsdk.NewHandler, under /v1) and returns it for shutdown.
func StartSignalingServer(sdk *dsdk.DataPlaneSDK, port int, name string) *http.Server {
	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: dsdk.NewHandler(sdk)}
	StartServer(server, name)
	return server
}

// RegisterDataPlane registers the data plane listening on signalingPort with the sample control plane at
// controlPlaneURL. The data plane authenticates as dataPlaneID and asks the control plane to authenticate as
// SampleControlPlaneID.
func RegisterDataPlane(ctx context.Context, sdk *dsdk.DataPlaneSDK, dataPlaneID string, signalingPort int, profiles []string, controlPlaneURL string) error {
	controlPlane := dsdk.ControlPlane{ID: SampleControlPlaneID, Endpoint: controlPlaneURL, Authorization: DemoEntry(dataPlaneID)}
	return sdk.RegisterDataPlane(ctx, controlPlane, dsdk.DataPlaneRegistrationMessage{
		DataPlaneID:   dataPlaneID,
		Endpoint:      fmt.Sprintf("http://localhost:%d%s", signalingPort, dsdk.DefaultBasePath),
		Profiles:      profiles,
		Authorization: DemoEntry(SampleControlPlaneID),
	})
}

// StartServer binds the server's address before it returns, so that requests sent right afterwards reach it, and then
// serves in the background. It exits the program if the address cannot be bound or serving fails.
func StartServer(server *http.Server, name string) {
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		log.Fatalf("%s failed to start: %v", name, err)
	}
	log.Printf("%s listening on %s\n", name, server.Addr)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("%s failed: %v", name, err)
		}
	}()
}

// NewDataServer creates and initializes a new HTTP server with a specified port and request handler.
func NewDataServer(port int, path string, handler func(http.ResponseWriter, *http.Request)) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc(path, handler)
	return &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
}

// ParseDataset extracts the dataset ID from the URL path in the incoming HTTP request.
// Returns the dataset ID as a string and an error if the URL path is invalid or the dataset ID is missing.
func ParseDataset(w http.ResponseWriter, r *http.Request) (string, error) {
	urlPath := strings.TrimSuffix(r.URL.Path, "/")
	pathParts := strings.Split(urlPath, "/")
	if len(pathParts) == 0 {
		return "", errors.New("invalid URL path")
	}

	datasetID := pathParts[len(pathParts)-1]
	if datasetID == "" {
		http.Error(w, "Dataset ID not found in URL path", http.StatusBadRequest)
	}
	return datasetID, nil
}

// ParseToken extracts and returns the Bearer token from the Authorization header in the HTTP request.
// Returns an error if the header is missing, invalid, or the token is empty.
func ParseToken(_ http.ResponseWriter, r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", errors.New("authorization header is empty")
	}

	if !strings.HasPrefix(authHeader, bearerPrefix) {
		return "", errors.New("invalid authorization header")
	}

	token := strings.TrimPrefix(authHeader, bearerPrefix)
	if token == "" {
		return "", errors.New("empty bearer token")
	}
	return token, nil
}
