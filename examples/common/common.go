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
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
)

const (
	ProviderSignalingPort = 8080
	ProviderDataPort      = 8181
	ConsumerSignalingPort = 9090
	ConsumerDataPort      = 9191

	bearerPrefix = "Bearer "
)

type TokenResponse struct {
	Token    string `json:"token"`
	Endpoint string `json:"url"`
}

// NewSignalingServer creates and returns a new HTTP server serving the data plane signaling endpoints under /v1.
func NewSignalingServer(sdk *dsdk.DataPlaneSDK, port int) *http.Server {
	return &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: dsdk.NewHandler(sdk)}
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
