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

package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
)

const (
	accept        = "Accept"
	authorization = "Authorization"
)

// TransferDataset mocks a client that transfers a dataset from the provider endpoint.
func TransferDataset(datasetID string) error {
	if datasetID == "" {
		return fmt.Errorf("dataset ID cannot be empty")
	}

	ctx := context.Background()
	defer ctx.Done()
	consumerEndpoint := fmt.Sprintf("http://localhost:%d/tokens/%s", common.ConsumerDataPort, datasetID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, consumerEndpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create token request: %w", err)
	}

	httpClient := http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResponse common.TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResponse); err != nil {
		return fmt.Errorf("failed to decode token response: %w", err)
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/%s", tokenResponse.Endpoint, datasetID), nil)
	if err != nil {
		return fmt.Errorf("failed to create dataset request: %w", err)
	}
	req.Header.Set(authorization, fmt.Sprintf("Bearer %s", tokenResponse.Token))
	req.Header.Set(accept, jsonContentType)

	resp, err = httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response: %d", resp.StatusCode)

	}
	log.Printf("[Client] Successful dataset request")
	return nil
}
