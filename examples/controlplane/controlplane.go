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

package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/google/uuid"
)

const (
	signalingUrl    = "http://localhost:%d" + dsdk.DefaultBasePath + "/dataflows"
	contentType     = "Content-Type"
	jsonContentType = "application/json"
	consumerID      = "did:web:consumer.com"
	providerID      = "did:web:provider.com"
)

// ControlPlaneSimulator simulates control plane interactions between a consumer and provider and drives their respective data planes.
type ControlPlaneSimulator struct {
	client *http.Client
}

func NewSimulator() (*ControlPlaneSimulator, error) {
	return &ControlPlaneSimulator{client: &http.Client{Timeout: 30 * time.Second}}, nil
}

// ProviderStart sends a start message to the provider data plane. For push transfers da is the consumer's DataAddress;
// for pull transfers it must be nil and the provider's DataAddress is returned.
func (c *ControlPlaneSimulator) ProviderStart(ctx context.Context,
	dataFlowID string,
	agreementID string,
	datasetID string,
	profile string,
	da *dsdk.DataAddress) (*dsdk.DataAddress, error) {

	startMessage := dsdk.DataFlowStartMessage{
		DataFlowBaseMessage: newBaseMessage(dataFlowID, agreementID, datasetID, profile, providerID, consumerID),
		DataAddress:         da,
	}
	status, err := c.send(ctx, fmt.Sprintf(signalingUrl+"/start", common.ProviderSignalingPort), startMessage)
	if err != nil {
		return nil, fmt.Errorf("start request failed: %w", err)
	}
	return status.DataAddress, nil
}

// ConsumerPrepare sends a prepare message to the consumer data plane. For push transfers the consumer's DataAddress
// is returned.
func (c *ControlPlaneSimulator) ConsumerPrepare(ctx context.Context, dataFlowID string, agreementID string, datasetID string, profile string) (*dsdk.DataAddress, error) {
	prepareMessage := dsdk.DataFlowPrepareMessage{
		DataFlowBaseMessage: newBaseMessage(dataFlowID, agreementID, datasetID, profile, consumerID, providerID),
	}
	status, err := c.send(ctx, fmt.Sprintf(signalingUrl+"/prepare", common.ConsumerSignalingPort), prepareMessage)
	if err != nil {
		return nil, fmt.Errorf("prepare request failed: %w", err)
	}
	return status.DataAddress, nil
}

// ConsumerStarted notifies the consumer data plane that the provider started the transfer. For pull transfers source
// is the provider's DataAddress; for push transfers it must be nil.
func (c *ControlPlaneSimulator) ConsumerStarted(ctx context.Context, dataFlowID string, source *dsdk.DataAddress) error {
	message := dsdk.DataFlowStartedNotificationMessage{
		MessageID:   uuid.NewString(),
		DataAddress: source,
	}
	_, err := c.send(ctx, fmt.Sprintf(signalingUrl+"/%s/started", common.ConsumerSignalingPort, dataFlowID), message)
	if err != nil {
		return fmt.Errorf("started request failed: %w", err)
	}
	return nil
}

func (c *ControlPlaneSimulator) ProviderTerminate(ctx context.Context, dataFlowID string) error {
	terminateMessage := dsdk.DataFlowTerminateMessage{MessageID: uuid.NewString(), Reason: "violation"}
	resp, err := c.post(ctx, fmt.Sprintf(signalingUrl+"/%s/terminate", common.ProviderSignalingPort, dataFlowID), terminateMessage)
	if err != nil {
		return fmt.Errorf("terminate request failed: %w", err)
	}
	_ = resp.Body.Close()
	return nil
}

func newBaseMessage(dataFlowID, agreementID, datasetID, profile, participantID, counterPartyID string) dsdk.DataFlowBaseMessage {
	return dsdk.DataFlowBaseMessage{
		MessageID:        uuid.NewString(),
		ParticipantID:    participantID,
		CounterPartyID:   counterPartyID,
		DataspaceContext: "dscontext",
		DataFlowID:       dataFlowID,
		AgreementID:      agreementID,
		DatasetID:        datasetID,
		Profile:          profile,
		Claims:           map[string]any{"membership": "active"},
	}
}

// send posts the message and decodes the DataFlowStatusMessage of a 200 response.
func (c *ControlPlaneSimulator) send(ctx context.Context, url string, message any) (*dsdk.DataFlowStatusMessage, error) {
	resp, err := c.post(ctx, url, message)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var status dsdk.DataFlowStatusMessage
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	return &status, nil
}

// post sends the message and returns the response if the data plane answered with 200 OK.
func (c *ControlPlaneSimulator) post(ctx context.Context, url string, message any) (*http.Response, error) {
	serialized, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(serialized))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set(contentType, jsonContentType)
	req.Header.Set("Authorization", common.DemoHeader(common.SampleControlPlaneID))

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, body)
	}
	return resp, nil
}
