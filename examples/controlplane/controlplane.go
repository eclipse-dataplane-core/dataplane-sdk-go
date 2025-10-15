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
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
)

const (
	startUrl            = "http://localhost:%d/dataflows/start"
	terminateUrl        = "http://localhost:%d/dataflows/%s/terminate"
	consumerPrepareURL  = "http://localhost:%d/dataflows/prepare"
	providerCallbackURL = "http://provider.com/dp/callback"
	contentType         = "Content-Type"
	jsonContentType     = "application/json"
)

// ControlPlaneSimulator simulates control plane interactions between a consumer and provider and drives their respective data planes.
type ControlPlaneSimulator struct {
	consumerDataPlane string
	providerDataPlane string
}

func NewSimulator() (*ControlPlaneSimulator, error) {
	return &ControlPlaneSimulator{}, nil
}

func (c *ControlPlaneSimulator) ProviderStart(ctx context.Context,
	processID string,
	agreementID string,
	datasetID string,
	da *dsdk.DataAddress) (*dsdk.DataAddress, error) {
	callbackURL, _ := url.Parse(providerCallbackURL)

	startMessage := dsdk.DataFlowStartMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageID:        uuid.NewString(),
			AgreementID:      agreementID,
			DatasetID:        datasetID,
			ProcessID:        processID,
			DataspaceContext: "dscontext",
			CounterPartyID:   "did:web:consumer.com",
			ParticipantID:    "did:web:provider.com",
			CallbackAddress:  dsdk.CallbackURL(*callbackURL),
			TransferType:     dsdk.TransferType{DestinationType: "custom", FlowType: dsdk.Pull},
			DataAddress:      da,
		},
	}

	serialized, err := json.Marshal(startMessage)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal start message: %w", err)
	}

	// Create the request
	providerSignalingUrl := fmt.Sprintf(startUrl, common.ProviderSignalingPort)
	req, err := http.NewRequestWithContext(ctx, "POST", providerSignalingUrl, bytes.NewBuffer(serialized))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set(contentType, jsonContentType)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("start request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var message dsdk.DataFlowResponseMessage
	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return message.DataAddress, nil
}

func (c *ControlPlaneSimulator) ConsumerStart(ctx context.Context, processID string, source *dsdk.DataAddress) error {
	callbackURL, _ := url.Parse(providerCallbackURL)
	startMessage := dsdk.DataFlowStartMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageID:        uuid.NewString(),
			ProcessID:        processID,
			AgreementID:      uuid.NewString(),
			DataspaceContext: "dscontext",
			ParticipantID:    "did:web:consumer.com",
			CounterPartyID:   "did:web:provider.com",
			CallbackAddress:  dsdk.CallbackURL(*callbackURL),
			TransferType:     dsdk.TransferType{DestinationType: "custom", FlowType: dsdk.Pull},
			DataAddress:      source,
		},
	}

	serialized, err := json.Marshal(startMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal start message: %w", err)
	}

	// Create the request
	consumerSignalingUrl := fmt.Sprintf(startUrl, common.ConsumerSignalingPort)
	req, err := http.NewRequestWithContext(ctx, "POST", consumerSignalingUrl, bytes.NewBuffer(serialized))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set(contentType, jsonContentType)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("start request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var message dsdk.DataFlowResponseMessage
	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}

func (c *ControlPlaneSimulator) ConsumerPrepare(ctx context.Context, processID string, agreementID string, datasetID string) (*dsdk.DataAddress, error) {
	callbackURL, _ := url.Parse(providerCallbackURL)
	prepareMessage := dsdk.DataFlowPrepareMessage{
		DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
			MessageID:        uuid.NewString(),
			AgreementID:      agreementID,
			DatasetID:        datasetID,
			ProcessID:        processID,
			DataspaceContext: "dscontext",
			ParticipantID:    "did:web:consumer.com",
			CounterPartyID:   "did:web:provider.com",
			CallbackAddress:  dsdk.CallbackURL(*callbackURL),
			TransferType:     dsdk.TransferType{DestinationType: "custom", FlowType: dsdk.Pull},
		},
	}

	serialized, err := json.Marshal(prepareMessage)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal prepare message: %w", err)
	}

	// Create the request
	consumerSignalingUrl := fmt.Sprintf(consumerPrepareURL, common.ConsumerSignalingPort)
	req, err := http.NewRequestWithContext(ctx, "POST", consumerSignalingUrl, bytes.NewBuffer(serialized))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set(contentType, jsonContentType)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prepare request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var message dsdk.DataFlowResponseMessage
	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return message.DataAddress, nil
}

func (c *ControlPlaneSimulator) ProviderTerminate(ctx context.Context, processID string, agreementID string, datasetID string) error {
	terminateMessage := dsdk.DataFlowTransitionMessage{Reason: "violation"}

	serialized, err := json.Marshal(terminateMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal terminate message: %w", err)
	}

	// Create the request
	providerSignalingUrl := fmt.Sprintf(terminateUrl, common.ProviderSignalingPort, processID)
	req, err := http.NewRequestWithContext(ctx, "POST", providerSignalingUrl, bytes.NewBuffer(serialized))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set(contentType, jsonContentType)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("start request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
