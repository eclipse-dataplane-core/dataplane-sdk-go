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

// Package controlplane is a minimal control plane for the samples. It plays both the consumer and the provider
// control plane: it sends signaling messages to the data planes, and it serves the control plane endpoints of the
// Data Plane Signaling API that the data planes call (registration and data flow callbacks). All requests are
// authorized with the sample-only demo authorization of examples/common.
package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/google/uuid"
)

const (
	contentType     = "Content-Type"
	jsonContentType = "application/json"
	consumerID      = "did:web:consumer.com"
	providerID      = "did:web:provider.com"
)

// Callback is a data flow callback received from a data plane.
type Callback struct {
	Action  string
	Message dsdk.DataFlowStatusMessage
}

// ControlPlaneSimulator simulates the consumer and provider control planes and drives their data planes.
type ControlPlaneSimulator struct {
	client *http.Client
	server *http.Server

	mu         sync.Mutex
	dataPlanes map[string]dsdk.DataPlaneRegistrationMessage
	callbacks  []Callback
	received   chan struct{}
}

// NewSimulator starts the control plane's server on common.ControlPlanePort.
func NewSimulator() (*ControlPlaneSimulator, error) {
	c := &ControlPlaneSimulator{
		client:     &http.Client{Timeout: 30 * time.Second},
		dataPlanes: map[string]dsdk.DataPlaneRegistrationMessage{},
		received:   make(chan struct{}, 1),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /dataplanes", c.registerDataPlane)
	mux.HandleFunc("DELETE /dataplanes/{id}", c.deregisterDataPlane)
	mux.HandleFunc("POST /transfers/{id}/dataflow/{action}", c.callback)
	c.server = &http.Server{Addr: fmt.Sprintf(":%d", common.ControlPlanePort), Handler: mux}
	common.StartServer(c.server, "[Control Plane] Server")
	return c, nil
}

// URL returns the base URL of the control plane's endpoints.
func (c *ControlPlaneSimulator) URL() string {
	return fmt.Sprintf("http://localhost:%d", common.ControlPlanePort)
}

func (c *ControlPlaneSimulator) Shutdown(ctx context.Context) {
	_ = c.server.Shutdown(ctx)
}

// Connect registers the data plane with this control plane (the data plane calls PUT /dataplanes) and this control
// plane with the data plane (PUT /v1/controlplanes on the data plane).
func (c *ControlPlaneSimulator) Connect(ctx context.Context, sdk *dsdk.DataPlaneSDK, dataPlaneID string, signalingPort int, profiles ...string) error {
	if err := common.RegisterDataPlane(ctx, sdk, dataPlaneID, signalingPort, profiles, c.URL()); err != nil {
		return fmt.Errorf("registering data plane %s: %w", dataPlaneID, err)
	}
	registration := dsdk.ControlPlaneRegistrationMessage{
		ControlPlaneID: common.SampleControlPlaneID,
		Endpoint:       c.URL(),
		// the data plane authenticates its callbacks to this control plane as itself
		Authorization: common.DemoEntry(dataPlaneID),
	}
	resp, err := c.request(ctx, http.MethodPut, signalingURL(signalingPort, "/controlplanes"), registration)
	if err != nil {
		return fmt.Errorf("registering with data plane %s: %w", dataPlaneID, err)
	}
	_ = resp.Body.Close()
	log.Printf("[Control Plane] Registered with data plane %s", dataPlaneID)
	return nil
}

// ConsumerPrepare sends a prepare message to the consumer data plane. For push transfers the response carries the
// consumer's DataAddress.
func (c *ControlPlaneSimulator) ConsumerPrepare(ctx context.Context, dataFlowID string, agreementID string, datasetID string, profile string) (*dsdk.DataFlowStatusMessage, error) {
	message := dsdk.DataFlowPrepareMessage{
		DataFlowBaseMessage: newBaseMessage(dataFlowID, agreementID, datasetID, profile, consumerID, providerID),
	}
	return c.send(ctx, signalingURL(common.ConsumerSignalingPort, "/dataflows/prepare"), message)
}

// ProviderStart sends a start message to the provider data plane. For push transfers da is the consumer's
// DataAddress; for pull transfers it must be nil and a synchronous response carries the provider's DataAddress.
func (c *ControlPlaneSimulator) ProviderStart(ctx context.Context,
	dataFlowID string,
	agreementID string,
	datasetID string,
	profile string,
	da *dsdk.DataAddress) (*dsdk.DataFlowStatusMessage, error) {

	message := dsdk.DataFlowStartMessage{
		DataFlowBaseMessage: newBaseMessage(dataFlowID, agreementID, datasetID, profile, providerID, consumerID),
		DataAddress:         da,
	}
	return c.send(ctx, signalingURL(common.ProviderSignalingPort, "/dataflows/start"), message)
}

// ConsumerStarted notifies the consumer data plane that the provider started the transfer. For pull transfers source
// is the provider's DataAddress; for push transfers it must be nil.
func (c *ControlPlaneSimulator) ConsumerStarted(ctx context.Context, dataFlowID string, source *dsdk.DataAddress) error {
	message := dsdk.DataFlowStartedNotificationMessage{MessageID: uuid.NewString(), DataAddress: source}
	_, err := c.send(ctx, signalingURL(common.ConsumerSignalingPort, "/dataflows/"+dataFlowID+"/started"), message)
	return err
}

// ProviderCompleted tells the provider data plane that the consumer finished the transfer.
func (c *ControlPlaneSimulator) ProviderCompleted(ctx context.Context, dataFlowID string) error {
	resp, err := c.request(ctx, http.MethodPost, signalingURL(common.ProviderSignalingPort, "/dataflows/"+dataFlowID+"/completed"), nil)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// ProviderTerminate terminates the transfer on the provider data plane.
func (c *ControlPlaneSimulator) ProviderTerminate(ctx context.Context, dataFlowID string) error {
	message := dsdk.DataFlowTerminateMessage{MessageID: uuid.NewString(), Reason: "violation"}
	resp, err := c.request(ctx, http.MethodPost, signalingURL(common.ProviderSignalingPort, "/dataflows/"+dataFlowID+"/terminate"), message)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// WaitForCallback waits until a data plane sent the callback with the given action for the data flow.
func (c *ControlPlaneSimulator) WaitForCallback(dataFlowID string, action string, timeout time.Duration) (*dsdk.DataFlowStatusMessage, error) {
	deadline := time.After(timeout)
	for {
		c.mu.Lock()
		for _, callback := range c.callbacks {
			if callback.Action == action && callback.Message.DataFlowID == dataFlowID {
				c.mu.Unlock()
				return &callback.Message, nil
			}
		}
		c.mu.Unlock()
		select {
		case <-c.received:
		case <-deadline:
			return nil, fmt.Errorf("no %s callback for data flow %s within %s", action, dataFlowID, timeout)
		}
	}
}

// --- control plane endpoints ---

// registerDataPlane handles PUT /dataplanes. The data plane must authenticate as the data plane it registers.
func (c *ControlPlaneSimulator) registerDataPlane(w http.ResponseWriter, r *http.Request) {
	var registration dsdk.DataPlaneRegistrationMessage
	if err := json.NewDecoder(r.Body).Decode(&registration); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.Header.Get("Authorization") != common.DemoHeader(registration.DataPlaneID) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	c.mu.Lock()
	c.dataPlanes[registration.DataPlaneID] = registration
	c.mu.Unlock()
	log.Printf("[Control Plane] Registered data plane %s at %s for %v", registration.DataPlaneID, registration.Endpoint, registration.Profiles)
}

// deregisterDataPlane handles DELETE /dataplanes/{id}.
func (c *ControlPlaneSimulator) deregisterDataPlane(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if r.Header.Get("Authorization") != common.DemoHeader(id) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	c.mu.Lock()
	delete(c.dataPlanes, id)
	c.mu.Unlock()
	log.Printf("[Control Plane] Deregistered data plane %s", id)
	w.WriteHeader(http.StatusNoContent)
}

// callback handles POST /transfers/{id}/dataflow/{action} from a registered data plane.
func (c *ControlPlaneSimulator) callback(w http.ResponseWriter, r *http.Request) {
	caller, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Demo ")
	c.mu.Lock()
	_, registered := c.dataPlanes[caller]
	c.mu.Unlock()
	if !ok || !registered {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var message dsdk.DataFlowStatusMessage
	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if message.DataFlowID != r.PathValue("id") {
		http.Error(w, "data flow id mismatch", http.StatusBadRequest)
		return
	}
	action := r.PathValue("action")
	log.Printf("[Control Plane] Received %s callback from %s for data flow %s (%s)", action, caller, message.DataFlowID, message.State)
	c.mu.Lock()
	c.callbacks = append(c.callbacks, Callback{Action: action, Message: message})
	c.mu.Unlock()
	select {
	case c.received <- struct{}{}:
	default:
	}
}

// --- helpers ---

func signalingURL(port int, path string) string {
	return fmt.Sprintf("http://localhost:%d%s%s", port, dsdk.DefaultBasePath, path)
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

// send posts the message and decodes the DataFlowStatusMessage of a 200 or 202 response.
func (c *ControlPlaneSimulator) send(ctx context.Context, url string, message any) (*dsdk.DataFlowStatusMessage, error) {
	resp, err := c.request(ctx, http.MethodPost, url, message)
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

// request sends the message, authenticated as the sample control plane, and returns a 2xx response.
func (c *ControlPlaneSimulator) request(ctx context.Context, method string, url string, message any) (*http.Response, error) {
	var body io.Reader = http.NoBody
	if message != nil {
		serialized, err := json.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal message: %w", err)
		}
		body = bytes.NewReader(serialized)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set(contentType, jsonContentType)
	req.Header.Set("Authorization", common.DemoHeader(common.SampleControlPlaneID))

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%s %s: unexpected status code %d: %s", method, url, resp.StatusCode, body)
	}
	return resp, nil
}
