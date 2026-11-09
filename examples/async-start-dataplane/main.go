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

// Demonstrates registration and an asynchronous start on a provider data plane. The data plane registers itself with
// the control plane (PUT /dataplanes) and the control plane registers itself with the data plane
// (PUT /v1/controlplanes). The start processor then answers STARTING (HTTP 202), and once the transfer is ready the
// data plane calls NotifyStarted, which sends the started callback with the pull DataAddress to the registered
// control plane.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/memory"
	"github.com/google/uuid"
)

const (
	pullProfile    = "https://w3id.org/dspace-sig/profile/http-pull"
	dataPlaneID    = "async-start-dataplane"
	controlPlaneID = "sample-control-plane"
)

func main() {
	// A minimal control plane that accepts data plane registrations and receives the data plane's callbacks.
	callbacks := make(chan dsdk.DataFlowStatusMessage, 1)
	controlPlaneMux := http.NewServeMux()
	controlPlaneMux.HandleFunc("PUT /dataplanes", func(w http.ResponseWriter, r *http.Request) {
		var registration dsdk.DataPlaneRegistrationMessage
		if err := json.NewDecoder(r.Body).Decode(&registration); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("[Control Plane] Registered data plane %s at %s for %v", registration.DataPlaneID, registration.Endpoint, registration.Profiles)
	})
	controlPlaneMux.HandleFunc("DELETE /dataplanes/{id}", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[Control Plane] Deregistered data plane %s", r.PathValue("id"))
		w.WriteHeader(http.StatusNoContent)
	})
	controlPlaneMux.HandleFunc("POST /transfers/{id}/dataflow/started", func(w http.ResponseWriter, r *http.Request) {
		var message dsdk.DataFlowStatusMessage
		if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		callbacks <- message
	})
	controlPlaneURL := serve(controlPlaneMux)

	var sdk *dsdk.DataPlaneSDK
	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithControlPlaneStore(memory.NewInMemoryControlPlaneStore()),
		dsdk.WithStartProcessor(func(_ context.Context, flow *dsdk.DataFlow, _ *dsdk.DataPlaneSDK, _ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
			// Provisioning takes a while: answer STARTING now and notify the control plane when done.
			go func() {
				time.Sleep(100 * time.Millisecond)
				if _, err := sdk.NotifyStarted(context.Background(), flow.ID, provideAddress); err != nil {
					log.Printf("[Provider Data Plane] NotifyStarted failed: %v", err)
				}
			}()
			return &dsdk.DataFlowStatusMessage{State: dsdk.Starting}, nil
		}),
	)
	if err != nil {
		log.Fatalf("Unable to create SDK: %v", err)
	}
	dataPlaneURL := serve(dsdk.NewHandler(sdk))
	ctx := context.Background()

	// The data plane registers itself with the control plane.
	authorization := dsdk.AuthorizationEntry{Type: "none"}
	err = sdk.RegisterDataPlane(ctx, controlPlaneURL, dsdk.DataPlaneRegistrationMessage{
		DataPlaneID:   dataPlaneID,
		Endpoint:      dataPlaneURL + dsdk.DefaultBasePath,
		Profiles:      []string{pullProfile},
		Authorization: authorization,
	})
	if err != nil {
		log.Fatalf("Unable to register the data plane: %v", err)
	}
	defer func() {
		if err := sdk.DeregisterDataPlane(ctx, controlPlaneURL, dataPlaneID); err != nil {
			log.Printf("Unable to deregister the data plane: %v", err)
		}
	}()

	// The control plane registers itself with the data plane. As it is the only registered control plane, the data
	// plane records it on new data flows and sends their callbacks to it.
	put(dataPlaneURL+dsdk.DefaultBasePath+"/controlplanes", dsdk.ControlPlaneRegistrationMessage{
		ControlPlaneID: controlPlaneID,
		Endpoint:       controlPlaneURL,
		Authorization:  authorization,
	})

	// The control plane starts the transfer.
	dataFlowID := uuid.NewString()
	start := dsdk.DataFlowStartMessage{DataFlowBaseMessage: dsdk.DataFlowBaseMessage{
		MessageID:        uuid.NewString(),
		ParticipantID:    "did:web:provider.com",
		CounterPartyID:   "did:web:consumer.com",
		DataspaceContext: "dscontext",
		DataFlowID:       dataFlowID,
		AgreementID:      uuid.NewString(),
		DatasetID:        uuid.NewString(),
		Profile:          pullProfile,
		Claims:           map[string]any{},
	}}
	body, _ := json.Marshal(start)
	resp, err := http.Post(dataPlaneURL+dsdk.DefaultBasePath+"/dataflows/start", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("Start request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		log.Fatalf("Expected 202 Accepted, got %d", resp.StatusCode)
	}
	log.Printf("[Control Plane] Start accepted, status at %s", resp.Header.Get("Location"))

	select {
	case message := <-callbacks:
		if message.State != dsdk.Started || message.DataAddress == nil {
			log.Fatalf("Unexpected started callback: %+v", message)
		}
		log.Printf("[Control Plane] Received started callback for %s, data at %s", message.DataFlowID, message.DataAddress.Endpoint)
	case <-time.After(10 * time.Second):
		log.Fatal("No started callback received")
	}

	// The SDK persists STARTED once the control plane has acknowledged the callback, so poll briefly.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		flow, err := sdk.Status(context.Background(), dataFlowID)
		if err == nil && flow.State == dsdk.Started {
			log.Printf("[Provider Data Plane] Data flow %s is %s", dataFlowID, flow.State)
			return
		}
		if time.Now().After(deadline) {
			log.Fatalf("Expected the flow to be STARTED: %v %v", flow, err)
		}
	}
}

// provideAddress returns the DataAddress the consumer pulls the data from.
func provideAddress(_ context.Context, flow *dsdk.DataFlow, _ *dsdk.DataPlaneSDK, _ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
	address, err := dsdk.NewDataAddressBuilder().
		EndpointType("https://w3id.org/idsa/v4.1/HTTP").
		Endpoint("http://localhost:8181/datasets/"+flow.DatasetID).
		Property("authorization", uuid.NewString()).
		Property("authType", "bearer").
		Build()
	if err != nil {
		return nil, err
	}
	return &dsdk.DataFlowStatusMessage{State: dsdk.Started, DataAddress: address}, nil
}

// put sends the message with PUT and fails unless the response is 200.
func put(url string, message any) {
	body, _ := json.Marshal(message)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		log.Fatalf("Unable to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("PUT %s failed: %v", url, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("PUT %s: expected 200, got %d", url, resp.StatusCode)
	}
	log.Printf("[Control Plane] Registered with the data plane at %s", url)
}

// serve starts an HTTP server on a free local port and returns its base URL.
func serve(handler http.Handler) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("Unable to listen: %v", err)
	}
	go func() {
		_ = http.Serve(listener, handler)
	}()
	return fmt.Sprintf("http://%s", listener.Addr())
}
