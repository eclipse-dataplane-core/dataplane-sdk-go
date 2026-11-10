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

// Demonstrates an asynchronous start on a provider data plane. After registration in both directions, the start
// processor answers STARTING (HTTP 202 with a Location header), and once the transfer is ready the data plane calls
// NotifyStarted, which sends the started callback with the pull DataAddress to the registered control plane.
package main

import (
	"context"
	"log"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/controlplane"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/memory"
	"github.com/google/uuid"
)

const pullProfile = "https://w3id.org/dspace-sig/profile/http-pull"

func main() {
	ctx := context.Background()

	cp, err := controlplane.NewSimulator()
	if err != nil {
		log.Fatalf("Unable to launch control plane simulator: %v", err)
	}
	defer cp.Shutdown(ctx)

	var sdk *dsdk.DataPlaneSDK
	sdk, err = dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithControlPlaneStore(memory.NewInMemoryControlPlaneStore()),
		dsdk.WithAuthorization(common.DemoAuthorization{}),
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
	server := common.StartSignalingServer(sdk, common.ProviderSignalingPort, "[Provider Data Plane] Signaling server")
	defer func() { _ = server.Shutdown(ctx) }()

	if err := cp.Connect(ctx, sdk, common.ProviderDataPlaneID, common.ProviderSignalingPort, pullProfile); err != nil {
		log.Fatalf("Unable to connect the data plane: %v", err)
	}
	defer func() {
		controlPlane := dsdk.ControlPlane{ID: common.SampleControlPlaneID, Endpoint: cp.URL(), Authorization: common.DemoEntry(common.ProviderDataPlaneID)}
		if err := sdk.DeregisterDataPlane(ctx, controlPlane, common.ProviderDataPlaneID); err != nil {
			log.Printf("Unable to deregister the data plane: %v", err)
		}
	}()

	// The control plane starts the transfer; the data plane accepts it asynchronously.
	dataFlowID := uuid.NewString()
	status, err := cp.ProviderStart(ctx, dataFlowID, uuid.NewString(), uuid.NewString(), pullProfile, nil)
	if err != nil {
		log.Fatalf("Start request failed: %v", err)
	}
	if status.State != dsdk.Starting {
		log.Fatalf("Expected STARTING, got %s", status.State)
	}
	log.Printf("[Control Plane] Start accepted, data flow %s is %s", dataFlowID, status.State)

	message, err := cp.WaitForCallback(dataFlowID, dsdk.ActionStarted, 10*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	if message.State != dsdk.Started || message.DataAddress == nil {
		log.Fatalf("Unexpected started callback: %+v", message)
	}
	log.Printf("[Control Plane] Data at %s", message.DataAddress.Endpoint)

	// The SDK persists STARTED once the control plane has acknowledged the callback, so poll briefly.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		flow, err := sdk.Status(ctx, dataFlowID)
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
