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

package main

import (
	"context"
	"log"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/controlplane"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/sync-pull-dataplane/consumer"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/sync-pull-dataplane/launcher"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/google/uuid"
)

const pullProfile = "https://w3id.org/dspace-sig/profile/http-pull"

// Demonstrates a consumer-pull transfer with data planes that answer prepare and start synchronously:
//
//  1. the sample control plane and the data planes register with each other,
//  2. the control plane prepares the consumer, starts the provider and passes the provider's DataAddress to the
//     consumer with /started,
//  3. a client fetches the dataset through the consumer data plane,
//  4. the consumer data plane reports the finished transfer with a completed callback (NotifyCompleted), and the
//     control plane forwards /completed to the provider data plane.
func main() {
	ctx := context.Background()

	cp, err := controlplane.NewSimulator()
	if err != nil {
		log.Fatalf("Unable to launch control plane simulator: %v\n", err)
	}
	defer cp.Shutdown(ctx)

	providerDataPlane, consumerDataPlane := launcher.LaunchServices()
	defer providerDataPlane.Shutdown(ctx)
	defer consumerDataPlane.Shutdown(ctx)

	if err := cp.Connect(ctx, providerDataPlane.SDK(), common.ProviderDataPlaneID, common.ProviderSignalingPort, pullProfile); err != nil {
		log.Fatalf("Unable to connect the provider data plane: %v\n", err)
	}
	if err := cp.Connect(ctx, consumerDataPlane.SDK(), common.ConsumerDataPlaneID, common.ConsumerSignalingPort, pullProfile); err != nil {
		log.Fatalf("Unable to connect the consumer data plane: %v\n", err)
	}

	agreementID := uuid.NewString()
	datasetID := uuid.NewString()

	consumerFlowID := uuid.NewString()
	if _, err := cp.ConsumerPrepare(ctx, consumerFlowID, agreementID, datasetID, pullProfile); err != nil {
		log.Fatalf("Unable to send prepare to the consumer data plane: %v\n", err)
	}

	providerFlowID := uuid.NewString()
	started, err := cp.ProviderStart(ctx, providerFlowID, agreementID, datasetID, pullProfile, nil)
	if err != nil {
		log.Fatalf("Unable to send start to the provider data plane: %v\n", err)
	}

	// pass the provider's DataAddress to the consumer data plane
	if err := cp.ConsumerStarted(ctx, consumerFlowID, started.DataAddress); err != nil {
		log.Fatalf("Unable to send started to the consumer data plane: %v\n", err)
	}

	if err := consumer.TransferDataset(datasetID); err != nil {
		log.Fatalf("Unable to transfer the dataset: %v\n", err)
	}

	// The client is done: the consumer data plane completes the transfer and notifies its control plane, which
	// forwards the completion to the provider data plane.
	if err := consumerDataPlane.SDK().NotifyCompleted(ctx, consumerFlowID); err != nil {
		log.Fatalf("Unable to complete the transfer: %v\n", err)
	}
	if _, err := cp.WaitForCallback(consumerFlowID, dsdk.ActionCompleted, 10*time.Second); err != nil {
		log.Fatalf("%v\n", err)
	}
	if err := cp.ProviderCompleted(ctx, providerFlowID); err != nil {
		log.Fatalf("Unable to send completed to the provider data plane: %v\n", err)
	}
	flow, err := providerDataPlane.SDK().Status(ctx, providerFlowID)
	if err != nil || flow.State != dsdk.Completed {
		log.Fatalf("Expected the provider data flow to be COMPLETED: %v %v\n", flow, err)
	}
	log.Printf("[Control Plane] Transfer completed on both data planes")
}
