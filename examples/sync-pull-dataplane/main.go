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
	"sync"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/controlplane"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/sync-pull-dataplane/consumer"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/sync-pull-dataplane/launcher"
	"github.com/google/uuid"
)

// Demonstrates initiating a data transfer using a provider data plane that implements synchronous signaling start operations.
func main() {

	var wg sync.WaitGroup
	defer wg.Done()
	wg.Add(1)

	launcher.LaunchServicesAndWait(&wg)

	cp, err := controlplane.NewSimulator()
	if err != nil {
		log.Fatalf("Unable to launch control plane simulator: %v\n", err)
	}

	ctx := context.Background()
	defer ctx.Done()

	agreementID := uuid.NewString()
	datasetID := uuid.NewString()

	consumerProcessID := uuid.NewString()
	_, err = cp.ConsumerPrepare(ctx, consumerProcessID, agreementID, datasetID)
	if err != nil {
		log.Fatalf("Unable to send prepare to consumer control plane: %v\n", err)
	}

	// Signal to the provider data plane
	providerProcessID := uuid.NewString()
	da, err := cp.ProviderStart(ctx, providerProcessID, agreementID, datasetID, nil)
	if err != nil {
		log.Fatalf("Unable to send start to provider control plane: %v\n", err)
	}

	// Signal to the consumer data plane in response
	err = cp.ConsumerStart(ctx, consumerProcessID, da)
	if err != nil {
		log.Fatalf("Unable to send start to consumer control plane: %v\n", err)
	}

	if err = consumer.TransferDataset(datasetID); err != nil {
		log.Fatalf("Unable to start data transfer: %v\n", err)
	}
}
