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

package streaming

import (
	"context"
	"log"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/controlplane"
	"github.com/google/uuid"
)

// TerminateScenario coordinates a simulated data transfer scenario and forcibly terminates it after a predefined duration.
// TerminateScenario runs a streaming transfer with the given profile (ending in -push or -pull) and terminates it.
func TerminateScenario(profile string) {
	cp, err := controlplane.NewSimulator()
	if err != nil {
		log.Fatalf("Unable to launch control plane simulator: %v\n", err)
	}

	ctx := context.Background()
	defer ctx.Done()

	agreementID := uuid.NewString()
	datasetID := uuid.NewString()

	consumerProcessID := uuid.NewString()
	consumerDA, err := cp.ConsumerPrepare(ctx, consumerProcessID, agreementID, datasetID, profile)
	if err != nil {
		log.Fatalf("Unable to send prepare to consumer control plane: %v\n", err)
	}
	providerProcessID := uuid.NewString()

	// Signal to the provider data plane
	providerDA, err := cp.ProviderStart(ctx, providerProcessID, agreementID, datasetID, profile, consumerDA)
	if err != nil {
		log.Fatalf("Unable to send start to provider control plane: %v\n", err)
	}

	err = cp.ConsumerStarted(ctx, consumerProcessID, providerDA)
	if err != nil {
		log.Fatalf("Unable to send started to consumer control plane: %v\n", err)
	}

	// Simulate ongoing processing
	time.Sleep(10 * time.Second)

	// Terminate the transfer and forcibly disconnect the client
	err = cp.ProviderTerminate(ctx, providerProcessID)
	if err != nil {
		log.Fatalf("Unable to send terminate to provider control plane: %v\n", err)
	}

	// Simulate ongoing processing until terminate is completed
	time.Sleep(2 * time.Second)
}
