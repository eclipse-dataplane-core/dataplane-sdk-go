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

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/controlplane"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/google/uuid"
)

// TerminateScenario runs a streaming transfer with the given profile (ending in -push or -pull) between the provider
// and consumer data planes and forcibly terminates it after a while. The sample control plane first registers in both
// directions with each data plane.
func TerminateScenario(profile string, provider *dsdk.DataPlaneSDK, consumer *dsdk.DataPlaneSDK) {
	cp, err := controlplane.NewSimulator()
	if err != nil {
		log.Fatalf("Unable to launch control plane simulator: %v\n", err)
	}
	ctx := context.Background()
	defer cp.Shutdown(ctx)

	if err := cp.Connect(ctx, provider, common.ProviderDataPlaneID, common.ProviderSignalingPort, profile); err != nil {
		log.Fatalf("Unable to connect the provider data plane: %v\n", err)
	}
	if err := cp.Connect(ctx, consumer, common.ConsumerDataPlaneID, common.ConsumerSignalingPort, profile); err != nil {
		log.Fatalf("Unable to connect the consumer data plane: %v\n", err)
	}

	agreementID := uuid.NewString()
	datasetID := uuid.NewString()

	consumerFlowID := uuid.NewString()
	prepared, err := cp.ConsumerPrepare(ctx, consumerFlowID, agreementID, datasetID, profile)
	if err != nil {
		log.Fatalf("Unable to send prepare to consumer control plane: %v\n", err)
	}
	providerFlowID := uuid.NewString()

	// Signal to the provider data plane
	// for push transfers the consumer's DataAddress from prepare goes to the provider
	started, err := cp.ProviderStart(ctx, providerFlowID, agreementID, datasetID, profile, prepared.DataAddress)
	if err != nil {
		log.Fatalf("Unable to send start to provider control plane: %v\n", err)
	}

	// for pull transfers the provider's DataAddress from start goes to the consumer
	err = cp.ConsumerStarted(ctx, consumerFlowID, started.DataAddress)
	if err != nil {
		log.Fatalf("Unable to send started to consumer control plane: %v\n", err)
	}

	// Simulate ongoing processing
	time.Sleep(10 * time.Second)

	// Terminate the transfer and forcibly disconnect the client
	err = cp.ProviderTerminate(ctx, providerFlowID)
	if err != nil {
		log.Fatalf("Unable to send terminate to provider control plane: %v\n", err)
	}

	// Simulate ongoing processing until terminate is completed
	time.Sleep(2 * time.Second)
}
