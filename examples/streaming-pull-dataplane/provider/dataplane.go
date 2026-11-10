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

package provider

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/natsservices"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/memory"
)

// ProviderDataPlane demonstrates how to use the Data Plane SDK. This implementation supports pull event streaming.
type ProviderDataPlane struct {
	sdk                   *dsdk.DataPlaneSDK
	signalingServer       *http.Server
	authService           *natsservices.AuthService
	connectionInvalidator ConnectionInvalidator
	publisherService      *EventPublisherService
}

func NewDataPlane(authService *natsservices.AuthService,
	invalidator ConnectionInvalidator,
	publisherService *EventPublisherService) (*ProviderDataPlane, error) {
	providerDataPlane := &ProviderDataPlane{authService: authService, connectionInvalidator: invalidator, publisherService: publisherService}

	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithAuthorization(common.DemoAuthorization{}),
		dsdk.WithControlPlaneStore(memory.NewInMemoryControlPlaneStore()),
		dsdk.WithPrepareProcessor(providerDataPlane.prepareProcessor),
		dsdk.WithStartProcessor(providerDataPlane.startProcessor),
		dsdk.WithSuspendProcessor(providerDataPlane.suspendProcessor),
		dsdk.WithTerminateProcessor(providerDataPlane.terminateProcessor),
	)
	if err != nil {
		return nil, err
	}

	providerDataPlane.sdk = sdk

	return providerDataPlane, nil
}

// SDK returns the data plane's SDK instance.
func (d *ProviderDataPlane) SDK() *dsdk.DataPlaneSDK {
	return d.sdk
}

func (d *ProviderDataPlane) Init() {
	d.signalingServer = common.StartSignalingServer(d.sdk, common.ProviderSignalingPort, "[Provider Data Plane] Signaling server")
}

func (d *ProviderDataPlane) Shutdown(ctx context.Context) {
	if d.signalingServer != nil {
		if err := d.signalingServer.Shutdown(ctx); err != nil {
			log.Printf("Provider signaling server shutdown error: %v", err)
		}
	}
	log.Println("Provider data plane shutdown")
}

func (d *ProviderDataPlane) prepareProcessor(_ context.Context,
	_ *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	_ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
	return nil, errors.New("not supported on provider")
}

func (d *ProviderDataPlane) startProcessor(_ context.Context,
	flow *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	options *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
	if options.Duplicate {
		// Perform de-duplication. This code path is not needed, but it demonstrates how de-deduplication can be handled
	}

	token, err := d.authService.CreateToken(flow.ID, true)
	if err != nil {
		return nil, err
	}

	channel := flow.ID + "." + natsservices.ForwardSuffix
	replyChannel := flow.ID + "." + natsservices.ReplySuffix
	da, err := dsdk.NewDataAddressBuilder().
		EndpointType(natsservices.NATSEndpointType).
		Endpoint(natsservices.NatsUrl).
		Property(natsservices.TokenKey, token).
		Property(natsservices.ChannelKey, channel).
		Property(natsservices.ReplyChannelKey, replyChannel).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build data address: %w", err)
	}

	// Start publishing events. In a real system, this could be done via a queue or notification mechanism
	d.publisherService.Start(channel)

	log.Printf("[Provider Data Plane] Starting transfer for %s\n", flow.CounterPartyID)
	return &dsdk.DataFlowStatusMessage{State: dsdk.Started, DataAddress: da}, nil
}

func (d *ProviderDataPlane) suspendProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	channel := flow.ID + "." + natsservices.ForwardSuffix
	d.publisherService.Terminate(channel)
	log.Printf("[Provider Data Plane] Suspending transfer for %s\n", flow.CounterPartyID)
	return d.invalidate(flow)
}

func (d *ProviderDataPlane) terminateProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	channel := flow.ID + "." + natsservices.ForwardSuffix
	d.publisherService.Terminate(channel)
	log.Printf("[Provider Data Plane] Terminating transfer for %s\n", flow.CounterPartyID)
	return d.invalidate(flow)
}

func (d *ProviderDataPlane) invalidate(flow *dsdk.DataFlow) error {
	// Connection terminated after to avoid case where a client reconnects before the token is invalidated
	defer d.connectionInvalidator.InvalidateConnection(flow.ID)
	err := d.authService.InvalidateToken(flow.ID)
	if err != nil {
		return err
	}
	return nil
}

type ConnectionInvalidator interface {
	InvalidateConnection(dataFlowID string)
}
