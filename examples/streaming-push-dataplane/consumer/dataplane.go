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

package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/examples/natsservices"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"github.com/metaform/dataplane-sdk-go/pkg/memory"
)

// ConsumerDataPlane demonstrates how to use the Data Plane SDK. This implementation supports push event streaming.
type ConsumerDataPlane struct {
	api                   *dsdk.DataPlaneApi
	signalingServer       *http.Server
	authService           *natsservices.AuthService
	connectionInvalidator ConnectionInvalidator
	eventSubscriber       *natsservices.EventSubscriber
	natsUrl               string
}

func NewDataPlane(authService *natsservices.AuthService,
	invalidator ConnectionInvalidator,
	natsUrl string,
	eventSubscriber *natsservices.EventSubscriber) (*ConsumerDataPlane, error) {

	dataPlane := &ConsumerDataPlane{
		authService:           authService,
		connectionInvalidator: invalidator,
		natsUrl:               natsUrl,
		eventSubscriber:       eventSubscriber}

	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithPrepareProcessor(dataPlane.prepareProcessor),
		dsdk.WithStartProcessor(dataPlane.startProcessor),
		dsdk.WithSuspendProcessor(dataPlane.suspendProcessor),
		dsdk.WithTerminateProcessor(dataPlane.terminateProcessor),
	)
	if err != nil {
		return nil, err
	}

	dataPlane.api = dsdk.NewDataPlaneApi(sdk)

	return dataPlane, nil
}

func (d *ConsumerDataPlane) Init() {
	d.signalingServer = common.NewSignalingServer(d.api, common.ConsumerSignalingPort)

	// Start signaling server
	go func() {
		log.Printf("[Consumer Data Plane] Signaling server listening on port %d\n", common.ConsumerSignalingPort)
		if err := d.signalingServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Consumer signaling server failed to start: %v", err)
		}
	}()
}

func (d *ConsumerDataPlane) Shutdown(ctx context.Context) {
	if d.signalingServer != nil {
		if err := d.signalingServer.Shutdown(ctx); err != nil {
			log.Printf("Consumer signaling server shutdown error: %v", err)
		}
	}
	log.Println("Consumer data plane shutdown")
}

func (d *ConsumerDataPlane) prepareProcessor(_ context.Context,
	flow *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	if options.Duplicate {
		// Perform de-duplication. This code path is not needed, but it demonstrates how de-deduplication can be handled
	}

	token, err := d.authService.CreateToken(flow.ID, false)
	if err != nil {
		return nil, err
	}

	channel := flow.ID + "." + natsservices.ForwardSuffix

	err = d.eventSubscriber.Subscribe(channel, d.natsUrl, channel, token)
	if err != nil {
		return nil, err
	}

	replyChannel := flow.ID + "." + natsservices.ReplySuffix
	da, err := dsdk.NewDataAddressBuilder().
		Property(dsdk.EndpointType, natsservices.NATSEndpointType).
		Property(dsdk.EndpointKey, natsservices.NatsUrl).
		EndpointProperty(natsservices.TokenKey, "string", token).
		EndpointProperty(natsservices.ChannelKey, "string", channel).
		EndpointProperty(natsservices.ReplyChannelKey, "string", replyChannel).
		Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build data address: %w", err)
	}

	log.Printf("[Consumer Data Plane] Prepare transfer for %s\n", flow.CounterPartyID)
	return &dsdk.DataFlowResponseMessage{State: dsdk.Prepared, DataAddress: da}, nil
}

func (d *ConsumerDataPlane) startProcessor(_ context.Context,
	flow *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {
	return &dsdk.DataFlowResponseMessage{State: dsdk.Started, DataAddress: options.DataAddress}, nil
}

func (d *ConsumerDataPlane) suspendProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	log.Printf("[Consumer Data Plane] Suspending transfer for %s\n", flow.CounterPartyID)
	return d.invalidate(flow)
}

func (d *ConsumerDataPlane) terminateProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	log.Printf("[Consumer Data Plane] Terminating transfer for %s\n", flow.CounterPartyID)
	return d.invalidate(flow)
}

func (d *ConsumerDataPlane) invalidate(flow *dsdk.DataFlow) error {
	// Connection terminated after to avoid case where a client reconnects before the token is invalidated
	defer d.connectionInvalidator.InvalidateConnection(flow.ID)
	err := d.authService.InvalidateToken(flow.ID)
	if err != nil {
		return err
	}
	return nil
}

type ConnectionInvalidator interface {
	InvalidateConnection(processID string)
}

func (d *ConsumerDataPlane) noopHandler(context.Context, *dsdk.DataFlow) error {
	return nil
}
