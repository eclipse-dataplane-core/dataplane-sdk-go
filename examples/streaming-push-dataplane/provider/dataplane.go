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
	"log"
	"net/http"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/natsservices"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/memory"
)

type ProviderDataPlane struct {
	sdk              *dsdk.DataPlaneSDK
	signalingServer  *http.Server
	dataServer       *http.Server
	publisherService *EventPublisherService
}

func NewDataPlane(publisherService *EventPublisherService) (*ProviderDataPlane, error) {
	dataplane := &ProviderDataPlane{publisherService: publisherService}
	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithAuthorization(common.DemoAuthorization{}),
		dsdk.WithPrepareProcessor(dataplane.prepareProcessor),
		dsdk.WithStartProcessor(dataplane.startProcessor),
		dsdk.WithSuspendProcessor(dataplane.suspendProcessor),
		dsdk.WithTerminateProcessor(dataplane.terminateProcessor),
	)

	if err != nil {
		return nil, err
	}
	dataplane.sdk = sdk
	return dataplane, nil
}

func (d *ProviderDataPlane) Init() {
	d.signalingServer = common.NewSignalingServer(d.sdk, common.ProviderSignalingPort)
	// Start signaling server
	common.StartServer(d.signalingServer, "[Provider Data Plane] Signaling server")
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

func (d *ProviderDataPlane) startProcessor(ctx context.Context,
	flow *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	options *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {

	endpoint := options.DataAddress.Endpoint
	token, found := parseToken(natsservices.TokenKey, options.DataAddress)
	if !found {
		return nil, errors.New("token not found in endpoint properties")
	}
	channel, found := parseToken(natsservices.ChannelKey, options.DataAddress)
	if !found {
		return nil, errors.New("channel not found in endpoint properties")
	}

	// publisher close and reopen
	d.publisherService.Terminate(channel)
	d.publisherService.Start(flow.ID, endpoint, channel, token)

	log.Printf("[Provider Data Plane] Started NATS subscriber for participant %s dataset %s\n", flow.ParticipantID, flow.DatasetID)
	return &dsdk.DataFlowStatusMessage{State: dsdk.Started}, nil
}

func (d *ProviderDataPlane) terminateProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	d.publisherService.Terminate(flow.ID)

	log.Printf("[Provider Data Plane] Terminated transfer for %s\n", flow.CounterPartyID)
	return nil
}

func (d *ProviderDataPlane) suspendProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	d.publisherService.Terminate(flow.ID)

	log.Printf("[Provider Data Plane] Suspended transfer for %s\n", flow.CounterPartyID)
	return nil
}

func parseToken(keyValue string, da *dsdk.DataAddress) (string, bool) {
	return da.Property(keyValue)
}
