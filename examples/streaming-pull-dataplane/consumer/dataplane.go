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
	"log"
	"net/http"

	"github.com/metaform/dataplane-sdk-go/examples/common"
	"github.com/metaform/dataplane-sdk-go/examples/natsservices"
	"github.com/metaform/dataplane-sdk-go/pkg/dsdk"
	"github.com/metaform/dataplane-sdk-go/pkg/memory"
)

type ConsumerDataPlane struct {
	api             *dsdk.DataPlaneApi
	signalingServer *http.Server
	dataServer      *http.Server
	eventSubscriber *natsservices.EventSubscriber
}

func NewDataPlane(eventSubscriber *natsservices.EventSubscriber) (*ConsumerDataPlane, error) {
	dataplane := &ConsumerDataPlane{eventSubscriber: eventSubscriber}

	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithPrepareProcessor(dataplane.prepareProcessor),
		dsdk.WithStartProcessor(dataplane.startProcessor),
	)
	if err != nil {
		return nil, err
	}
	dataplane.api = dsdk.NewDataPlaneApi(sdk)
	return dataplane, nil
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
	_ *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {

	log.Printf("[Consumer Data Plane] Prepared transfer for participant %s dataset %s\n", flow.ParticipantID, flow.DatasetID)
	return &dsdk.DataFlowResponseMessage{State: dsdk.Prepared}, nil
}

func (d *ConsumerDataPlane) startProcessor(_ context.Context,
	flow *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	options *dsdk.ProcessorOptions) (*dsdk.DataFlowResponseMessage, error) {

	endpoint := options.DataAddress.Properties[dsdk.EndpointKey].(string)
	token, found := parseToken(natsservices.TokenKey, options.DataAddress)
	if !found {
		return nil, errors.New("token not found in endpoint properties")
	}
	channel, found := parseToken(natsservices.ChannelKey, options.DataAddress)
	if !found {
		return nil, errors.New("channel not found in endpoint properties")
	}

	d.eventSubscriber.CloseConnection(flow.ID) // Close any existing connection

	err := d.eventSubscriber.Subscribe(channel, endpoint, channel, token)
	if err != nil {
		return nil, err
	}
	log.Printf("[Consumer Data Plane] Started NATS subscriber for participant %s dataset %s\n", flow.ParticipantID, flow.DatasetID)
	return &dsdk.DataFlowResponseMessage{State: dsdk.Started}, nil
}

func (d *ConsumerDataPlane) terminateProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	d.eventSubscriber.CloseConnection(flow.ID)
	log.Printf("[Consumer Data Plane] Terminated transfer for %s\n", flow.CounterPartyID)
	return nil
}

func (d *ConsumerDataPlane) suspendProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	d.eventSubscriber.CloseConnection(flow.ID)
	log.Printf("[Consumer Data Plane] Suspended transfer for %s\n", flow.CounterPartyID)
	return nil
}

func parseToken(keyValue string, da *dsdk.DataAddress) (string, bool) {
	rawProps, ok := da.Properties[dsdk.EndpointProperties].([]any)
	if !ok {
		log.Fatal("endpoint properties is not a slice")
	}
	for _, item := range rawProps {
		if item.(map[string]any)["key"] == keyValue {
			return item.(map[string]any)["value"].(string), true
		}
	}
	return "", false
}
