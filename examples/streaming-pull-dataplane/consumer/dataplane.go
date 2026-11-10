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

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/natsservices"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/memory"
)

type ConsumerDataPlane struct {
	sdk             *dsdk.DataPlaneSDK
	signalingServer *http.Server
	dataServer      *http.Server
	eventSubscriber *natsservices.EventSubscriber
}

func NewDataPlane(eventSubscriber *natsservices.EventSubscriber) (*ConsumerDataPlane, error) {
	dataplane := &ConsumerDataPlane{eventSubscriber: eventSubscriber}

	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithAuthorization(common.DemoAuthorization{}),
		dsdk.WithControlPlaneStore(memory.NewInMemoryControlPlaneStore()),
		dsdk.WithPrepareProcessor(dataplane.prepareProcessor),
		dsdk.WithStartedProcessor(dataplane.startedProcessor),
	)
	if err != nil {
		return nil, err
	}
	dataplane.sdk = sdk
	return dataplane, nil
}

// SDK returns the data plane's SDK instance.
func (d *ConsumerDataPlane) SDK() *dsdk.DataPlaneSDK {
	return d.sdk
}

func (d *ConsumerDataPlane) Init() {
	d.signalingServer = common.StartSignalingServer(d.sdk, common.ConsumerSignalingPort, "[Consumer Data Plane] Signaling server")
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
	_ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {

	log.Printf("[Consumer Data Plane] Prepared transfer for participant %s dataset %s\n", flow.ParticipantID, flow.DatasetID)
	return &dsdk.DataFlowStatusMessage{State: dsdk.Prepared}, nil
}

// startedProcessor runs on /started: the provider's DataAddress is on the flow as its source address. The
// specification makes it optional, but this data plane cannot pull without it.
func (d *ConsumerDataPlane) startedProcessor(_ context.Context, flow *dsdk.DataFlow) error {
	if flow.SourceDataAddress == nil {
		return fmt.Errorf("%w: the started message contains no data address", dsdk.ErrInvalidInput)
	}
	endpoint := flow.SourceDataAddress.Endpoint
	token, found := parseToken(natsservices.TokenKey, flow.SourceDataAddress)
	if !found {
		return errors.New("token not found in endpoint properties")
	}
	channel, found := parseToken(natsservices.ChannelKey, flow.SourceDataAddress)
	if !found {
		return errors.New("channel not found in endpoint properties")
	}

	d.eventSubscriber.CloseConnection(flow.ID) // Close any existing connection

	err := d.eventSubscriber.Subscribe(channel, endpoint, channel, token)
	if err != nil {
		return err
	}
	log.Printf("[Consumer Data Plane] Started NATS subscriber for participant %s dataset %s\n", flow.ParticipantID, flow.DatasetID)
	return nil
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
	return da.Property(keyValue)
}
