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
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/memory"
)

const (
	contentType     = "Content-Type"
	jsonContentType = "application/json"
)

// ConsumerDataPlane is a consumer data plane that demonstrates how to use the Data Plane SDK. This implementation supports
// the transfer of simple JSON datasets over HTTP and Data Plane Signaling start and prepare handling using synchronous responses.
// After a transfer is started, clients obtain the access token from this data plane and issue the request to the provider data plane.
//
// Note that this data plane does not proxy requests to the provider data plane. This is recommended best practice as it avoids
// unnecessary overhead and a potential failure point.
//
// This data plane implements non-finite data transfers. Multiple requests may be issued to the provider data plane over a
// period of time. For example, the dataset could be access to an API.
type ConsumerDataPlane struct {
	sdk             *dsdk.DataPlaneSDK
	signalingServer *http.Server
	dataServer      *http.Server
	tokenStore      *common.Store[tokenEntry]
}

func NewDataPlane() (*ConsumerDataPlane, error) {
	dataplane := &ConsumerDataPlane{tokenStore: common.NewStore[tokenEntry]()}

	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithAuthorization(common.DemoAuthorization{}),
		dsdk.WithPrepareProcessor(dataplane.prepareProcessor),
		dsdk.WithStartProcessor(dataplane.startProcessor),
	)
	if err != nil {
		return nil, err
	}
	dataplane.sdk = sdk
	return dataplane, nil
}

func (d *ConsumerDataPlane) Init() {
	d.signalingServer = common.NewSignalingServer(d.sdk, common.ConsumerSignalingPort)
	// Start signaling server
	common.StartServer(d.signalingServer, "[Consumer Data Plane] Signaling server")

	d.dataServer = common.NewDataServer(common.ConsumerDataPort, "/tokens/", d.getEndpointToken)
	common.StartServer(d.dataServer, "[Consumer Data Plane] Data server")
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

func (d *ConsumerDataPlane) startProcessor(_ context.Context,
	flow *dsdk.DataFlow,
	_ *dsdk.DataPlaneSDK,
	options *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
	log.Printf("[Consumer Data Plane] Transfer access token available for participant %s dataset %s\n", flow.ParticipantID, flow.DatasetID)
	endpoint := options.DataAddress.Endpoint
	token, found := options.DataAddress.Property("token")
	if !found {
		return nil, errors.New("token not found in endpoint properties")
	}
	d.tokenStore.Create(flow.DatasetID, tokenEntry{datasetID: flow.DatasetID, token: token, endpoint: endpoint})
	return &dsdk.DataFlowStatusMessage{State: dsdk.Started}, nil
}

func (d *ConsumerDataPlane) getEndpointToken(w http.ResponseWriter, r *http.Request) {
	// Check if it's a GET request
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

	datasetID, err := common.ParseDataset(w, r)
	if err != nil {
		http.Error(w, "Invalid URL path: "+err.Error(), http.StatusBadRequest)
		return
	}
	entry, exists := d.tokenStore.Find(datasetID)
	if !exists {
		http.Error(w, "Token not found for dataset", http.StatusNotFound)
		return
	}

	response := common.TokenResponse{
		Token:    entry.token,
		Endpoint: entry.endpoint,
	}

	w.Header().Set(contentType, jsonContentType)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

type tokenEntry struct {
	datasetID string
	token     string
	endpoint  string
}
