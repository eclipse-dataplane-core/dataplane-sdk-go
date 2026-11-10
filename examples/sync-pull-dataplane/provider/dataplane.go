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
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/memory"
	"github.com/google/uuid"
)

const (
	contentType      = "Content-Type"
	jsonContentType  = "application/json"
	endpointUrl      = "http://localhost:%d/datasets"
	httpEndpointType = "https://w3id.org/idsa/v4.1/HTTP"
)

// ProviderDataPlane is a provider data plane that demonstrates how to use the Data Plane SDK. This implementation supports
// the transfer of simple JSON datasets over HTTP and Data Plane Signaling start and prepare handling using synchronous responses.
type ProviderDataPlane struct {
	sdk             *dsdk.DataPlaneSDK
	tokenStore      *common.Store[tokenEntry]
	signalingServer *http.Server
	dataServer      *http.Server
}

func NewDataPlane() (*ProviderDataPlane, error) {
	providerDataPlane := &ProviderDataPlane{
		tokenStore: common.NewStore[tokenEntry](),
	}

	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithAuthorization(common.DemoAuthorization{}),
		dsdk.WithControlPlaneStore(memory.NewInMemoryControlPlaneStore()),
		dsdk.WithPrepareProcessor(providerDataPlane.prepareProcessor),
		dsdk.WithStartProcessor(providerDataPlane.startProcessor),
		dsdk.WithSuspendProcessor(providerDataPlane.suspendProcessor),
		dsdk.WithTerminateProcessor(providerDataPlane.terminateProcessor),
		dsdk.WithCompleteProcessor(providerDataPlane.terminateProcessor), // a completed transfer invalidates the token too
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
	d.dataServer = common.NewDataServer(common.ProviderDataPort, "/datasets/", d.transferDataset)

	// Start data server
	common.StartServer(d.dataServer, "[Provider Data Plane] Data server")
}

func (d *ProviderDataPlane) Shutdown(ctx context.Context) {
	if d.signalingServer != nil {
		if err := d.signalingServer.Shutdown(ctx); err != nil {
			log.Printf("Provider signaling server shutdown error: %v", err)
		}
	}
	if d.dataServer != nil {
		if err := d.dataServer.Shutdown(ctx); err != nil {
			log.Printf("Provider data server shutdown error: %v", err)
		}
	}
	log.Println("Provider data plane shutdown")
}

func (d *ProviderDataPlane) prepareProcessor(ctx context.Context, flow *dsdk.DataFlow, sdk *dsdk.DataPlaneSDK, options *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
	return nil, errors.New("not supported on provider")
}

func (d *ProviderDataPlane) startProcessor(ctx context.Context, flow *dsdk.DataFlow, sdk *dsdk.DataPlaneSDK, options *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
	token := uuid.NewString()

	if options.Duplicate {
		// Perform de-duplication. This code path is not needed, but it demonstrates how de-deduplication can be handled
		d.tokenStore.Delete(flow.ID)
	}

	// Store token first, then build data address
	tokenEntry := tokenEntry{
		token:    token,
		datsetId: flow.DatasetID,
		binding:  flow.CounterPartyID,
	}
	d.tokenStore.Create(flow.DatasetID, tokenEntry) // the token is bound to the dataset of the data flow

	da, err := dsdk.NewDataAddressBuilder().
		EndpointType(httpEndpointType).
		Endpoint(fmt.Sprintf(endpointUrl, common.ProviderDataPort)).
		Property("token", token).
		Build()
	if err != nil {
		// remove up token on error
		d.tokenStore.Delete(flow.ID)
		return nil, fmt.Errorf("failed to build data address: %w", err)
	}

	log.Printf("[Provider Data Plane] Started transfer for %s and returning access token\n", flow.CounterPartyID)
	return &dsdk.DataFlowStatusMessage{State: dsdk.Started, DataAddress: da}, nil
}

func (d *ProviderDataPlane) suspendProcessor(ctx context.Context, flow *dsdk.DataFlow) error {
	d.tokenStore.Delete(flow.ID) // invalidate token
	return nil
}

func (d *ProviderDataPlane) terminateProcessor(ctx context.Context, flow *dsdk.DataFlow) error {
	d.tokenStore.Delete(flow.ID) // invalidate token
	return nil
}

func (d *ProviderDataPlane) transferDataset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

	token, err := common.ParseToken(w, r)
	if err != nil {
		http.Error(w, "Invalid token: "+err.Error(), http.StatusBadRequest)
		return
	}

	datasetID, err := common.ParseDataset(w, r)
	if err != nil {
		http.Error(w, "Invalid URL path: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate token
	tokenEntry, found := d.tokenStore.Find(datasetID)
	if !found || tokenEntry.datsetId != datasetID || tokenEntry.token != token {
		http.Error(w, "Invalid token", http.StatusForbidden)
	}

	datasetContent := &DatasetContent{
		DatasetID: datasetID,
	}

	w.Header().Set(contentType, jsonContentType)
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(datasetContent); err != nil {
		log.Printf("[Provider Data Plane] Failed to serialize dataset: %v", err)
		http.Error(w, "unable to serialize dataset", http.StatusInternalServerError)
	}
}

type tokenEntry struct {
	token    string
	datsetId string
	binding  string
}

type DatasetContent struct {
	DatasetID string `json:"datasetID"`
}
