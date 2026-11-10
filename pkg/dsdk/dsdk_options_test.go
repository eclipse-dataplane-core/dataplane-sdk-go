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

package dsdk

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_WithStore(t *testing.T) {
	store := NewMockDataplaneStore(t)
	sdk := &DataPlaneSDK{}

	option := WithStore(store)
	option(sdk)

	assert.Equal(t, store, sdk.Store)
}

func Test_WithTransactionContext(t *testing.T) {
	trxContext := &mockTrxContext{}
	sdk := &DataPlaneSDK{}

	option := WithTransactionContext(trxContext)
	option(sdk)

	assert.Equal(t, trxContext, sdk.TrxContext)
}

func Test_WithMonitor(t *testing.T) {
	monitor := &defaultLogMonitor{}
	sdk := &DataPlaneSDK{}

	option := WithMonitor(monitor)
	option(sdk)

	assert.Equal(t, monitor, sdk.Monitor)
}

func Test_WithPrepareProcessor(t *testing.T) {
	processor := func(context context.Context, flow *DataFlow, sdk *DataPlaneSDK, options *ProcessorOptions) (*DataFlowStatusMessage, error) {
		return &DataFlowStatusMessage{State: Prepared}, nil
	}
	sdk := &DataPlaneSDK{}

	option := WithPrepareProcessor(processor)
	option(sdk)

	require.NotNil(t, sdk.onPrepare)
}

func Test_WithStartProcessor(t *testing.T) {
	processor := func(context context.Context, flow *DataFlow, sdk *DataPlaneSDK, options *ProcessorOptions) (*DataFlowStatusMessage, error) {
		return &DataFlowStatusMessage{State: Started}, nil
	}
	sdk := &DataPlaneSDK{}

	option := WithStartProcessor(processor)
	option(sdk)

	require.NotNil(t, sdk.onStart)
}

func Test_WithTerminateProcessor(t *testing.T) {
	handler := func(context.Context, *DataFlow) error {
		return nil
	}
	sdk := &DataPlaneSDK{}

	option := WithTerminateProcessor(handler)
	option(sdk)

	require.NotNil(t, sdk.onTerminate)
}

func Test_WithSuspendProcessor(t *testing.T) {
	handler := func(context.Context, *DataFlow) error {
		return nil
	}
	sdk := &DataPlaneSDK{}

	option := WithSuspendProcessor(handler)
	option(sdk)

	require.NotNil(t, sdk.onSuspend)
}

func Test_WithStartedProcessor(t *testing.T) {
	handler := func(context.Context, *DataFlow) error {
		return nil
	}
	sdk := &DataPlaneSDK{}

	option := WithStartedProcessor(handler)
	option(sdk)

	require.NotNil(t, sdk.onStarted)
}

func Test_WithResumeProcessor(t *testing.T) {
	processor := func(context context.Context, flow *DataFlow, sdk *DataPlaneSDK, options *ProcessorOptions) (*DataFlowStatusMessage, error) {
		return &DataFlowStatusMessage{State: Started}, nil
	}
	sdk := &DataPlaneSDK{}

	option := WithResumeProcessor(processor)
	option(sdk)

	require.NotNil(t, sdk.onResume)
}

func Test_WithCompleteProcessor(t *testing.T) {
	handler := func(context.Context, *DataFlow) error {
		return nil
	}
	sdk := &DataPlaneSDK{}

	option := WithCompleteProcessor(handler)
	option(sdk)

	require.NotNil(t, sdk.onComplete)
}

func Test_NewDataPlaneSDK_WithoutOptionalFields(t *testing.T) {
	store := NewMockDataplaneStore(t)
	trxContext := &mockTrxContext{}

	sdk, err := NewDataPlaneSDK(
		WithStore(store),
		WithTransactionContext(trxContext),
	)

	require.NoError(t, err)
	require.NotNil(t, sdk)
	assert.Equal(t, store, sdk.Store)
	assert.Equal(t, trxContext, sdk.TrxContext)
	assert.IsType(t, defaultLogMonitor{}, sdk.Monitor)
	assert.NotNil(t, sdk.onPrepare)
	assert.NotNil(t, sdk.onStart)
	assert.NotNil(t, sdk.onTerminate)
	assert.NotNil(t, sdk.onSuspend)
	assert.NotNil(t, sdk.onStarted)
	assert.NotNil(t, sdk.onResume)
	assert.NotNil(t, sdk.onComplete)
}

func Test_NewDataPlaneSDK_MissingStore(t *testing.T) {
	trxContext := &mockTrxContext{}

	sdk, err := NewDataPlaneSDK(
		WithTransactionContext(trxContext),
	)

	require.Error(t, err)
	assert.Nil(t, sdk)
	assert.Contains(t, err.Error(), "store is required")
}

func Test_NewDataPlaneSDK_MissingTransactionContext(t *testing.T) {
	store := NewMockDataplaneStore(t)

	sdk, err := NewDataPlaneSDK(
		WithStore(store),
	)

	require.Error(t, err)
	assert.Nil(t, sdk)
	assert.Contains(t, err.Error(), "transaction context is required")
}
