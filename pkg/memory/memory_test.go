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

package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewInMemoryStore(t *testing.T) {
	store := NewInMemoryStore()

	assert.NotNil(t, store)
	assert.NotNil(t, store.flows)
	assert.Equal(t, 0, len(store.flows))
}

func TestInMemoryStore_FindById(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	t.Run("find existing flow", func(t *testing.T) {
		// Create a test flow
		flow := &dsdk.DataFlow{
			ID:               "test-flow-1",
			RuntimeID:        "runtime-1",
			ParticipantID:    "participant-1",
			DataspaceContext: "dataspace-1",
			CounterPartyID:   "counterparty-1",
			State:            dsdk.Started,
			CreatedAt:        time.Now().UnixMilli(),
		}

		// Add flow to store
		store.flows[flow.ID] = flow

		// Find the flow
		result, err := store.FindById(ctx, "test-flow-1")

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test-flow-1", result.ID)
		assert.Equal(t, "runtime-1", result.RuntimeID)

		// Verify it's a copy, not the same reference
		assert.NotSame(t, flow, result)
	})

	t.Run("find non-existing flow", func(t *testing.T) {
		result, err := store.FindById(ctx, "non-existing")

		assert.Error(t, err)
		assert.True(t, errors.Is(err, dsdk.ErrNotFound))
		assert.Nil(t, result)
	})

}

func TestInMemoryStore_Create(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	t.Run("create valid flow", func(t *testing.T) {
		flow := &dsdk.DataFlow{
			ID:               "test-flow-1",
			RuntimeID:        "runtime-1",
			ParticipantID:    "participant-1",
			DataspaceContext: "dataspace-1",
			CounterPartyID:   "counterparty-1",
			State:            dsdk.Started,
			CreatedAt:        time.Now().UnixMilli(),
		}

		err := store.Create(ctx, flow)

		assert.NoError(t, err)
		assert.Equal(t, 1, len(store.flows))

		// Verify it's stored as a copy
		storedFlow, err := store.FindById(ctx, "test-flow-1")
		require.NoError(t, err)
		assert.NotSame(t, flow, storedFlow)
		assert.Equal(t, flow.ID, storedFlow.ID)
		assert.Equal(t, flow.RuntimeID, storedFlow.RuntimeID)
	})

	t.Run("create with nil flow", func(t *testing.T) {
		err := store.Create(ctx, nil)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, dsdk.ErrInvalidInput))
	})

	t.Run("create with empty id", func(t *testing.T) {
		flow := &dsdk.DataFlow{
			RuntimeID: "runtime-1",
		}

		err := store.Create(ctx, flow)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, dsdk.ErrInvalidInput))
	})

	t.Run("create duplicate flow", func(t *testing.T) {
		flow := &dsdk.DataFlow{
			ID:        "duplicate-flow",
			RuntimeID: "runtime-1",
		}

		// Create first time
		err := store.Create(ctx, flow)
		assert.NoError(t, err)

		// Try to create again
		err = store.Create(ctx, flow)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, dsdk.ErrConflict))
	})
}

func TestInMemoryStore_Save(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	t.Run("save existing flow", func(t *testing.T) {
		// First create a flow
		originalFlow := &dsdk.DataFlow{
			ID:        "test-flow-1",
			RuntimeID: "runtime-1",
			State:     dsdk.Started,
		}

		err := store.Create(ctx, originalFlow)
		require.NoError(t, err)

		// Update the flow
		updatedFlow := &dsdk.DataFlow{
			ID:        "test-flow-1",
			RuntimeID: "runtime-1-updated",
			State:     dsdk.Completed,
			UpdatedAt: time.Now().UnixMilli(),
		}

		err = store.Save(ctx, updatedFlow)

		assert.NoError(t, err)

		// Verify the update
		storedFlow, err := store.FindById(ctx, "test-flow-1")
		require.NoError(t, err)
		assert.Equal(t, "test-flow-1", storedFlow.ID)
		assert.Equal(t, "runtime-1-updated", storedFlow.RuntimeID)
		assert.Equal(t, dsdk.Completed, storedFlow.State)

		// Verify it's stored as a copy
		assert.NotSame(t, updatedFlow, storedFlow)
	})

	t.Run("save non-existing flow", func(t *testing.T) {
		flow := &dsdk.DataFlow{
			ID:        "non-existing",
			RuntimeID: "runtime-1",
		}

		err := store.Save(ctx, flow)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, dsdk.ErrNotFound))
	})

	t.Run("save with nil flow", func(t *testing.T) {
		err := store.Save(ctx, nil)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, dsdk.ErrInvalidInput))
	})

	t.Run("save with empty id", func(t *testing.T) {
		flow := &dsdk.DataFlow{
			RuntimeID: "runtime-1",
		}

		err := store.Save(ctx, flow)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, dsdk.ErrInvalidInput))
	})
}

func TestInMemoryStore_Delete(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	t.Run("delete existing flow", func(t *testing.T) {
		// First create a flow
		flow := &dsdk.DataFlow{
			ID:        "test-flow-1",
			RuntimeID: "runtime-1",
		}

		err := store.Create(ctx, flow)
		require.NoError(t, err)
		assert.Equal(t, 1, len(store.flows))

		// Delete the flow
		err = store.Delete(ctx, "test-flow-1")

		assert.NoError(t, err)
		assert.Equal(t, 0, len(store.flows))
		_, err = store.FindById(ctx, "test-flow-1")
		require.Error(t, err)
	})

	t.Run("delete non-existing flow", func(t *testing.T) {
		err := store.Delete(ctx, "non-existing")

		assert.Error(t, err)
		assert.True(t, errors.Is(err, dsdk.ErrNotFound))
	})

	t.Run("delete with empty id", func(t *testing.T) {
		err := store.Delete(ctx, "")

		assert.Error(t, err)
		assert.True(t, errors.Is(err, dsdk.ErrInvalidInput))
	})
}

func TestMemoryIterator(t *testing.T) {
	t.Run("iterate through items", func(t *testing.T) {
		items := []string{"item1", "item2", "item3"}
		iterator := &memoryIterator[string]{
			items: items,
			index: -1,
		}

		var results []string
		for iterator.Next() {
			results = append(results, iterator.Get())
		}

		assert.Equal(t, items, results)
		assert.NoError(t, iterator.Error())
	})

	t.Run("get without next returns zero value", func(t *testing.T) {
		iterator := &memoryIterator[string]{
			items: []string{"item1"},
			index: -1,
		}

		// Get without Next should return zero value
		result := iterator.Get()
		assert.Equal(t, "", result)

		// After Next, should get the actual value
		iterator.Next()
		result = iterator.Get()
		assert.Equal(t, "item1", result)
	})

	t.Run("get after end returns zero value", func(t *testing.T) {
		iterator := &memoryIterator[string]{
			items: []string{"item1"},
			index: 1, // Beyond the array
		}

		result := iterator.Get()
		assert.Equal(t, "", result)
	})

	t.Run("close clears items", func(t *testing.T) {
		iterator := &memoryIterator[string]{
			items: []string{"item1", "item2"},
			index: 0,
		}

		err := iterator.Close()

		assert.NoError(t, err)
		assert.Nil(t, iterator.items)
	})

	t.Run("empty iterator", func(t *testing.T) {
		iterator := &memoryIterator[string]{
			items: []string{},
			index: -1,
		}

		assert.False(t, iterator.Next())
		assert.NoError(t, iterator.Error())
	})
}

func TestInMemoryStore_ThreadSafety(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 4) // 4 types of operations

	// Test concurrent creates
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				flow := &dsdk.DataFlow{
					ID:        fmt.Sprintf("flow-%d-%d", routineID, j),
					RuntimeID: fmt.Sprintf("runtime-%d-%d", routineID, j),
				}
				store.Create(ctx, flow)
			}
		}(i)
	}

	// Test concurrent reads
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				store.FindById(ctx, fmt.Sprintf("flow-%d-%d", routineID, j))
			}
		}(i)
	}

	// Test concurrent updates
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				flow := &dsdk.DataFlow{
					ID:        fmt.Sprintf("flow-%d-%d", routineID, j),
					RuntimeID: fmt.Sprintf("updated-runtime-%d-%d", routineID, j),
				}
				store.Save(ctx, flow)
			}
		}(i)
	}

	// Test concurrent deletes
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				store.Delete(ctx, fmt.Sprintf("flow-%d-%d", routineID, j))
			}
		}(i)
	}

	wg.Wait()

	// Test concurrent recovery iterations
	var iterWg sync.WaitGroup
	iterWg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer iterWg.Done()
		}()
	}

	iterWg.Wait()

	// The test passes if no race conditions are detected and no panics occur
	t.Log("Thread safety test completed successfully")
}

func TestInMemoryStore_DataIsolation(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	t.Run("modifications to returned flow don't affect stored flow", func(t *testing.T) {
		originalFlow := &dsdk.DataFlow{
			ID:        "test-flow",
			RuntimeID: "original-runtime",
			State:     dsdk.Started,
		}

		err := store.Create(ctx, originalFlow)
		require.NoError(t, err)

		// Get the flow and modify it
		retrievedFlow, err := store.FindById(ctx, "test-flow")
		require.NoError(t, err)

		retrievedFlow.RuntimeID = "modified-runtime"
		retrievedFlow.State = dsdk.Completed

		// Verify the stored flow is unchanged
		storedFlow, err := store.FindById(ctx, "test-flow")
		require.NoError(t, err)

		assert.Equal(t, "original-runtime", storedFlow.RuntimeID)
		assert.Equal(t, dsdk.Started, storedFlow.State)
	})

	t.Run("modifications to input flow don't affect stored flow", func(t *testing.T) {
		inputFlow := &dsdk.DataFlow{
			ID:        "test-flow-2",
			RuntimeID: "original-runtime",
			State:     dsdk.Started,
		}

		err := store.Create(ctx, inputFlow)
		require.NoError(t, err)

		// Modify the input flow after creation
		inputFlow.RuntimeID = "modified-runtime"
		inputFlow.State = dsdk.Completed

		// Verify the stored flow is unchanged
		storedFlow, err := store.FindById(ctx, "test-flow-2")
		require.NoError(t, err)

		assert.Equal(t, "original-runtime", storedFlow.RuntimeID)
		assert.Equal(t, dsdk.Started, storedFlow.State)
	})
}

func TestInMemoryStore_DeepCopiesNewFields(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	flow := &dsdk.DataFlow{
		ID:             "deep-copy",
		Profile:        "https://w3id.org/dspace-sig/profile/http-pull",
		Claims:         map[string]any{"membership": "active"},
		Labels:         []string{"gold"},
		Metadata:       map[string]any{"region": "westeurope"},
		ControlPlaneID: "cp-1",
		SuspendedBy:    dsdk.SuspendedByRemote,
		SourceDataAddress: &dsdk.DataAddress{
			EndpointType:       "https://w3id.org/idsa/v4.1/HTTP",
			EndpointProperties: []dsdk.EndpointProperty{{Name: "authorization", Value: "token"}},
		},
	}
	require.NoError(t, store.Create(ctx, flow))

	// mutating the caller's instance must not change the stored flow
	flow.Claims["membership"] = "revoked"
	flow.Labels[0] = "changed"
	flow.Metadata["region"] = "changed"
	flow.SourceDataAddress.EndpointProperties[0].Value = "changed"

	found, err := store.FindById(ctx, "deep-copy")
	require.NoError(t, err)
	assert.Equal(t, "active", found.Claims["membership"])
	assert.Equal(t, []string{"gold"}, found.Labels)
	assert.Equal(t, "westeurope", found.Metadata["region"])
	assert.Equal(t, "cp-1", found.ControlPlaneID)
	assert.Equal(t, dsdk.SuspendedByRemote, found.SuspendedBy)
	value, _ := found.SourceDataAddress.Property("authorization")
	assert.Equal(t, "token", value)

	// mutating a returned instance must not change the stored flow either
	found.Labels[0] = "changed"
	again, err := store.FindById(ctx, "deep-copy")
	require.NoError(t, err)
	assert.Equal(t, []string{"gold"}, again.Labels)
}
