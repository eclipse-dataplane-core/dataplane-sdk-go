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
	"sync"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
)

// InMemoryStore is a thread-safe in-memory implementation of DataplaneStore
type InMemoryStore struct {
	mu    sync.RWMutex
	flows map[string]*dsdk.DataFlow
}

// NewInMemoryStore creates a new thread-safe in-memory store
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		flows: make(map[string]*dsdk.DataFlow),
	}
}

// FindById returns a DataFlow for the given id or an error
func (s *InMemoryStore) FindById(ctx context.Context, id string) (*dsdk.DataFlow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	flow, exists := s.flows[id]
	if !exists {
		return nil, dsdk.ErrNotFound
	}

	// Return a deep copy to prevent external modifications
	return flow.Copy(), nil
}

// Create creates a new DataFlow entry
func (s *InMemoryStore) Create(ctx context.Context, flow *dsdk.DataFlow) error {
	if flow == nil {
		return dsdk.ErrInvalidInput
	}
	if flow.ID == "" {
		return dsdk.ErrInvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.flows[flow.ID]; exists {
		return dsdk.ErrConflict
	}

	// Store a deep copy to prevent external modifications
	s.flows[flow.ID] = flow.Copy()
	return nil
}

// Save updates an existing DataFlow entry
func (s *InMemoryStore) Save(ctx context.Context, flow *dsdk.DataFlow) error {
	if flow == nil {
		return dsdk.ErrInvalidInput
	}
	if flow.ID == "" {
		return dsdk.ErrInvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.flows[flow.ID]; !exists {
		return dsdk.ErrNotFound
	}

	// Store a deep copy to prevent external modifications
	s.flows[flow.ID] = flow.Copy()
	return nil
}

// Delete removes a DataFlow entry by id
func (s *InMemoryStore) Delete(ctx context.Context, id string) error {
	if id == "" {
		return dsdk.ErrInvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.flows[id]; !exists {
		return dsdk.ErrNotFound
	}

	delete(s.flows, id)
	return nil
}

// memoryIterator is a simple iterator implementation for slice data
type memoryIterator[T any] struct {
	items []T
	index int
	err   error
}

// Next advances the iterator and returns true if there is a next element
func (it *memoryIterator[T]) Next() bool {
	it.index++
	return it.index < len(it.items)
}

// Get returns the current element
func (it *memoryIterator[T]) Get() T {
	if it.index < 0 || it.index >= len(it.items) {
		var zero T
		return zero
	}
	return it.items[it.index]
}

// Error returns any error that occurred during iteration
func (it *memoryIterator[T]) Error() error {
	return it.err
}

// Close releases resources associated with the iterator
func (it *memoryIterator[T]) Close() error {
	it.items = nil
	return nil
}

type InMemoryTrxContext struct {
}

func (c InMemoryTrxContext) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}
