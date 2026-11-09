package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
)

// InMemoryControlPlaneStore is a thread-safe in-memory implementation of dsdk.ControlPlaneStore.
type InMemoryControlPlaneStore struct {
	mu            sync.RWMutex
	controlPlanes map[string]*dsdk.ControlPlane
}

func NewInMemoryControlPlaneStore() *InMemoryControlPlaneStore {
	return &InMemoryControlPlaneStore{controlPlanes: make(map[string]*dsdk.ControlPlane)}
}

// Save creates or replaces the control plane with the same id.
func (s *InMemoryControlPlaneStore) Save(_ context.Context, controlPlane *dsdk.ControlPlane) error {
	if controlPlane == nil || controlPlane.ID == "" {
		return dsdk.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.controlPlanes[controlPlane.ID] = controlPlane.Copy()
	return nil
}

func (s *InMemoryControlPlaneStore) FindById(_ context.Context, id string) (*dsdk.ControlPlane, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	controlPlane, ok := s.controlPlanes[id]
	if !ok {
		return nil, dsdk.ErrNotFound
	}
	return controlPlane.Copy(), nil
}

// FindAll returns copies of all control planes, ordered by id.
func (s *InMemoryControlPlaneStore) FindAll(_ context.Context) ([]*dsdk.ControlPlane, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*dsdk.ControlPlane, 0, len(s.controlPlanes))
	for _, controlPlane := range s.controlPlanes {
		result = append(result, controlPlane.Copy())
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (s *InMemoryControlPlaneStore) Delete(_ context.Context, id string) error {
	if id == "" {
		return dsdk.ErrInvalidInput
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.controlPlanes[id]; !ok {
		return dsdk.ErrNotFound
	}
	delete(s.controlPlanes, id)
	return nil
}
