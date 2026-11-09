package dsdk

import (
	"context"
)

//go:generate go run github.com/vektra/mockery/v2@latest --name=DataplaneStore --output=. --outpkg=dsdk --filename=mock_dataplane_store_test.go --structname=MockDataplaneStore --with-expecter --inpackage

// DataplaneStore defines the extension point for finding, creating, saving, and iterating over DataFlow entities.
type DataplaneStore interface {
	// FindById returns a DataFlow for the given id or an error.
	FindById(context.Context, string) (*DataFlow, error)
	Create(context.Context, *DataFlow) error
	Save(context.Context, *DataFlow) error
	Delete(ctx context.Context, id string) error
}

// ControlPlaneStore stores the control planes registered with this data plane.
type ControlPlaneStore interface {
	// Save creates or replaces the control plane with the same id.
	Save(ctx context.Context, controlPlane *ControlPlane) error
	// FindById returns the control plane with the given id, or ErrNotFound.
	FindById(ctx context.Context, id string) (*ControlPlane, error)
	// FindAll returns all registered control planes.
	FindAll(ctx context.Context) ([]*ControlPlane, error)
	// Delete removes the control plane with the given id, or returns ErrNotFound.
	Delete(ctx context.Context, id string) error
}

// TransactionContext defines an extension point for executing operations within a transactional context.
type TransactionContext interface {
	Execute(ctx context.Context, callback func(ctx context.Context) error) error
}

// Iterator for store operations
type Iterator[T any] interface {
	// Next advances the iterator and returns true if there is a next element
	Next() bool

	// Get returns the current element
	Get() T

	// Error returns any error that occurred during iteration
	Error() error

	// Close Releases resources associated with the iterator
	Close() error
}
