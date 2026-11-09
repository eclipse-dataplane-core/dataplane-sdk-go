package memory

import (
	"context"
	"testing"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newControlPlane(id string) *dsdk.ControlPlane {
	return &dsdk.ControlPlane{
		ID:       id,
		Endpoint: "https://cp.example.com/" + id,
		Authorization: dsdk.AuthorizationEntry{
			Type:       "oauth2_client_credentials",
			Properties: map[string]any{"clientId": "client-" + id},
		},
	}
}

func TestInMemoryControlPlaneStore(t *testing.T) {
	store := NewInMemoryControlPlaneStore()
	ctx := context.Background()

	_, err := store.FindById(ctx, "cp-1")
	assert.ErrorIs(t, err, dsdk.ErrNotFound)

	controlPlane := newControlPlane("cp-1")
	require.NoError(t, store.Save(ctx, controlPlane))
	require.NoError(t, store.Save(ctx, newControlPlane("cp-0")))

	// the store keeps its own copy
	controlPlane.Authorization.Properties["clientId"] = "changed"
	found, err := store.FindById(ctx, "cp-1")
	require.NoError(t, err)
	clientID, _ := found.Authorization.Property("clientId")
	assert.Equal(t, "client-cp-1", clientID)
	found.Authorization.Properties["clientId"] = "changed"
	again, _ := store.FindById(ctx, "cp-1")
	clientID, _ = again.Authorization.Property("clientId")
	assert.Equal(t, "client-cp-1", clientID)

	// save replaces
	replacement := &dsdk.ControlPlane{ID: "cp-1", Endpoint: "https://new.example.com", Authorization: dsdk.AuthorizationEntry{Type: "none"}}
	require.NoError(t, store.Save(ctx, replacement))
	found, _ = store.FindById(ctx, "cp-1")
	assert.Equal(t, replacement, found)

	all, err := store.FindAll(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.Equal(t, "cp-0", all[0].ID)
	assert.Equal(t, "cp-1", all[1].ID)

	require.NoError(t, store.Delete(ctx, "cp-1"))
	assert.ErrorIs(t, store.Delete(ctx, "cp-1"), dsdk.ErrNotFound)
	_, err = store.FindById(ctx, "cp-1")
	assert.ErrorIs(t, err, dsdk.ErrNotFound)

	assert.ErrorIs(t, store.Save(ctx, &dsdk.ControlPlane{}), dsdk.ErrInvalidInput)
	assert.ErrorIs(t, store.Save(ctx, nil), dsdk.ErrInvalidInput)
	assert.ErrorIs(t, store.Delete(ctx, ""), dsdk.ErrInvalidInput)
}

func TestInMemoryControlPlaneStore_EmptyFindAll(t *testing.T) {
	all, err := NewInMemoryControlPlaneStore().FindAll(context.Background())
	require.NoError(t, err)
	assert.Empty(t, all)
}

type ctxKey struct{}

func TestInMemoryTrxContext_PassesContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKey{}, "value")
	var got any
	require.NoError(t, InMemoryTrxContext{}.Execute(ctx, func(ctx context.Context) error {
		got = ctx.Value(ctxKey{})
		return nil
	}))
	assert.Equal(t, "value", got)
}
