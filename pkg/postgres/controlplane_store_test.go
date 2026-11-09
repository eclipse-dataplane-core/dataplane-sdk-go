//go:build postgres

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestControlPlane(id string) *dsdk.ControlPlane {
	return &dsdk.ControlPlane{
		ID:       id,
		Endpoint: "https://cp.example.com/" + id,
		Authorization: dsdk.AuthorizationEntry{
			Type: "oauth2_client_credentials",
			Properties: map[string]any{
				"tokenEndpoint": "https://auth.example.com/token",
				"clientId":      "client-" + id,
				"clientSecret":  "secret",
			},
		},
	}
}

func Test_ControlPlaneStore_SaveFindDelete(t *testing.T) {
	controlPlanes := NewControlPlaneStore(testDB)
	id := uuid.NewString()

	_, err := controlPlanes.FindById(ctx, id)
	assert.ErrorIs(t, err, dsdk.ErrNotFound)

	controlPlane := newTestControlPlane(id)
	require.NoError(t, controlPlanes.Save(ctx, controlPlane))
	found, err := controlPlanes.FindById(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, controlPlane, found)

	// save replaces the registration
	replacement := &dsdk.ControlPlane{ID: id, Endpoint: "https://new.example.com", Authorization: dsdk.AuthorizationEntry{Type: "none"}}
	require.NoError(t, controlPlanes.Save(ctx, replacement))
	found, err = controlPlanes.FindById(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, replacement, found)

	require.NoError(t, controlPlanes.Delete(ctx, id))
	_, err = controlPlanes.FindById(ctx, id)
	assert.ErrorIs(t, err, dsdk.ErrNotFound)
	assert.ErrorIs(t, controlPlanes.Delete(ctx, id), dsdk.ErrNotFound)
}

func Test_ControlPlaneStore_FindAll(t *testing.T) {
	controlPlanes := NewControlPlaneStore(testDB)
	_, err := testDB.ExecContext(ctx, `DELETE FROM control_planes`)
	require.NoError(t, err)

	all, err := controlPlanes.FindAll(ctx)
	require.NoError(t, err)
	assert.Empty(t, all)

	require.NoError(t, controlPlanes.Save(ctx, newTestControlPlane("cp-b")))
	require.NoError(t, controlPlanes.Save(ctx, newTestControlPlane("cp-a")))
	all, err = controlPlanes.FindAll(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.Equal(t, "cp-a", all[0].ID)
	assert.Equal(t, "cp-b", all[1].ID)
	clientID, ok := all[1].Authorization.Property("clientId")
	assert.True(t, ok)
	assert.Equal(t, "client-cp-b", clientID)
}

func Test_ControlPlaneStore_InvalidInput(t *testing.T) {
	controlPlanes := NewControlPlaneStore(testDB)
	assert.ErrorIs(t, controlPlanes.Save(ctx, &dsdk.ControlPlane{}), dsdk.ErrInvalidInput)
	assert.ErrorIs(t, controlPlanes.Save(ctx, nil), dsdk.ErrInvalidInput)
}

func Test_ControlPlaneStore_UsesTransaction(t *testing.T) {
	controlPlanes := NewControlPlaneStore(testDB)
	trx := NewDBTransactionContext(testDB)
	id := uuid.NewString()

	// a failed transaction rolls the save back
	err := trx.Execute(ctx, func(ctx context.Context) error {
		require.NoError(t, controlPlanes.Save(ctx, newTestControlPlane(id)))
		found, err := controlPlanes.FindById(ctx, id)
		require.NoError(t, err, "visible inside the transaction")
		assert.Equal(t, id, found.ID)
		return errors.New("rollback")
	})
	assert.EqualError(t, err, "rollback")
	_, err = controlPlanes.FindById(ctx, id)
	assert.ErrorIs(t, err, dsdk.ErrNotFound)

	// a committed transaction keeps it
	require.NoError(t, trx.Execute(ctx, func(ctx context.Context) error {
		return controlPlanes.Save(ctx, newTestControlPlane(id))
	}))
	_, err = controlPlanes.FindById(ctx, id)
	assert.NoError(t, err)
}
