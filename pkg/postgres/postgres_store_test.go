//go:build postgres

package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"os"
	"testing"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
)

var (
	testDB *sql.DB
	store  *PostgresStore
	ctx    = context.Background()
)

func TestMain(m *testing.M) {
	container, c := setupDb(&testing.T{})
	ctx = c
	store = &PostgresStore{db: testDB}
	db, container := SetupDatabase(&testing.T{}, ctx)
	testDB = db
	store = NewStore(testDB)

	code := m.Run()

	if err := container.Terminate(ctx); err != nil {
		panic(err)
	}
	if err := testDB.Close(); err != nil {
		panic(err)
	}
	os.Exit(code)
}

func setupDb(t *testing.T) (testcontainers.Container, context.Context) {
	ctx := context.Background()

	// Start a Postgres testcontainer once for all tests
	container, dsn := setupTestContainer(t)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	testDB = db
	// Apply schema (expects the SQL file to be available in repo)
	if _, err := testDB.ExecContext(ctx, schema); err != nil {
		panic(err)
	}
	return container, ctx
}

// Smoke test ensuring container and DB are usable.
func Test_PostgresContainer_Ready(t *testing.T) {
	if testDB == nil {
		t.Fatal("testDB not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := testDB.PingContext(ctx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func Test_Insert_New(t *testing.T) {
	err := store.Create(ctx, &dsdk.DataFlow{
		ID: uuid.New().String(),
	})
	assert.NoError(t, err)
}

func Test_Insert_Existing(t *testing.T) {
	err := store.Create(ctx, &dsdk.DataFlow{
		ID: "test-id1",
	})
	assert.NoError(t, err)
	err2 := store.Create(ctx, &dsdk.DataFlow{
		ID: "test-id1",
	})
	assert.ErrorIs(t, err2, dsdk.ErrConflict)
}

func Test_Insert_MissingID(t *testing.T) {
	err := store.Create(ctx, &dsdk.DataFlow{})
	assert.ErrorIs(t, err, dsdk.ErrInvalidInput)
}

func Test_Delete_NonExisting(t *testing.T) {
	err := store.Delete(ctx, "non-existing")
	assert.ErrorIs(t, err, dsdk.ErrNotFound)
}

func Test_Delete_Existing(t *testing.T) {
	id := uuid.New().String()
	err := store.Create(ctx, &dsdk.DataFlow{
		ID: id,
	})
	assert.NoError(t, err)
	err2 := store.Delete(ctx, id)
	assert.NoError(t, err2)
}

func Test_Save_Exists_ShouldUpdate(t *testing.T) {
	id := uuid.New().String()
	err := store.Create(ctx, &dsdk.DataFlow{
		ID: id,
	})
	assert.NoError(t, err)
	err2 := store.Save(ctx, &dsdk.DataFlow{
		ID:          id,
		AgreementID: "new-agreement-id",
	})
	assert.NoError(t, err2)

	// verify the number of rows
	var num int
	_ = testDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM data_flows WHERE id = $1", id).Scan(&num)
	assert.Equal(t, 1, num)

	// verify that the update went through
	var agr string
	err = testDB.QueryRowContext(ctx, "SELECT (agreement_id) FROM data_flows WHERE id = $1", id).Scan(&agr)
	assert.NoError(t, err)
	assert.Equal(t, "new-agreement-id", agr)
}

func Test_Save_NotExists_ShouldCreateNew(t *testing.T) {
	id := uuid.New().String()
	err2 := store.Save(ctx, &dsdk.DataFlow{
		ID:          id,
		AgreementID: "agreement-id",
	})
	assert.NoError(t, err2)

	// verify the number of rows
	var num int
	e := testDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM data_flows WHERE id = $1", id).Scan(&num)
	assert.NoError(t, e)
	assert.Equal(t, 1, num)
}

func Test_Save_InvalidInput(t *testing.T) {
	err2 := store.Save(ctx, &dsdk.DataFlow{
		AgreementID: "agreement-id",
	})
	assert.ErrorIs(t, err2, dsdk.ErrInvalidInput)
}

func Test_FindById(t *testing.T) {
	id := uuid.New().String()
	err := store.Save(ctx, &dsdk.DataFlow{
		ID: id,
	})
	assert.NoError(t, err)

	found, err := store.FindById(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, id, found.ID)
}

func Test_FindById_WithDataAddress(t *testing.T) {
	id := uuid.New().String()
	err := store.Save(ctx, &dsdk.DataFlow{
		ID: id,
		SourceDataAddress: &dsdk.DataAddress{
			EndpointType:       "https://w3id.org/idsa/v4.1/HTTP",
			Endpoint:           "https://example.com",
			EndpointProperties: []dsdk.EndpointProperty{{Name: "foo", Value: "bar"}},
		},
	})
	assert.NoError(t, err)

	found, err := store.FindById(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, id, found.ID)
	value, ok := found.SourceDataAddress.Property("foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", value)
	assert.Nil(t, found.DestinationDataAddress)
}

func Test_FindById_RoundTripsAllFields(t *testing.T) {
	id := uuid.New().String()
	flow := &dsdk.DataFlow{
		ID:               id,
		Consumer:         true,
		AgreementID:      "agreement-id",
		DatasetID:        "dataset-id",
		RuntimeID:        "runtime-id",
		ParticipantID:    "participant-id",
		DataspaceContext: "dataspace-context",
		CounterPartyID:   "counterparty-id",
		Profile:          "https://w3id.org/dspace-sig/profile/http-pull",
		Claims:           map[string]any{"membership": "active"},
		Labels:           []string{"gold", "blue"},
		Metadata:         map[string]any{"region": "westeurope"},
		ControlPlaneID:   "control-plane-id",
		SuspendedBy:      dsdk.SuspendedByRemote,
		State:            dsdk.Suspended,
		StateCount:       3,
		ErrorDetail:      "maintenance",
		DestinationDataAddress: &dsdk.DataAddress{
			EndpointType: "https://w3id.org/idsa/v4.1/HTTP",
			Endpoint:     "https://example.com",
		},
	}
	assert.NoError(t, store.Create(ctx, flow))

	found, err := store.FindById(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, flow.Profile, found.Profile)
	assert.Equal(t, flow.Claims, found.Claims)
	assert.Equal(t, flow.Labels, found.Labels)
	assert.Equal(t, flow.Metadata, found.Metadata)
	assert.Equal(t, flow.ControlPlaneID, found.ControlPlaneID)
	assert.Equal(t, dsdk.SuspendedByRemote, found.SuspendedBy)
	assert.Equal(t, dsdk.Suspended, found.State)
	assert.Equal(t, uint(3), found.StateCount)
	assert.Equal(t, "maintenance", found.ErrorDetail)
	assert.Equal(t, flow.DestinationDataAddress, found.DestinationDataAddress)
	assert.Nil(t, found.SourceDataAddress)

	found.SuspendedBy = dsdk.SuspendedByNone
	found.State = dsdk.Started
	found.Labels = nil
	assert.NoError(t, store.Save(ctx, found))

	updated, err := store.FindById(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, dsdk.SuspendedByNone, updated.SuspendedBy)
	assert.Equal(t, dsdk.Started, updated.State)
	assert.Nil(t, updated.Labels)
}

func Test_FindById_NotExists(t *testing.T) {
	_, err := store.FindById(ctx, "non-existing")
	assert.ErrorIs(t, err, dsdk.ErrNotFound)
}
