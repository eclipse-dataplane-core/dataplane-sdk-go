package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupTestContainer(t *testing.T) (testcontainers.Container, string) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:15-alpine",
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("5432/tcp"),
			wait.ForSQL("5432/tcp", "postgres", func(host string, port nat.Port) string {
				return fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())
			}),
		),
		Env: map[string]string{
			"POSTGRES_DB":       "testdb",
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("Failed to get container port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())
	return container, dsn
}

//go:embed dataflow_schema.sql
var schema string

//go:embed control_plane_schema.sql
var controlPlaneSchema string

// Schemas returns the SQL that creates all tables of the SDK's Postgres stores.
func Schemas() string {
	return schema + "\n" + controlPlaneSchema
}

func SetupDatabase(t *testing.T, ctx context.Context) (*sql.DB, testcontainers.Container) {

	// Start a Postgres testcontainer once for all tests
	container, dsn := setupTestContainer(t)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Apply schema (expects the SQL file to be available in repo)
	if _, err := db.ExecContext(ctx, Schemas()); err != nil {
		panic(err)
	}
	return db, container
}
