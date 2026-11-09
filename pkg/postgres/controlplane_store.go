//go:build postgres

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
)

// queryExecutor is implemented by *sql.DB and *sql.Tx.
type queryExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// PostgresControlPlaneStore is a dsdk.ControlPlaneStore backed by the control_planes table
// (control_plane_schema.sql). Inside DBTransactionContext.Execute it uses the transaction from the context.
type PostgresControlPlaneStore struct {
	db *sql.DB
}

func NewControlPlaneStore(db *sql.DB) *PostgresControlPlaneStore {
	return &PostgresControlPlaneStore{db: db}
}

func (p *PostgresControlPlaneStore) executor(ctx context.Context) queryExecutor {
	if tx, ok := ctx.Value(DBTransactionKey).(*sql.Tx); ok {
		return tx
	}
	return p.db
}

// Save creates or replaces the control plane with the same id.
func (p *PostgresControlPlaneStore) Save(ctx context.Context, controlPlane *dsdk.ControlPlane) error {
	if controlPlane == nil || controlPlane.ID == "" {
		return dsdk.ErrInvalidInput
	}
	auth, err := json.Marshal(controlPlane.Authorization)
	if err != nil {
		return fmt.Errorf("encoding authorization of control plane %s: %w", controlPlane.ID, err)
	}
	_, err = p.executor(ctx).ExecContext(ctx, `
		INSERT INTO control_planes (id, endpoint, auth) VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET endpoint = EXCLUDED.endpoint, auth = EXCLUDED.auth`,
		controlPlane.ID, controlPlane.Endpoint, string(auth))
	return err
}

func (p *PostgresControlPlaneStore) FindById(ctx context.Context, id string) (*dsdk.ControlPlane, error) {
	row := p.executor(ctx).QueryRowContext(ctx, `SELECT id, endpoint, auth FROM control_planes WHERE id = $1`, id)
	controlPlane, err := scanControlPlane(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, dsdk.ErrNotFound
	}
	return controlPlane, err
}

// FindAll returns all control planes, ordered by id.
func (p *PostgresControlPlaneStore) FindAll(ctx context.Context) ([]*dsdk.ControlPlane, error) {
	rows, err := p.executor(ctx).QueryContext(ctx, `SELECT id, endpoint, auth FROM control_planes ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]*dsdk.ControlPlane, 0)
	for rows.Next() {
		controlPlane, err := scanControlPlane(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, controlPlane)
	}
	return result, rows.Err()
}

func (p *PostgresControlPlaneStore) Delete(ctx context.Context, id string) error {
	res, err := p.executor(ctx).ExecContext(ctx, `DELETE FROM control_planes WHERE id = $1`, id)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return dsdk.ErrNotFound
	}
	return nil
}

func scanControlPlane(row interface{ Scan(dest ...any) error }) (*dsdk.ControlPlane, error) {
	var controlPlane dsdk.ControlPlane
	var auth []byte
	if err := row.Scan(&controlPlane.ID, &controlPlane.Endpoint, &auth); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(auth, &controlPlane.Authorization); err != nil {
		return nil, fmt.Errorf("decoding authorization of control plane %s: %w", controlPlane.ID, err)
	}
	return &controlPlane, nil
}
