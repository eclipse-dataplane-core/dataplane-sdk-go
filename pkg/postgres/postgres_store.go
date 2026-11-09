//go:build postgres

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"reflect"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/lib/pq"
)

type PostgresStore struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

const columns = `id, version, consumer, agreement_id, dataset_id, runtime_id, participant_id, dataspace_context,
	counterparty_id, profile, claims, labels, metadata, control_plane_id, suspended_by, source_data_address,
	dest_data_address, state, state_count, state_timestamp_ms, error_detail, created_at_ms, updated_at_ms`

func (p PostgresStore) FindById(ctx context.Context, id string) (*dsdk.DataFlow, error) {
	query := `SELECT ` + columns + ` FROM data_flows WHERE id = $1`

	var df dsdk.DataFlow
	var claims, labels, metadata, sourceDataAddress, destDataAddress []byte
	var errorDetail sql.NullString
	var suspendedBy string

	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&df.ID,
		&df.Version,
		&df.Consumer,
		&df.AgreementID,
		&df.DatasetID,
		&df.RuntimeID,
		&df.ParticipantID,
		&df.DataspaceContext,
		&df.CounterPartyID,
		&df.Profile,
		&claims,
		&labels,
		&metadata,
		&df.ControlPlaneID,
		&suspendedBy,
		&sourceDataAddress,
		&destDataAddress,
		&df.State,
		&df.StateCount,
		&df.StateTimestamp,
		&errorDetail,
		&df.CreatedAt,
		&df.UpdatedAt,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, dsdk.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	df.SuspendedBy = dsdk.SuspendedBy(suspendedBy)
	df.ErrorDetail = errorDetail.String

	for _, field := range []struct {
		data   []byte
		target any
	}{
		{claims, &df.Claims},
		{labels, &df.Labels},
		{metadata, &df.Metadata},
		{sourceDataAddress, &df.SourceDataAddress},
		{destDataAddress, &df.DestinationDataAddress},
	} {
		if err := fromJson(field.data, field.target); err != nil {
			return nil, err
		}
	}

	return &df, nil
}

func (p PostgresStore) Create(ctx context.Context, flow *dsdk.DataFlow) error {
	if flow.ID == "" {
		return dsdk.ErrInvalidInput
	}
	query := `
		INSERT INTO data_flows (
			id,
		    consumer,
		    agreement_id,
		    dataset_id,
		    runtime_id,
		    participant_id,
		    dataspace_context,
		    counterparty_id,
		    profile,
		    claims,
		    labels,
		    metadata,
		    control_plane_id,
		    suspended_by,
		    source_data_address,
		    dest_data_address,
		    state,
		    state_count,
		    state_timestamp_ms,
		    error_detail,
		    created_at_ms,
		    updated_at_ms
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)`

	now := time.Now().UnixMilli()
	_, err := p.db.ExecContext(ctx, query,
		flow.ID,
		flow.Consumer,
		flow.AgreementID,
		flow.DatasetID,
		flow.RuntimeID,
		flow.ParticipantID,
		flow.DataspaceContext,
		flow.CounterPartyID,
		flow.Profile,
		toJson(flow.Claims),
		toJson(flow.Labels),
		toJson(flow.Metadata),
		flow.ControlPlaneID,
		string(flow.SuspendedBy),
		toJson(flow.SourceDataAddress),
		toJson(flow.DestinationDataAddress),
		flow.State,
		flow.StateCount,
		now,
		flow.ErrorDetail,
		now,
		now,
	)

	if err != nil {
		if isUniqueViolation(err) {
			return dsdk.ErrConflict
		}
		return err
	}

	return nil
}

func (p PostgresStore) Save(ctx context.Context, flow *dsdk.DataFlow) error {
	if flow.ID == "" {
		return dsdk.ErrInvalidInput
	}
	if exists(p.db, ctx, flow.ID) {
		// update
		query := `
		UPDATE data_flows
		SET
		    consumer = $1,
		    agreement_id = $2,
		    dataset_id = $3,
		    runtime_id = $4,
		    participant_id = $5,
		    dataspace_context = $6,
		    counterparty_id = $7,
		    profile = $8,
		    claims = $9,
		    labels = $10,
		    metadata = $11,
		    control_plane_id = $12,
		    suspended_by = $13,
		    source_data_address = $14,
		    dest_data_address = $15,
		    state = $16,
		    state_count = $17,
		    state_timestamp_ms = $18,
		    error_detail = $19,
		    updated_at_ms = $20
		WHERE id = $21`

		_, err := p.db.ExecContext(ctx, query,
			flow.Consumer,
			flow.AgreementID,
			flow.DatasetID,
			flow.RuntimeID,
			flow.ParticipantID,
			flow.DataspaceContext,
			flow.CounterPartyID,
			flow.Profile,
			toJson(flow.Claims),
			toJson(flow.Labels),
			toJson(flow.Metadata),
			flow.ControlPlaneID,
			string(flow.SuspendedBy),
			toJson(flow.SourceDataAddress),
			toJson(flow.DestinationDataAddress),
			flow.State,
			flow.StateCount,
			flow.StateTimestamp,
			flow.ErrorDetail,
			time.Now().UnixMilli(),
			flow.ID)
		if err != nil {
			return err
		}
		return nil
	}
	return p.Create(ctx, flow)
}

func (p PostgresStore) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM data_flows WHERE id = $1`
	res, err := p.db.ExecContext(ctx, query, id)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return dsdk.ErrNotFound
	}
	return nil
}

// toJson marshals v into a JSONB value; nil maps, slices and pointers are stored as SQL NULL.
func toJson(v any) *string {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || ((rv.Kind() == reflect.Map || rv.Kind() == reflect.Slice || rv.Kind() == reflect.Pointer) && rv.IsNil()) {
		return nil
	}
	j, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("Failed to marshal: %v", err)
		return nil
	}
	s := string(j)
	return &s
}

func fromJson(data []byte, target any) error {
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, target)
}

func exists(db *sql.DB, ctx context.Context, id string) bool {
	query := `SELECT COUNT(*) FROM data_flows WHERE id = $1`
	var count int
	err := db.QueryRowContext(ctx, query, id).Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
}

// isUniqueViolation detects Postgres unique violations by checking for SQLState "23505".
func isUniqueViolation(err error) bool {
	var pgErr *pq.Error
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}
