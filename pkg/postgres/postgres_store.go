//go:build postgres

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
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

func (p PostgresStore) FindById(ctx context.Context, id string) (*dsdk.DataFlow, error) {
	query := `SELECT * FROM data_flows WHERE id = $1`

	var df dsdk.DataFlow
	var callbackAddressJson string
	var sourceDataAddressJson, destDataAddressJson *string

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
		&callbackAddressJson,
		&df.TransferType.DestinationType,
		&df.TransferType.FlowType,
		&sourceDataAddressJson,
		&destDataAddressJson,
		&df.State,
		&df.StateCount,
		&df.StateTimestamp,
		&df.ErrorDetail,
		&df.CreatedAt,
		&df.UpdatedAt,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, dsdk.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if err := df.CallbackAddress.UnmarshalJSON([]byte(callbackAddressJson)); err != nil {
		return nil, err
	}

	if sourceDataAddressJson != nil {
		if err := json.Unmarshal([]byte(*sourceDataAddressJson), &df.SourceDataAddress); err != nil {
			return nil, err
		}
	}

	if destDataAddressJson != nil {
		if err := json.Unmarshal([]byte(*destDataAddressJson), &df.DestinationDataAddress); err != nil {
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
		    callback_address,
		    transfer_type_dest,
		    transfer_type_flowtype,
		    source_data_address,
		    dest_data_address,
		    state,
		    state_timestamp_ms,
		    error_detail,
		    created_at_ms,
		    updated_at_ms
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`

	cba, err := flow.CallbackAddress.MarshalJSON()
	if err != nil {
		return err
	}
	_, err = p.db.ExecContext(ctx, query,
		flow.ID,
		flow.Consumer,
		flow.AgreementID,
		flow.DatasetID,
		flow.RuntimeID,
		flow.ParticipantID,
		flow.DataspaceContext,
		flow.CounterPartyID,
		cba,
		flow.TransferType.DestinationType,
		flow.TransferType.FlowType,
		toJson(flow.SourceDataAddress),
		toJson(flow.DestinationDataAddress),
		flow.State,
		time.Now().UnixMilli(),
		flow.ErrorDetail,
		time.Now().UnixMilli(),
		time.Now().UnixMilli(),
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
		    callback_address = $8,
		    transfer_type_dest = $9,
		    transfer_type_flowtype = $10,
		    source_data_address = $11,
		    dest_data_address = $12,
		    state = $13,
			state_timestamp_ms = $14,
		    error_detail = $15,
		    updated_at_ms = $16
		WHERE id = $17`

		_, err := p.db.ExecContext(ctx, query,
			flow.Consumer,
			flow.AgreementID,
			flow.DatasetID,
			flow.RuntimeID,
			flow.ParticipantID,
			flow.DataspaceContext,
			flow.CounterPartyID,
			toJson(flow.CallbackAddress),
			flow.TransferType.DestinationType,
			flow.TransferType.FlowType,
			toJson(flow.SourceDataAddress),
			toJson(flow.DestinationDataAddress),
			flow.State,
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

func toJson(v any) *string {
	j, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("Failed to marshal: %v", err)
		return nil
	}
	s := string(j)
	return &s
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
