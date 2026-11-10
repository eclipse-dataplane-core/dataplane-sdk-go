// Package postgres provides PostgreSQL implementations of the dsdk stores: PostgresStore for data flows
// (dataflow_schema.sql) and PostgresControlPlaneStore for control plane registrations (control_plane_schema.sql), and
// DBTransactionContext, which runs SDK operations in a database transaction. Apply both schema files before use.
//
// The stores are compiled with the build tag "postgres".
package postgres
