// Package memory provides in-memory implementations of the dsdk stores (InMemoryStore, InMemoryControlPlaneStore)
// and a no-op transaction context (InMemoryTrxContext), for tests, samples and single-instance data planes that do
// not need to survive a restart. The stores keep deep copies, so callers cannot modify stored entities by accident.
package memory
