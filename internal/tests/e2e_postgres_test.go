//go:build postgres

package tests

import (
	"testing"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/postgres"
)

// postgresStores backs a data plane with the Postgres data flow and control plane stores of the shared test database.
func postgresStores(*testing.T) e2eStores {
	return e2eStores{
		flows:         postgres.NewStore(database),
		trx:           postgres.NewDBTransactionContext(database),
		controlPlanes: postgres.NewControlPlaneStore(database),
	}
}

// Test_E2E_Postgres runs scenarios with the provider data plane on the Postgres stores, including the control plane
// registration over HTTP. The consumer uses the memory stores, so that the two data planes do not share the
// control_planes table.
func Test_E2E_Postgres(t *testing.T) {
	for _, s := range []e2eScenario{
		{profile: e2ePushProfile, async: true},
		{profile: e2ePullProfile, async: false},
	} {
		t.Run(s.name(), func(t *testing.T) {
			tr := startTransfer(t, s, memoryStores(t), postgresStores(t))

			controlPlane, err := postgres.NewControlPlaneStore(database).FindById(t.Context(), e2eControlPlaneID)
			if err != nil {
				t.Fatalf("control plane not registered in Postgres: %v", err)
			}
			if controlPlane.Endpoint != tr.cp.server.URL {
				t.Fatalf("registered endpoint %q, want %q", controlPlane.Endpoint, tr.cp.server.URL)
			}

			tr.suspendAndResume(t, s)
			tr.complete(t, s)
		})
	}
}
