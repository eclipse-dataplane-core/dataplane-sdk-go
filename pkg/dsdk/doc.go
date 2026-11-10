// Package dsdk is an SDK for building data planes that implement the Data Plane Signaling protocol v1.0
// (https://eclipse-dataplane-signaling.github.io/dataplane-signaling/HEAD/).
//
// A DataPlaneSDK (NewDataPlaneSDK) manages the data flow state machine and persists data flows in a DataplaneStore
// within a TransactionContext. The data plane's own logic plugs in as processors, which report the resulting state
// and optionally a DataAddress (WithPrepareProcessor and WithStartProcessor, e.g. PREPARED for a synchronous or
// PREPARING for an asynchronous prepare, and WithResumeProcessor), and as handlers, after which the SDK performs the
// transition (WithStartedProcessor, WithSuspendProcessor, WithTerminateProcessor and WithCompleteProcessor).
//
// NewHandler serves the data plane endpoints of the signaling API (and the control plane registration endpoints if a
// ControlPlaneStore is configured) under /v1. Every request must be authorized by an Authorization registered with
// WithAuthorization, unless WithAuthorizationDisabled is set.
//
// The data plane calls its control plane with the Notify* methods (asynchronous transitions and data-plane-initiated
// completion or failure) and registers itself with RegisterDataPlane. Control planes are configured statically
// (WithControlPlane) or registered at runtime (WithControlPlaneStore).
//
// Push or pull is derived from the last '-' token of a data flow's profile, e.g. ".../http-pull". The SDK enforces
// which side sends a DataAddress: the consumer for push transfers (prepare), the provider for pull transfers (start).
package dsdk
