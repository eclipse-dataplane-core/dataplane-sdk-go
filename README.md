![](./docs/logo/go.sdk.logo.stacked.svg)

A Data Plane SDK for Go. This SDK provides components for creating Go-based data planes that interface with Dataspace
Protocol Control Planes via the `Data Plane Signaling API`. The SDK includes state management, support for reliable
qualities of service, and error handling.

## Main Operations

### 1. Prepare

- Purpose: Prepares for receiving data
- Function: `Prepare(ctx context.Context, message DataFlowPrepareMessage) (*DataFlowResponseMessage, error)`
- Returns: Response message or error

### 2. Start

- Purpose: Initiates a data flow on the provider side
- Function: `Start(ctx context.Context, message DataFlowStartMessage) (*DataFlowResponseMessage, error)`
- Takes: DataFlowStartMessage as input

### 3. Terminate

- Purpose: Ends a data flow
- Function: `Terminate(ctx context.Context, processID string) error`
- Requires: Process ID

### 4. Suspend

- Purpose: Temporarily halts a data flow
- Function: `Suspend(ctx context.Context, processID string) error`
- Requires: Process ID

### 5. Control plane callbacks

- Purpose: Completes asynchronous transitions and reports data-plane-initiated changes to the control plane
- Functions: `NotifyPrepared(ctx, id, processor)`, `NotifyStarted(ctx, id, processor)`, `NotifyCompleted(ctx, id)`,
  `NotifyErrored(ctx, id, cause)`
- Each call transitions the flow, POSTs a `DataFlowStatusMessage` to
  `{controlPlaneURL}/transfers/{id}/dataflow/{prepared|started|completed|errored}` and persists the flow only when the
  control plane answers 2xx. 5xx responses and network errors are retried (see `RetryPolicy`).
- Configure the endpoint with `WithControlPlane(url)`, or register control planes (see below); see
  `examples/async-start-dataplane`.

### 6. Registration

- Control planes register with the data plane: configure `WithControlPlaneStore(store)` (`memory` and `postgres`
  implementations are provided, see `control_plane_schema.sql`) and `NewHandler` serves `PUT /v1/controlplanes` and
  `DELETE /v1/controlplanes/{controlPlaneId}`, or call `RegisterControlPlane` / `DeleteControlPlane` directly.
- Prepare and start record the calling control plane on the data flow (`WithControlPlaneIdentifier`; by default the
  only registered control plane), and the `Notify*` callbacks go to its registered endpoint.
- The data plane registers with a control plane via `RegisterDataPlane(ctx, controlPlaneURL, message)`
  (`PUT {controlPlaneURL}/dataplanes`) and `DeregisterDataPlane` (`DELETE {controlPlaneURL}/dataplanes/{id}`).

## Key Features

### State Management

- Maintains different states for data flows:
    - Preparing
    - Prepared
    - Starting
    - Started
    - Terminated
    - Suspended

### Built-in Capabilities

- Deduplication logic for handling duplicate messages
- Transaction support via TransactionContext
- Comprehensive error handling and propagation
- Extension points through callback functions

## Extension Points

- : Custom prepare logic `OnPrepare`
- : Custom start logic `OnStart`
- : Custom termination logic `OnTerminate`
- : Custom suspension logic `OnSuspend`

## Usage Example

See the examples.
