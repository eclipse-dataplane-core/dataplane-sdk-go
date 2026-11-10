![](./docs/logo/go.sdk.logo.stacked.svg)

A Go SDK for building data planes that implement
[Data Plane Signaling v1.0](https://eclipse-dataplane-signaling.github.io/dataplane-signaling/HEAD/), the protocol a
Dataspace Protocol control plane uses to drive data planes. The SDK manages the data flow state machine and its
persistence, serves the signaling API, calls the control plane back, and handles registration and authorization. Your
data plane plugs in the actual data transfer.

## Install

```bash
go get github.com/eclipse-dataplane-core/dataplane-sdk-go
```

The module requires Go 1.24.

## Quick start

`myAuthorization` and `issueToken` stand for your authorization profile and token issuing.

```go
package main

import (
	"context"
	"log"
	"net/http"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/memory"
)

func main() {
	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithControlPlaneStore(memory.NewInMemoryControlPlaneStore()),
		dsdk.WithAuthorization(myAuthorization), // e.g. the oauth2 package, see Authorization
		dsdk.WithStartProcessor(func(ctx context.Context, flow *dsdk.DataFlow, _ *dsdk.DataPlaneSDK, _ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
			// provider-pull: hand out the address the consumer pulls from
			address, err := dsdk.NewDataAddressBuilder().
				EndpointType("https://w3id.org/idsa/v4.1/HTTP").
				Endpoint("https://provider.example.com/data/"+flow.DatasetID).
				Property("authorization", issueToken(flow)).
				Build()
			if err != nil {
				return nil, err
			}
			return &dsdk.DataFlowStatusMessage{State: dsdk.Started, DataAddress: address}, nil
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	// serves the signaling API under /v1
	log.Fatal(http.ListenAndServe(":8080", dsdk.NewHandler(sdk)))
}
```

## Endpoints

`dsdk.NewHandler(sdk)` serves the data plane endpoints of the specification under the base path `/v1`
(`dsdk.WithBasePath` changes it):

| Method | Path                                    | Body                                 | Response                                     |
|--------|-----------------------------------------|--------------------------------------|----------------------------------------------|
| POST   | `/v1/dataflows/prepare`                 | `DataFlowPrepareMessage`             | 200 or 202 + `DataFlowStatusMessage`         |
| POST   | `/v1/dataflows/start`                   | `DataFlowStartMessage`               | 200 or 202 + `DataFlowStatusMessage`         |
| POST   | `/v1/dataflows/{id}/started`            | `DataFlowStartedNotificationMessage` | 200 + `DataFlowStatusMessage`                |
| POST   | `/v1/dataflows/{id}/suspend`            | `DataFlowSuspendMessage`             | 200                                          |
| POST   | `/v1/dataflows/{id}/resume`             | `DataFlowResumeMessage`              | 200 + `DataFlowStatusMessage`                |
| POST   | `/v1/dataflows/{id}/terminate`          | `DataFlowTerminateMessage`           | 200                                          |
| POST   | `/v1/dataflows/{id}/completed`          | none                                 | 200                                          |
| GET    | `/v1/dataflows/{id}/status`             | none                                 | 200 + `DataFlowStatusResponseMessage`        |
| PUT    | `/v1/controlplanes`                     | `ControlPlaneRegistrationMessage`    | 200 (only with a `ControlPlaneStore`)        |
| DELETE | `/v1/controlplanes/{controlPlaneId}`    | none                                 | 204 (only with a `ControlPlaneStore`)        |

A 202 carries a `Location` header pointing at the status endpoint. Errors are answered with an `ErrorResponse`
(`{"error": "..."}`): 400 for invalid messages or transitions, 401 for failed authentication, 403 for another control
plane's data flow, 404 for unknown data flows and 409 for a `/start` on an existing data flow.

## Data flows and processors

The SDK implements the specification's state machine (INITIALIZED, PREPARING, PREPARED, STARTING, STARTED, SUSPENDED,
COMPLETED, TERMINATED; COMPLETED and TERMINATED are final) and persists every transition. Your code plugs in as
processors:

| Option                    | Invoked for            | Kind      | Reports               |
|---------------------------|------------------------|-----------|-----------------------|
| `WithPrepareProcessor`    | `/prepare` (consumer)  | processor | PREPARED or PREPARING |
| `WithStartProcessor`      | `/start` (provider)    | processor | STARTED or STARTING   |
| `WithStartedProcessor`    | `/started` (consumer)  | handler   |                       |
| `WithResumeProcessor`     | `/resume`              | processor | STARTED               |
| `WithSuspendProcessor`    | `/suspend`             | handler   |                       |
| `WithTerminateProcessor`  | `/terminate`           | handler   |                       |
| `WithCompleteProcessor`   | `/completed`           | handler   |                       |

Processors and handlers may modify the `DataFlow` they receive; the SDK persists it. A processor
(`DataFlowProcessor`) returns a `DataFlowStatusMessage` with the resulting state and optionally a `DataAddress`; the
SDK fills in `messageId` and `dataFlowId`. A handler (`DataFlowHandler`) only returns an error, which aborts the
operation and leaves the data flow unchanged; the SDK then performs the transition itself. On `/started` the address
received from the provider (pull) is already on the flow (`SourceDataAddress`) when the handler runs, and the flow
always ends up STARTED. A re-delivered `/prepare` reaches the processor again with `ProcessorOptions.Duplicate` set;
re-delivered started, suspend, terminate and completed messages are acknowledged without invoking the handler.

- **Synchronous or asynchronous.** Report PREPARED or STARTED to complete the transition in the response (200).
  Report PREPARING or STARTING to answer 202 and complete it later with `NotifyPrepared` or `NotifyStarted`.
- **Push or pull** is derived from the last `-` token of the data flow's `profile`, case-insensitively
  (`.../http-push`, `.../http-pull`); see `DataFlow.IsPush` and `IsPull`.
- **Data addresses.** In a push transfer, the consumer hands out the address the provider pushes to (prepare response),
  and the provider receives it with `/start`. In a pull transfer, the provider hands out the address the consumer pulls
  from (start response), and the consumer receives it with `/started`. The SDK enforces these rules (400 otherwise),
  drops addresses a processor returns where the specification allows none, and stores the addresses on the data flow
  (`SourceDataAddress`, `DestinationDataAddress`).

## Control plane callbacks

The data plane reports asynchronous transitions and its own decisions to the control plane:

| Method                                  | Callback                                        |
|-----------------------------------------|-------------------------------------------------|
| `NotifyPrepared(ctx, id, processor)`    | `POST {endpoint}/transfers/{id}/dataflow/prepared`  |
| `NotifyStarted(ctx, id, processor)`     | `POST {endpoint}/transfers/{id}/dataflow/started`   |
| `NotifyCompleted(ctx, id)`              | `POST {endpoint}/transfers/{id}/dataflow/completed` |
| `NotifyErrored(ctx, id, cause)`         | `POST {endpoint}/transfers/{id}/dataflow/errored`   |

Each call transitions the data flow, sends a `DataFlowStatusMessage` and persists the transition only if the control
plane answers 2xx; otherwise the stored data flow is unchanged and the error is returned (`*ControlPlaneError` for an
HTTP error). 5xx responses and network errors are retried with exponential backoff (`RetryPolicy`, default 3 attempts
starting at 200ms, configured on `NewHTTPControlPlaneClient` and set with `WithControlPlaneClient`). Because the
transition is persisted after the callback, a control plane that reads `/status` while handling the callback still
sees the previous state.

A data flow's control plane is the one that sent its `/prepare` or `/start`. Its endpoint and authorization come from
its registration (see below), or from `WithControlPlane(dsdk.ControlPlane{...})` for a single, statically configured
control plane. `SuspendLocally` and `ResumeLocally` suspend and resume a data flow on the data plane's own initiative;
the specification defines no callback for them, and only the data plane can resume such a flow.

## Registration

- **Control planes register with the data plane.** With `WithControlPlaneStore(store)`, `NewHandler` serves
  `PUT /v1/controlplanes` and `DELETE /v1/controlplanes/{controlPlaneId}`; `RegisterControlPlane` and
  `DeleteControlPlane` do the same in code. Registrations are replaced on update.
- **The data plane registers with a control plane** with `RegisterDataPlane(ctx, controlPlane, message)`
  (`PUT {endpoint}/dataplanes`) and `DeregisterDataPlane` (`DELETE {endpoint}/dataplanes/{id}`). The message's
  `authorization` entry tells the control plane how to authorize its requests to this data plane.

## Authorization

Authorization is mandatory. Register one or more authorization profiles with `WithAuthorization`; each implements
`dsdk.Authorization`:

- Inbound, every endpoint requires an `Authorization` header that a registered profile accepts; it yields the calling
  control plane's id. Requests without it are answered with 401, before the body is read. A control plane that
  accesses another control plane's data flow or registration gets 403, and a warning is logged.
- Outbound, callbacks and registrations carry the `Authorization` header that the profile computes from the target
  control plane's authorization entry. Registrations whose authorization type no profile handles are rejected.

`pkg/authorization/oauth2` implements the `oauth2_client_credentials` profile: outbound, it obtains tokens from the
control plane's `tokenEndpoint` with the client credentials grant and caches them until they expire; inbound, it
verifies bearer JWTs against a JWKS endpoint and uses their `sub` claim as the control plane id.

```go
authorization, err := oauth2.NewClientCredentialsAuthorization(ctx, "https://idp.example.com/.well-known/jwks.json",
	oauth2.WithIssuer("https://idp.example.com"))
if err != nil {
	log.Fatal(err)
}
sdk, err := dsdk.NewDataPlaneSDK(
	dsdk.WithStore(store),
	dsdk.WithTransactionContext(trx),
	dsdk.WithAuthorization(authorization),
)
```

`NewHandler`'s `WithAuthenticator` option replaces the inbound scheme. `WithAuthorizationDisabled()` turns
authorization off explicitly, e.g. for the Data Plane Signaling TCK, which sends `Authorization: dummy-authorization`.

Control plane endpoints must use https, except for loopback hosts; `WithInsecureEndpoints()` allows http.

## Storage

| Package        | Data flows        | Control planes              | Transactions           |
|----------------|-------------------|-----------------------------|------------------------|
| `pkg/memory`   | `InMemoryStore`   | `InMemoryControlPlaneStore` | `InMemoryTrxContext`   |
| `pkg/postgres` | `PostgresStore`   | `PostgresControlPlaneStore` | `DBTransactionContext` |

The Postgres stores need the tables of `pkg/postgres/dataflow_schema.sql` and `pkg/postgres/control_plane_schema.sql`
and are compiled with the build tag `postgres`. Other databases plug in through `dsdk.DataplaneStore`,
`dsdk.ControlPlaneStore` and `dsdk.TransactionContext`.

## Examples

Each sample starts its data planes and the sample control plane (`examples/controlplane`), registers them with each
other, and runs a transfer. They use a demo authorization profile (`examples/common`) that is for samples only.

| Sample                     | Shows                                                                          |
|----------------------------|--------------------------------------------------------------------------------|
| `sync-pull-dataplane`      | HTTP consumer-pull with synchronous prepare and start, consumer `NotifyCompleted` |
| `streaming-pull-dataplane` | NATS event stream, consumer-pull, terminated by the provider control plane      |
| `streaming-push-dataplane` | NATS event stream, provider-push, terminated by the provider control plane      |
| `async-start-dataplane`    | asynchronous start (202) completed with `NotifyStarted`                         |

```bash
cd examples/sync-pull-dataplane && go run .
```

The samples use the ports 7070 (control plane), 8080/8181 (provider) and 9090/9191 (consumer); the streaming samples
also start an embedded NATS server.

## Testing

```bash
go generate ./...           # generates the mocks
go test ./...               # unit and end-to-end tests
go test -tags postgres ./... # also the Postgres stores and integration tests; needs Docker (testcontainers)
```
