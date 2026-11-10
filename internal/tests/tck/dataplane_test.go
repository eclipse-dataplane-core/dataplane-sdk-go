//go:build tck

package tck

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/memory"
	"github.com/stretchr/testify/require"
)

const (
	controlPlaneID       = "tck-control-plane"
	tckAuthorizationType = "tck"

	// agreementAsync is the agreement id of TCK tests that expect the data plane to prepare or start asynchronously.
	agreementAsync = "async"
	// agreementComplete is the agreement id of TCK tests that expect the data plane to complete the transfer.
	agreementComplete = "complete"

	// asyncDelay lets the SDK store the data flow, which it does after the processor returned, before the callback
	// transitions it.
	asyncDelay = 50 * time.Millisecond
	// completeDelay gives the TCK time to see the data flow in STARTED, which it polls every 100ms, and to start
	// listening for the completed callback.
	completeDelay = 250 * time.Millisecond
)

// tckDataPlane is the data plane under test. It has no wire protocol: the processors only do what a TCK test asks
// for through the agreement id of its data flow.
type tckDataPlane struct {
	sdk *dsdk.DataPlaneSDK

	callbacks sync.WaitGroup
	mu        sync.Mutex
	errors    []error
}

// newDataPlane creates a data plane that sends its callbacks to the TCK at controlPlaneURL.
func newDataPlane(t *testing.T, controlPlaneURL string) *tckDataPlane {
	dataPlane := &tckDataPlane{}
	sdk, err := dsdk.NewDataPlaneSDK(
		dsdk.WithStore(memory.NewInMemoryStore()),
		dsdk.WithTransactionContext(memory.InMemoryTrxContext{}),
		dsdk.WithAuthorization(tckAuthorization{}),
		dsdk.WithControlPlane(dsdk.ControlPlane{
			ID:            controlPlaneID,
			Endpoint:      controlPlaneURL,
			Authorization: dsdk.AuthorizationEntry{Type: tckAuthorizationType},
		}),
		dsdk.WithPrepareProcessor(dataPlane.prepare),
		dsdk.WithStartProcessor(dataPlane.start),
		dsdk.WithStartedProcessor(dataPlane.started),
	)
	require.NoError(t, err)
	dataPlane.sdk = sdk
	return dataPlane
}

// prepare is called on the consumer.
func (d *tckDataPlane) prepare(_ context.Context, flow *dsdk.DataFlow, _ *dsdk.DataPlaneSDK, _ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
	if flow.AgreementID != agreementAsync {
		return status(flow, dsdk.Prepared), nil
	}
	id := flow.ID
	d.callback(asyncDelay, "prepared", id, func(ctx context.Context) error {
		_, err := d.sdk.NotifyPrepared(ctx, id, reportState(dsdk.Prepared))
		return err
	})
	return &dsdk.DataFlowStatusMessage{State: dsdk.Preparing}, nil
}

// start is called on the provider.
func (d *tckDataPlane) start(_ context.Context, flow *dsdk.DataFlow, _ *dsdk.DataPlaneSDK, _ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
	id := flow.ID
	switch flow.AgreementID {
	case agreementAsync:
		d.callback(asyncDelay, "started", id, func(ctx context.Context) error {
			_, err := d.sdk.NotifyStarted(ctx, id, reportState(dsdk.Started))
			return err
		})
		return &dsdk.DataFlowStatusMessage{State: dsdk.Starting}, nil
	case agreementComplete:
		d.complete(id)
	}
	return status(flow, dsdk.Started), nil
}

// started is called on the consumer.
func (d *tckDataPlane) started(_ context.Context, flow *dsdk.DataFlow) error {
	if flow.AgreementID == agreementComplete {
		d.complete(flow.ID)
	}
	return nil
}

func (d *tckDataPlane) complete(id string) {
	d.callback(completeDelay, "completed", id, func(ctx context.Context) error {
		return d.sdk.NotifyCompleted(ctx, id)
	})
}

// callback sends a callback to the TCK after delay, and records its error.
func (d *tckDataPlane) callback(delay time.Duration, action string, id string, send func(ctx context.Context) error) {
	d.callbacks.Add(1)
	go func() {
		defer d.callbacks.Done()
		time.Sleep(delay)
		if err := send(context.Background()); err != nil {
			d.mu.Lock()
			defer d.mu.Unlock()
			d.errors = append(d.errors, fmt.Errorf("%s callback for data flow %s: %w", action, id, err))
		}
	}()
}

// wait blocks until all callbacks have been sent.
func (d *tckDataPlane) wait() {
	d.callbacks.Wait()
}

// callbackErrors returns the errors of the callbacks that could not be sent or were rejected by the TCK.
func (d *tckDataPlane) callbackErrors() []error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]error(nil), d.errors...)
}

// reportState returns a processor that reports the given state, with a DataAddress if the flow has to provide one.
func reportState(state dsdk.DataFlowState) dsdk.DataFlowProcessor {
	return func(_ context.Context, flow *dsdk.DataFlow, _ *dsdk.DataPlaneSDK, _ *dsdk.ProcessorOptions) (*dsdk.DataFlowStatusMessage, error) {
		return status(flow, state), nil
	}
}

// status reports the given state. A push consumer and a pull provider hand out the DataAddress of their endpoint.
func status(flow *dsdk.DataFlow, state dsdk.DataFlowState) *dsdk.DataFlowStatusMessage {
	message := &dsdk.DataFlowStatusMessage{State: state}
	if (flow.Consumer && flow.IsPush()) || (!flow.Consumer && flow.IsPull()) {
		message.DataAddress = &dsdk.DataAddress{EndpointType: "https://w3id.org/idsa/v4.1/HTTP", Endpoint: "http://localhost/data"}
	}
	return message
}

// tckAuthorization identifies the TCK by the token it sends with every request (tckToken), and sends the same token
// with the callbacks.
type tckAuthorization struct{}

func (tckAuthorization) Type() string {
	return tckAuthorizationType
}

func (tckAuthorization) AuthorizationHeader(context.Context, dsdk.AuthorizationEntry) (string, error) {
	return tckToken, nil
}

func (tckAuthorization) ExtractCallerID(_ context.Context, authorizationHeader string) (string, error) {
	if authorizationHeader != tckToken {
		return "", fmt.Errorf("%w: not the token of the TCK", dsdk.ErrUnauthorized)
	}
	return controlPlaneID, nil
}
