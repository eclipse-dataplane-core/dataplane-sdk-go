package dsdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// --- Prepare ---

func Test_Prepare_Pull(t *testing.T) {
	sdk, store := newTestSDK(t, WithPrepareProcessor(func(_ context.Context, flow *DataFlow, _ *DataPlaneSDK, _ *ProcessorOptions) (*DataFlowStatusMessage, error) {
		// a pull consumer must not hand out an address; the SDK drops it
		return &DataFlowStatusMessage{MessageID: "ignored", DataFlowID: "ignored", State: Prepared, DataAddress: newDataAddress()}, nil
	}))
	msg := createPrepareMessage(pullProfile)

	response, err := sdk.Prepare(context.Background(), msg)
	require.NoError(t, err)
	assert.Equal(t, Prepared, response.State)
	assert.Equal(t, msg.DataFlowID, response.DataFlowID)
	assert.NotEmpty(t, response.MessageID)
	assert.NotEqual(t, "ignored", response.MessageID)
	assert.Nil(t, response.DataAddress)

	flow := store.get(t, msg.DataFlowID)
	assert.True(t, flow.Consumer)
	assert.Equal(t, Prepared, flow.State)
	assert.Equal(t, msg.Profile, flow.Profile)
	assert.Equal(t, msg.Claims, flow.Claims)
	assert.Nil(t, flow.DestinationDataAddress)
}

func Test_Prepare_Push_ReturnsDataAddress(t *testing.T) {
	address := newDataAddress()
	sdk, store := newTestSDK(t, WithPrepareProcessor(func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowStatusMessage, error) {
		return &DataFlowStatusMessage{State: Prepared, DataAddress: address}, nil
	}))
	msg := createPrepareMessage(pushProfile)
	msg.Labels = []string{"gold"}
	msg.Metadata = map[string]any{"bucket": "b"}

	response, err := sdk.Prepare(context.Background(), msg)
	require.NoError(t, err)
	assert.Equal(t, address, response.DataAddress)

	flow := store.get(t, msg.DataFlowID)
	assert.Equal(t, address, flow.DestinationDataAddress)
	assert.Equal(t, []string{"gold"}, flow.Labels)
	assert.Equal(t, map[string]any{"bucket": "b"}, flow.Metadata)
}

func Test_Prepare_VerifyProcessorState(t *testing.T) {
	tests := []struct {
		state       DataFlowState
		expectError bool
	}{
		{Initialized, true},
		{Preparing, false},
		{Prepared, false},
		{Starting, true},
		{Started, true},
		{Suspended, true},
		{Completed, true},
		{Terminated, true},
	}
	for _, test := range tests {
		t.Run(test.state.String(), func(t *testing.T) {
			sdk, store := newTestSDK(t, WithPrepareProcessor(stateProcessor(test.state)))
			msg := createPrepareMessage(pullProfile)
			response, err := sdk.Prepare(context.Background(), msg)
			if test.expectError {
				assert.Error(t, err)
				assert.Nil(t, response)
				assert.False(t, store.has(msg.DataFlowID))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.state, store.get(t, msg.DataFlowID).State)
		})
	}
}

func Test_Prepare_Duplicate(t *testing.T) {
	var calls []bool
	sdk, store := newTestSDK(t, WithPrepareProcessor(func(_ context.Context, _ *DataFlow, _ *DataPlaneSDK, options *ProcessorOptions) (*DataFlowStatusMessage, error) {
		calls = append(calls, options.Duplicate)
		return &DataFlowStatusMessage{State: Prepared}, nil
	}))
	msg := createPrepareMessage(pullProfile)
	_, err := sdk.Prepare(context.Background(), msg)
	require.NoError(t, err)

	response, err := sdk.Prepare(context.Background(), msg)
	require.NoError(t, err)
	assert.Equal(t, Prepared, response.State)
	assert.Equal(t, []bool{false, true}, calls)
	assert.Equal(t, Prepared, store.get(t, msg.DataFlowID).State)
}

func Test_Prepare_InWrongState(t *testing.T) {
	sdk, store := newTestSDK(t)
	msg := createPrepareMessage(pullProfile)
	store.put(&DataFlow{ID: msg.DataFlowID, State: Started})

	_, err := sdk.Prepare(context.Background(), msg)
	assert.ErrorIs(t, err, ErrConflict)
}

func Test_Prepare_ProcessorError(t *testing.T) {
	sdk, store := newTestSDK(t, WithPrepareProcessor(func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowStatusMessage, error) {
		return nil, errors.New("boom")
	}))
	msg := createPrepareMessage(pullProfile)
	_, err := sdk.Prepare(context.Background(), msg)
	assert.ErrorContains(t, err, "boom")
	assert.False(t, store.has(msg.DataFlowID))
}

func Test_Prepare_ProcessorReturnsNil(t *testing.T) {
	sdk, _ := newTestSDK(t, WithPrepareProcessor(func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowStatusMessage, error) {
		return nil, nil
	}))
	_, err := sdk.Prepare(context.Background(), createPrepareMessage(pullProfile))
	assert.ErrorIs(t, err, errNilResponse)
}

func Test_Prepare_InvalidMessage(t *testing.T) {
	sdk, _ := newTestSDK(t)
	msg := createPrepareMessage(pullProfile)
	msg.Claims = nil
	_, err := sdk.Prepare(context.Background(), msg)
	assert.ErrorIs(t, err, ErrValidation)
}

// --- Start ---

func Test_Start_Pull_ReturnsDataAddress(t *testing.T) {
	address := newDataAddress()
	var received *ProcessorOptions
	sdk, store := newTestSDK(t, WithStartProcessor(func(_ context.Context, _ *DataFlow, _ *DataPlaneSDK, options *ProcessorOptions) (*DataFlowStatusMessage, error) {
		received = options
		return &DataFlowStatusMessage{State: Started, DataAddress: address}, nil
	}))
	msg := createStartMessage(pullProfile, nil)

	response, err := sdk.Start(context.Background(), msg)
	require.NoError(t, err)
	assert.Equal(t, Started, response.State)
	assert.Equal(t, msg.DataFlowID, response.DataFlowID)
	assert.NotEmpty(t, response.MessageID)
	assert.Equal(t, address, response.DataAddress)
	assert.False(t, received.Duplicate)
	assert.Nil(t, received.DataAddress)

	flow := store.get(t, msg.DataFlowID)
	assert.False(t, flow.Consumer)
	assert.Equal(t, Started, flow.State)
	assert.Equal(t, address, flow.SourceDataAddress)
}

func Test_Start_Push_ReceivesDataAddress(t *testing.T) {
	address := newDataAddress()
	var received *ProcessorOptions
	sdk, store := newTestSDK(t, WithStartProcessor(func(_ context.Context, _ *DataFlow, _ *DataPlaneSDK, options *ProcessorOptions) (*DataFlowStatusMessage, error) {
		received = options
		return &DataFlowStatusMessage{State: Started, DataAddress: newDataAddress()}, nil
	}))
	msg := createStartMessage(pushProfile, address)

	response, err := sdk.Start(context.Background(), msg)
	require.NoError(t, err)
	assert.Nil(t, response.DataAddress, "a push provider must not return a data address")
	assert.Equal(t, address, received.DataAddress)
	assert.Equal(t, address, store.get(t, msg.DataFlowID).DestinationDataAddress)
}

func Test_Start_DataAddressRules(t *testing.T) {
	tests := map[string]struct {
		profile string
		address *DataAddress
	}{
		"pull with data address":    {pullProfile, newDataAddress()},
		"push without data address": {pushProfile, nil},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sdk, store := newTestSDK(t)
			msg := createStartMessage(tc.profile, tc.address)
			_, err := sdk.Start(context.Background(), msg)
			assert.ErrorIs(t, err, ErrInvalidInput)
			assert.False(t, store.has(msg.DataFlowID))
		})
	}
}

func Test_Start_VerifyProcessorState(t *testing.T) {
	tests := []struct {
		state       DataFlowState
		expectError bool
	}{
		{Preparing, true},
		{Prepared, true},
		{Starting, false},
		{Started, false},
		{Suspended, true},
		{Completed, true},
	}
	for _, test := range tests {
		t.Run(test.state.String(), func(t *testing.T) {
			sdk, store := newTestSDK(t, WithStartProcessor(stateProcessor(test.state)))
			msg := createStartMessage(pullProfile, nil)
			_, err := sdk.Start(context.Background(), msg)
			if test.expectError {
				assert.Error(t, err)
				assert.False(t, store.has(msg.DataFlowID))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.state, store.get(t, msg.DataFlowID).State)
		})
	}
}

func Test_Start_ExistingFlow_Conflict(t *testing.T) {
	for _, state := range []DataFlowState{Prepared, Starting, Started, Suspended, Completed, Terminated} {
		t.Run(state.String(), func(t *testing.T) {
			sdk, store := newTestSDK(t)
			msg := createStartMessage(pullProfile, nil)
			store.put(&DataFlow{ID: msg.DataFlowID, State: state, Consumer: state == Prepared})

			_, err := sdk.Start(context.Background(), msg)
			assert.ErrorIs(t, err, ErrConflict)
			assert.Equal(t, state, store.get(t, msg.DataFlowID).State)
		})
	}
}

func Test_Start_StoreError(t *testing.T) {
	store := NewMockDataplaneStore(t)
	sdk := newMockSDK(store)
	msg := createStartMessage(pullProfile, nil)
	store.EXPECT().FindById(mock.Anything, msg.DataFlowID).Return(nil, errors.New("db down"))

	_, err := sdk.Start(context.Background(), msg)
	assert.ErrorContains(t, err, "db down")
}

// --- Started ---

func Test_Started_Pull(t *testing.T) {
	address := newDataAddress()
	var seen *DataAddress
	sdk, store := newTestSDK(t, WithStartedProcessor(func(_ context.Context, flow *DataFlow) error {
		seen = flow.SourceDataAddress // the received address is on the flow when the handler runs
		return nil
	}))
	store.put(consumerFlow("flow", pullProfile, Prepared))

	response, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{MessageID: "m", DataAddress: address})
	require.NoError(t, err)
	assert.Equal(t, Started, response.State)
	assert.Equal(t, "flow", response.DataFlowID)
	assert.NotEmpty(t, response.MessageID)
	assert.Nil(t, response.DataAddress)
	assert.Equal(t, address, seen)

	flow := store.get(t, "flow")
	assert.Equal(t, Started, flow.State)
	assert.Equal(t, address, flow.SourceDataAddress)
}

func Test_Started_DoesNotInvokeStartProcessor(t *testing.T) {
	sdk, store := newTestSDK(t, WithStartProcessor(func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowStatusMessage, error) {
		t.Fatal("the start processor serves the provider's /start only")
		return nil, nil
	}))
	store.put(consumerFlow("flow", pushProfile, Prepared))
	_, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{MessageID: "m"})
	require.NoError(t, err)
}

func Test_Started_HandlerError(t *testing.T) {
	sdk, store := newTestSDK(t, WithStartedProcessor(func(context.Context, *DataFlow) error {
		return errors.New("boom")
	}))
	store.put(consumerFlow("flow", pullProfile, Prepared))

	_, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{MessageID: "m", DataAddress: newDataAddress()})
	assert.ErrorContains(t, err, "boom")
	flow := store.get(t, "flow")
	assert.Equal(t, Prepared, flow.State)
	assert.Nil(t, flow.SourceDataAddress, "a failed /started leaves the flow unchanged")
}

// Test_Started_KeepsPushConsumerAddress is the regression test for the address a push consumer hands out at
// prepare: /started must not replace it, so a later /resume returns it again.
func Test_Started_KeepsPushConsumerAddress(t *testing.T) {
	prepared := newDataAddress()
	other := &DataAddress{EndpointType: "https://w3id.org/idsa/v4.1/HTTP", Endpoint: "https://other.example.com"}
	sdk, store := newTestSDK(t,
		WithPrepareProcessor(func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowStatusMessage, error) {
			return &DataFlowStatusMessage{State: Prepared, DataAddress: prepared}, nil
		}),
		// a start processor written for the provider side; it must not affect the consumer
		WithStartProcessor(func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowStatusMessage, error) {
			return &DataFlowStatusMessage{State: Started, DataAddress: other}, nil
		}),
	)
	msg := createPrepareMessage(pushProfile)
	response, err := sdk.Prepare(context.Background(), msg)
	require.NoError(t, err)
	assert.Equal(t, prepared, response.DataAddress)

	_, err = sdk.Started(context.Background(), msg.DataFlowID, DataFlowStartedNotificationMessage{MessageID: "m"})
	require.NoError(t, err)
	assert.Equal(t, prepared, store.get(t, msg.DataFlowID).DestinationDataAddress)

	require.NoError(t, sdk.Suspend(context.Background(), msg.DataFlowID, DataFlowSuspendMessage{MessageID: "m"}))
	resumed, err := sdk.Resume(context.Background(), msg.DataFlowID, DataFlowResumeMessage{MessageID: "m"})
	require.NoError(t, err)
	assert.Equal(t, prepared, resumed.DataAddress)
}

func Test_Started_Push(t *testing.T) {
	sdk, store := newTestSDK(t)
	flow := consumerFlow("flow", pushProfile, Prepared)
	flow.DestinationDataAddress = newDataAddress()
	store.put(flow)

	response, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{MessageID: "m"})
	require.NoError(t, err)
	assert.Equal(t, Started, response.State)
	assert.Nil(t, response.DataAddress)
}

// Test_Started_PullWithoutDataAddress: the specification makes the data address of a started message optional, so
// the SDK accepts a pull transfer without one and the handler sees a nil address.
func Test_Started_PullWithoutDataAddress(t *testing.T) {
	invoked := false
	sdk, store := newTestSDK(t, WithStartedProcessor(func(_ context.Context, flow *DataFlow) error {
		invoked = true
		assert.Nil(t, flow.SourceDataAddress)
		return nil
	}))
	store.put(consumerFlow("flow", pullProfile, Prepared))

	response, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{MessageID: "m"})
	require.NoError(t, err)
	assert.Equal(t, Started, response.State)
	assert.True(t, invoked)

	flow := store.get(t, "flow")
	assert.Equal(t, Started, flow.State)
	assert.Nil(t, flow.SourceDataAddress)
}

func Test_Started_PushWithDataAddress(t *testing.T) {
	sdk, store := newTestSDK(t)
	store.put(consumerFlow("flow", pushProfile, Prepared))
	_, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{MessageID: "m", DataAddress: newDataAddress()})
	assert.ErrorIs(t, err, ErrInvalidInput)
	assert.Equal(t, Prepared, store.get(t, "flow").State)
}

func Test_Started_Duplicate(t *testing.T) {
	sdk, store := newTestSDK(t, WithStartedProcessor(func(context.Context, *DataFlow) error {
		t.Fatal("the handler must not be invoked for a duplicate")
		return nil
	}))
	store.put(consumerFlow("flow", pushProfile, Started))

	response, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{MessageID: "m"})
	require.NoError(t, err)
	assert.Equal(t, Started, response.State)
	assert.Equal(t, "flow", response.DataFlowID)
	assert.NotEmpty(t, response.MessageID)
}

func Test_Started_WrongState(t *testing.T) {
	for _, state := range []DataFlowState{Initialized, Preparing, Suspended, Completed, Terminated} {
		t.Run(state.String(), func(t *testing.T) {
			sdk, store := newTestSDK(t)
			store.put(consumerFlow("flow", pushProfile, state))
			_, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{MessageID: "m"})
			assert.ErrorIs(t, err, ErrInvalidTransition)
		})
	}
}

func Test_Started_NotConsumer(t *testing.T) {
	sdk, store := newTestSDK(t)
	store.put(&DataFlow{ID: "flow", Profile: pushProfile, State: Prepared})
	_, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{MessageID: "m"})
	assert.ErrorIs(t, err, ErrInvalidInput)
}

func Test_Started_NotFound(t *testing.T) {
	sdk, _ := newTestSDK(t)
	_, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{MessageID: "m"})
	assert.ErrorIs(t, err, ErrNotFound)
}

func Test_Started_RequiresMessageID(t *testing.T) {
	sdk, _ := newTestSDK(t)
	_, err := sdk.Started(context.Background(), "flow", DataFlowStartedNotificationMessage{})
	assert.ErrorIs(t, err, ErrValidation)
}

// --- Suspend / Resume ---

func Test_Suspend(t *testing.T) {
	var suspended bool
	sdk, store := newTestSDK(t, WithSuspendProcessor(func(context.Context, *DataFlow) error {
		suspended = true
		return nil
	}))
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})

	require.NoError(t, sdk.Suspend(context.Background(), "flow", DataFlowSuspendMessage{MessageID: "m", Reason: "maintenance"}))
	assert.True(t, suspended)
	flow := store.get(t, "flow")
	assert.Equal(t, Suspended, flow.State)
	assert.Equal(t, SuspendedByRemote, flow.SuspendedBy)
	assert.Equal(t, "maintenance", flow.ErrorDetail)
}

func Test_Suspend_AlreadySuspended(t *testing.T) {
	sdk, store := newTestSDK(t, WithSuspendProcessor(func(context.Context, *DataFlow) error {
		t.Fatal("processor must not be invoked for a duplicate")
		return nil
	}))
	store.put(&DataFlow{ID: "flow", State: Suspended, SuspendedBy: SuspendedByRemote})
	assert.NoError(t, sdk.Suspend(context.Background(), "flow", DataFlowSuspendMessage{MessageID: "m"}))
}

func Test_Suspend_WrongState(t *testing.T) {
	sdk, store := newTestSDK(t)
	store.put(&DataFlow{ID: "flow", State: Prepared})
	assert.ErrorIs(t, sdk.Suspend(context.Background(), "flow", DataFlowSuspendMessage{MessageID: "m"}), ErrInvalidTransition)
}

func Test_Suspend_ProcessorError(t *testing.T) {
	sdk, store := newTestSDK(t, WithSuspendProcessor(func(context.Context, *DataFlow) error {
		return errors.New("boom")
	}))
	store.put(&DataFlow{ID: "flow", State: Started})
	assert.ErrorContains(t, sdk.Suspend(context.Background(), "flow", DataFlowSuspendMessage{MessageID: "m"}), "boom")
	assert.Equal(t, Started, store.get(t, "flow").State)
}

func Test_Suspend_NotFound(t *testing.T) {
	sdk, _ := newTestSDK(t)
	assert.ErrorIs(t, sdk.Suspend(context.Background(), "flow", DataFlowSuspendMessage{MessageID: "m"}), ErrNotFound)
}

func Test_Suspend_RequiresMessageID(t *testing.T) {
	sdk, _ := newTestSDK(t)
	assert.ErrorIs(t, sdk.Suspend(context.Background(), "flow", DataFlowSuspendMessage{}), ErrValidation)
}

func Test_Resume_ProviderPull_ReturnsDataAddress(t *testing.T) {
	address := newDataAddress()
	sdk, store := newTestSDK(t)
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Suspended, SuspendedBy: SuspendedByRemote, SourceDataAddress: address})

	response, err := sdk.Resume(context.Background(), "flow", DataFlowResumeMessage{MessageID: "m"})
	require.NoError(t, err)
	assert.Equal(t, Started, response.State)
	assert.Equal(t, "flow", response.DataFlowID)
	assert.Equal(t, address, response.DataAddress)

	flow := store.get(t, "flow")
	assert.Equal(t, Started, flow.State)
	assert.Equal(t, SuspendedByNone, flow.SuspendedBy)
}

func Test_Resume_ProviderPush_ReceivesDataAddress(t *testing.T) {
	address := newDataAddress()
	var received *ProcessorOptions
	sdk, store := newTestSDK(t, WithResumeProcessor(func(_ context.Context, _ *DataFlow, _ *DataPlaneSDK, options *ProcessorOptions) (*DataFlowStatusMessage, error) {
		received = options
		return &DataFlowStatusMessage{State: Started}, nil
	}))
	store.put(&DataFlow{ID: "flow", Profile: pushProfile, State: Suspended, SuspendedBy: SuspendedByRemote})

	response, err := sdk.Resume(context.Background(), "flow", DataFlowResumeMessage{MessageID: "m", DataAddress: address})
	require.NoError(t, err)
	assert.Nil(t, response.DataAddress)
	assert.Equal(t, address, received.DataAddress)
	assert.Equal(t, address, store.get(t, "flow").DestinationDataAddress)
}

func Test_Resume_SuspendedLocally(t *testing.T) {
	sdk, store := newTestSDK(t)
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Suspended, SuspendedBy: SuspendedByLocal})

	_, err := sdk.Resume(context.Background(), "flow", DataFlowResumeMessage{MessageID: "m"})
	assert.ErrorIs(t, err, ErrInvalidTransition)
	assert.Equal(t, Suspended, store.get(t, "flow").State)
}

func Test_Resume_WrongState(t *testing.T) {
	for _, state := range []DataFlowState{Prepared, Started, Completed, Terminated} {
		t.Run(state.String(), func(t *testing.T) {
			sdk, store := newTestSDK(t)
			store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: state})
			_, err := sdk.Resume(context.Background(), "flow", DataFlowResumeMessage{MessageID: "m"})
			assert.ErrorIs(t, err, ErrInvalidTransition)
		})
	}
}

func Test_Resume_ProcessorInvalidState(t *testing.T) {
	sdk, store := newTestSDK(t, WithResumeProcessor(stateProcessor(Suspended)))
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Suspended, SuspendedBy: SuspendedByRemote})
	_, err := sdk.Resume(context.Background(), "flow", DataFlowResumeMessage{MessageID: "m"})
	assert.Error(t, err)
	assert.Equal(t, Suspended, store.get(t, "flow").State)
}

func Test_SuspendThenResume(t *testing.T) {
	sdk, store := newTestSDK(t)
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})
	require.NoError(t, sdk.Suspend(context.Background(), "flow", DataFlowSuspendMessage{MessageID: "m1"}))
	_, err := sdk.Resume(context.Background(), "flow", DataFlowResumeMessage{MessageID: "m2"})
	require.NoError(t, err)
	assert.Equal(t, Started, store.get(t, "flow").State)
}

// --- Terminate ---

func Test_Terminate(t *testing.T) {
	for _, state := range []DataFlowState{Initialized, Preparing, Prepared, Starting, Started, Suspended} {
		t.Run(state.String(), func(t *testing.T) {
			var terminated bool
			sdk, store := newTestSDK(t, WithTerminateProcessor(func(context.Context, *DataFlow) error {
				terminated = true
				return nil
			}))
			store.put(&DataFlow{ID: "flow", State: state})
			require.NoError(t, sdk.Terminate(context.Background(), "flow", DataFlowTerminateMessage{MessageID: "m", Reason: "violation"}))
			assert.True(t, terminated)
			flow := store.get(t, "flow")
			assert.Equal(t, Terminated, flow.State)
			assert.Equal(t, "violation", flow.ErrorDetail)
		})
	}
}

func Test_Terminate_Completed(t *testing.T) {
	sdk, store := newTestSDK(t)
	store.put(&DataFlow{ID: "flow", State: Completed})
	assert.ErrorIs(t, sdk.Terminate(context.Background(), "flow", DataFlowTerminateMessage{MessageID: "m"}), ErrInvalidTransition)
	assert.Equal(t, Completed, store.get(t, "flow").State)
}

func Test_Terminate_AlreadyTerminated(t *testing.T) {
	sdk, store := newTestSDK(t, WithTerminateProcessor(func(context.Context, *DataFlow) error {
		t.Fatal("processor must not be invoked for a duplicate")
		return nil
	}))
	store.put(&DataFlow{ID: "flow", State: Terminated})
	assert.NoError(t, sdk.Terminate(context.Background(), "flow", DataFlowTerminateMessage{MessageID: "m"}))
}

func Test_Terminate_ProcessorError(t *testing.T) {
	sdk, store := newTestSDK(t, WithTerminateProcessor(func(context.Context, *DataFlow) error {
		return errors.New("boom")
	}))
	store.put(&DataFlow{ID: "flow", State: Started})
	assert.ErrorContains(t, sdk.Terminate(context.Background(), "flow", DataFlowTerminateMessage{MessageID: "m"}), "boom")
	assert.Equal(t, Started, store.get(t, "flow").State)
}

func Test_Terminate_NotFound(t *testing.T) {
	sdk, _ := newTestSDK(t)
	assert.ErrorIs(t, sdk.Terminate(context.Background(), "flow", DataFlowTerminateMessage{MessageID: "m"}), ErrNotFound)
}

func Test_Terminate_RequiresMessageID(t *testing.T) {
	sdk, _ := newTestSDK(t)
	assert.ErrorIs(t, sdk.Terminate(context.Background(), "flow", DataFlowTerminateMessage{}), ErrValidation)
}

// --- Status ---

func Test_Status(t *testing.T) {
	sdk, store := newTestSDK(t)
	store.put(&DataFlow{ID: "flow", State: Started})
	flow, err := sdk.Status(context.Background(), "flow")
	require.NoError(t, err)
	assert.Equal(t, Started, flow.State)

	_, err = sdk.Status(context.Background(), "unknown")
	assert.ErrorIs(t, err, ErrNotFound)
}

// --- Complete ---

func Test_DataPlaneSDK_Completed(t *testing.T) {
	var completed bool
	sdk, store := newTestSDK(t, WithCompleteProcessor(func(context.Context, *DataFlow) error {
		completed = true
		return nil
	}))
	store.put(&DataFlow{ID: "flow123", State: Started})

	require.NoError(t, sdk.Complete(context.Background(), "flow123"))
	assert.True(t, completed)
	assert.Equal(t, Completed, store.get(t, "flow123").State)
}

func Test_DataPlaneSDK_Completed_StoreError(t *testing.T) {
	store := NewMockDataplaneStore(t)
	sdk := newMockSDK(store)
	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Started,
	}, nil)
	store.EXPECT().Save(ctx, mock.AnythingOfType("*dsdk.DataFlow")).Return(fmt.Errorf("store error"))

	err := sdk.Complete(ctx, "flow123")

	assert.ErrorContains(t, err, "store error")
}

func Test_DataPlaneSDK_Completed_AlreadyCompleted(t *testing.T) {
	sdk, store := newTestSDK(t, WithCompleteProcessor(func(context.Context, *DataFlow) error {
		t.Fatal("processor must not be invoked for a duplicate")
		return nil
	}))
	store.put(&DataFlow{ID: "flow123", State: Completed})
	assert.NoError(t, sdk.Complete(context.Background(), "flow123"))
}

func Test_DataPlaneSDK_Completed_ProcessorError(t *testing.T) {
	sdk, store := newTestSDK(t, WithCompleteProcessor(func(context.Context, *DataFlow) error {
		return fmt.Errorf("some error")
	}))
	store.put(&DataFlow{ID: "flow123", State: Started})
	assert.ErrorContains(t, sdk.Complete(context.Background(), "flow123"), "some error")
	assert.Equal(t, Started, store.get(t, "flow123").State)
}

func Test_DataPlaneSDK_Completed_WrongState(t *testing.T) {
	sdk, store := newTestSDK(t)
	store.put(&DataFlow{ID: "flow123", State: Terminated})
	assert.ErrorIs(t, sdk.Complete(context.Background(), "flow123"), ErrInvalidTransition)
}

func Test_DataPlaneSDK_Completed_NotFound(t *testing.T) {
	sdk, _ := newTestSDK(t)
	assert.ErrorIs(t, sdk.Complete(context.Background(), "flow123"), ErrNotFound)
}

func Test_DataPlaneSDK_Completed_EmptyID(t *testing.T) {
	sdk, _ := newTestSDK(t)
	assert.ErrorIs(t, sdk.Complete(context.Background(), ""), ErrInvalidInput)
}

// --- helpers ---

func newTestSDK(t *testing.T, options ...DataPlaneSDKOption) (*DataPlaneSDK, *fakeStore) {
	t.Helper()
	store := &fakeStore{flows: map[string]*DataFlow{}}
	base := []DataPlaneSDKOption{WithStore(store), WithTransactionContext(&mockTrxContext{}), WithAuthorization(testAuthorization{})}
	sdk, err := NewDataPlaneSDK(append(base, options...)...)
	require.NoError(t, err)
	return sdk, store
}

func newMockSDK(store DataplaneStore) *DataPlaneSDK {
	sdk, _ := NewDataPlaneSDK(WithStore(store), WithTransactionContext(&mockTrxContext{}))
	return sdk
}

func consumerFlow(id string, profile string, state DataFlowState) *DataFlow {
	return &DataFlow{ID: id, Consumer: true, Profile: profile, State: state}
}

func createPrepareMessage(profile string) DataFlowPrepareMessage {
	base := newBaseMessage()
	base.Profile = profile
	return DataFlowPrepareMessage{DataFlowBaseMessage: base}
}

func createStartMessage(profile string, address *DataAddress) DataFlowStartMessage {
	base := newBaseMessage()
	base.Profile = profile
	base.DataFlowID = uuid.NewString()
	return DataFlowStartMessage{DataFlowBaseMessage: base, DataAddress: address}
}

// fakeStore is a minimal DataplaneStore that stores copies of the flows.
type fakeStore struct {
	mu    sync.Mutex
	flows map[string]*DataFlow
}

func (s *fakeStore) FindById(_ context.Context, id string) (*DataFlow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	flow, ok := s.flows[id]
	if !ok {
		return nil, ErrNotFound
	}
	return flow.Copy(), nil
}

func (s *fakeStore) Create(_ context.Context, flow *DataFlow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.flows[flow.ID]; ok {
		return ErrConflict
	}
	s.flows[flow.ID] = flow.Copy()
	return nil
}

func (s *fakeStore) Save(_ context.Context, flow *DataFlow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.flows[flow.ID]; !ok {
		return ErrNotFound
	}
	s.flows[flow.ID] = flow.Copy()
	return nil
}

func (s *fakeStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.flows, id)
	return nil
}

func (s *fakeStore) put(flow *DataFlow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flows[flow.ID] = flow.Copy()
}

func (s *fakeStore) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.flows[id]
	return ok
}

func (s *fakeStore) get(t *testing.T, id string) *DataFlow {
	t.Helper()
	flow, err := s.FindById(context.Background(), id)
	require.NoError(t, err)
	return flow
}

type mockTrxContext struct {
}

func (c *mockTrxContext) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}
