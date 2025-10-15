package dsdk

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_DataPlaneSDK_Start(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onStart: func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowResponseMessage, error) {
			return &DataFlowResponseMessage{State: Started}, nil
		},
	}

	ctx := context.Background()
	store.EXPECT().FindById(ctx, "process123").Return(nil, ErrNotFound)
	store.EXPECT().Create(ctx, mock.Anything).Return(nil)

	callbackURL, _ := url.Parse("http://test.com/callback")

	_, _ = dsdk.Start(ctx, DataFlowStartMessage{DataFlowBaseMessage: DataFlowBaseMessage{
		ProcessID:        "process123",
		AgreementID:      "agreement123",
		DatasetID:        "dataset123",
		ParticipantID:    "participant123",
		DataspaceContext: "dscontext",
		CounterPartyID:   "counterparty123",
		CallbackAddress:  CallbackURL(*callbackURL),
		TransferType:     TransferType{DestinationType: "test", FlowType: Pull},
	}})
}

func Test_DataPlaneSDK_Start_VerifySdkCallback(t *testing.T) {
	tests := []struct {
		state       DataFlowState
		expectError bool
	}{
		{
			Preparing,
			true,
		},
		{
			Prepared,
			true,
		},
		{
			Starting,
			false,
		},
		{
			Started,
			false,
		},
		{
			Suspended,
			true,
		},
		{
			Completed,
			true,
		},
	}

	for _, test := range tests {
		store := NewMockDataplaneStore(t)
		dsdk := DataPlaneSDK{
			Store:      store,
			TrxContext: &mockTrxContext{},
			onStart: func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowResponseMessage, error) {
				return &DataFlowResponseMessage{State: test.state}, nil
			},
		}

		ctx := context.Background()

		store.EXPECT().FindById(mock.Anything, mock.AnythingOfType("string")).Return(nil, ErrNotFound)

		// create is not invoked if the SDK hook returns an invalid state
		if !test.expectError {
			store.EXPECT().Create(ctx, mock.MatchedBy(func(df *DataFlow) bool {
				return df.State == test.state
			})).Return(nil)
		}

		_, err := dsdk.Start(ctx, createStartMessage())
		if test.expectError {
			// assert.Nil(t, response)
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func Test_DataPlaneSDK_Start_SdkCallbackInvalidState(t *testing.T) {
	tests := []struct {
		initialState   DataFlowState
		sdkTargetState DataFlowState
		expectError    bool
	}{
		{
			Starting,
			Starting,
			false,
		},
		{
			Started,
			Started,
			false,
		},
		{
			Prepared,
			Started,
			false,
		},
		{
			Prepared,
			Prepared,
			true,
		}}

	for _, test := range tests {
		store := NewMockDataplaneStore(t)
		dsdk := DataPlaneSDK{
			Store:      store,
			TrxContext: &mockTrxContext{},
			onStart: func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowResponseMessage, error) {
				return &DataFlowResponseMessage{State: test.sdkTargetState}, nil
			},
		}

		ctx := context.Background()

		store.EXPECT().FindById(mock.Anything, mock.AnythingOfType("string")).Return(&DataFlow{
			State:    test.initialState,
			Consumer: true, //cover the PREPARED case
		}, nil)

		// create is not invoked if the SDK hook returns an invalid initialState
		if !test.expectError {
			store.EXPECT().Save(ctx, mock.MatchedBy(func(df *DataFlow) bool {
				return df.State == test.sdkTargetState
			})).Return(nil)
		}

		_, err := dsdk.Start(ctx, createStartMessage())
		if test.expectError {
			// assert.Nil(t, response)
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func Test_DataPlaneSDK_Start_AlreadyStarted(t *testing.T) {
	states := []DataFlowState{
		Started,
		Starting,
	}

	for _, state := range states {
		store := NewMockDataplaneStore(t)
		dsdk := DataPlaneSDK{
			Store:      store,
			TrxContext: &mockTrxContext{},
			onStart: func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowResponseMessage, error) {
				return &DataFlowResponseMessage{State: state}, nil
			},
		}

		ctx := context.Background()

		store.EXPECT().FindById(mock.Anything, mock.AnythingOfType("string")).Return(&DataFlow{
			State: state,
		}, nil)
		store.EXPECT().Save(ctx, mock.AnythingOfType("*dsdk.DataFlow")).Return(nil)

		_, err := dsdk.Start(ctx, createStartMessage())
		assert.NoError(t, err)
	}
}

func Test_DataPlaneSDK_Start_ConsumerPrepared(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onStart: func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowResponseMessage, error) {
			return &DataFlowResponseMessage{State: Started}, nil
		},
	}

	ctx := context.Background()
	store.EXPECT().FindById(mock.Anything, mock.AnythingOfType("string")).Return(&DataFlow{
		State:    Prepared,
		Consumer: true,
	}, nil)
	store.EXPECT().Save(ctx, mock.AnythingOfType("*dsdk.DataFlow")).Return(nil)

	_, err := dsdk.Start(ctx, createStartMessage())
	assert.NoError(t, err)
}

func Test_DataPlaneSDK_StartById_Exists(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onStart: func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowResponseMessage, error) {
			return &DataFlowResponseMessage{State: Started}, nil
		},
	}
	ctx := context.Background()
	store.EXPECT().FindById(ctx, "process123").Return(&DataFlow{
		ID:       "process123",
		State:    Prepared,
		Consumer: true,
	}, nil)
	store.EXPECT().Save(ctx, mock.AnythingOfType("*dsdk.DataFlow")).Return(nil)

	r, err := dsdk.StartById(ctx, "process123", createStartByIdMessage())
	assert.NoError(t, err)
	assert.Equal(t, r.State, Started)
}

func Test_DataPlaneSDK_StartById_NotExists(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
	}
	ctx := context.Background()
	store.EXPECT().FindById(ctx, "process123").Return(nil, ErrNotFound)

	_, err := dsdk.StartById(ctx, "process123", createStartByIdMessage())
	assert.ErrorIs(t, err, ErrNotFound)
}

func Test_DataPlaneSDK_StartById_NotConsumer(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
	}
	ctx := context.Background()
	store.EXPECT().FindById(ctx, "process123").Return(&DataFlow{
		ID:       "process123",
		State:    Prepared,
		Consumer: false,
	}, nil)

	r, err := dsdk.StartById(ctx, "process123", createStartByIdMessage())
	assert.ErrorIs(t, err, ErrInvalidInput)
	assert.Nil(t, r)
}

func Test_DataPlaneSDK_StartById_WrongState(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
	}
	ctx := context.Background()
	store.EXPECT().FindById(ctx, "process123").Return(&DataFlow{
		ID:       "process123",
		State:    Uninitialized,
		Consumer: true,
	}, nil)

	r, err := dsdk.StartById(ctx, "process123", createStartByIdMessage())
	assert.ErrorIs(t, err, ErrInvalidTransition)
	assert.Nil(t, r)
}

func Test_DataPlaneSDK_StartById_AlreadyStarted(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onStart: func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowResponseMessage, error) {
			return &DataFlowResponseMessage{State: Started}, nil
		},
	}
	ctx := context.Background()
	store.EXPECT().FindById(ctx, "process123").Return(&DataFlow{
		ID:       "process123",
		State:    Started,
		Consumer: true,
	}, nil)
	store.EXPECT().Save(ctx, mock.AnythingOfType("*dsdk.DataFlow")).Return(nil)

	r, err := dsdk.StartById(ctx, "process123", createStartByIdMessage())
	assert.NoError(t, err)
	assert.Equal(t, r.State, Started)
}

func Test_DataPlaneSDK_Status(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
	}

	ctx := context.Background()
	store.EXPECT().FindById(ctx, "process123").Return(&DataFlow{
		ID:    "process123",
		State: Preparing,
	}, nil)

	flow, err := dsdk.Status(ctx, "process123")
	assert.NoError(t, err)
	assert.Equal(t, flow.State, Preparing)
}

func Test_DataPlaneSDK_Status_WhenNotFound(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
	}

	ctx := context.Background()
	store.EXPECT().FindById(ctx, "process123").Return(nil, ErrNotFound)

	flow, err := dsdk.Status(ctx, "process123")
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Nil(t, flow)
}

func Test_DataPlaneSDK_Prepare(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onPrepare: func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowResponseMessage, error) {
			return &DataFlowResponseMessage{State: Prepared}, nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(mock.Anything, mock.AnythingOfType("string")).Return(nil, ErrNotFound)
	store.EXPECT().Create(ctx, mock.AnythingOfType("*dsdk.DataFlow")).Return(nil)

	response, err := dsdk.Prepare(ctx, createPrepareMessage())
	assert.NoError(t, err)
	assert.NotNil(t, response)
}

func Test_DataPlaneSDK_Prepare_VerifySdkCallback(t *testing.T) {
	tests := []struct {
		state       DataFlowState
		expectError bool
	}{
		{
			Preparing,
			false,
		},
		{
			Prepared,
			false,
		},
		{
			Starting,
			true,
		},
		{
			Started,
			true,
		},
		{
			Completed,
			true,
		},
	}
	for _, test := range tests {
		store := NewMockDataplaneStore(t)
		dsdk := DataPlaneSDK{
			Store:      store,
			TrxContext: &mockTrxContext{},
			onPrepare: func(context.Context, *DataFlow, *DataPlaneSDK, *ProcessorOptions) (*DataFlowResponseMessage, error) {
				return &DataFlowResponseMessage{State: test.state}, nil
			},
		}

		ctx := context.Background()

		store.EXPECT().FindById(mock.Anything, mock.AnythingOfType("string")).Return(nil, ErrNotFound)

		// create is not invoked if the SDK hook returns an invalid initialState
		if !test.expectError {
			store.EXPECT().Create(ctx, mock.MatchedBy(func(df *DataFlow) bool {
				return df.State == test.state
			})).Return(nil)
		}

		_, err := dsdk.Prepare(ctx, createPrepareMessage())
		if test.expectError {
			// assert.Nil(t, response)
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func Test_DataPlaneSDK_Prepare_AlreadyPreparing(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onPrepare: func(ctx context.Context, flow *DataFlow, sdk *DataPlaneSDK, opts *ProcessorOptions) (*DataFlowResponseMessage, error) {
			assert.Equal(t, opts.Duplicate, true)
			return &DataFlowResponseMessage{State: Prepared}, nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(mock.Anything, mock.AnythingOfType("string")).Return(&DataFlow{
		ID:    "process123",
		State: Preparing,
	}, nil)
	store.EXPECT().Save(ctx, mock.AnythingOfType("*dsdk.DataFlow")).Return(nil)

	response, err := dsdk.Prepare(ctx, createPrepareMessage())
	assert.NoError(t, err)
	assert.NotNil(t, response)
}

func Test_DataPlaneSDK_Prepare_InWrongState(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
	}

	ctx := context.Background()

	store.EXPECT().FindById(mock.Anything, mock.AnythingOfType("string")).Return(&DataFlow{
		ID:    "process123",
		State: Started,
	}, nil)
	store.AssertNotCalled(t, "Create", mock.AnythingOfType("*dsdk.DataFlow"))

	_, err := dsdk.Prepare(ctx, createPrepareMessage())
	assert.ErrorIs(t, err, ErrConflict)
	assert.ErrorContains(t, err, "is not in PREPARING or PREPARED state")
}

func Test_DataPlaneSDK_Prepare_SdkCallbackReturnsError(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onPrepare: func(ctx context.Context, flow *DataFlow, sdk *DataPlaneSDK, opts *ProcessorOptions) (*DataFlowResponseMessage, error) {
			return nil, errors.New("some error")
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(mock.Anything, mock.AnythingOfType("string")).Return(nil, ErrNotFound)
	store.AssertNotCalled(t, "Create", mock.AnythingOfType("*dsdk.DataFlow"))

	_, err := dsdk.Prepare(ctx, createPrepareMessage())
	assert.ErrorContains(t, err, "processing data flow process123: some error")
}

func Test_DataPlaneSDK_Terminate(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onTerminate: func(ctx context.Context, flow *DataFlow) error {
			return nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Started,
	}, nil)

	store.EXPECT().Save(ctx, mock.MatchedBy(func(df *DataFlow) bool {
		return df.State == Terminated
	})).Return(nil)

	err := dsdk.Terminate(ctx, "flow123", "")

	assert.NoError(t, err)
}

func Test_DataPlaneSDK_Terminate_NotFound(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onTerminate: func(ctx context.Context, flow *DataFlow) error {
			return nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(nil, ErrNotFound)
	err := dsdk.Terminate(ctx, "flow123", "")

	assert.ErrorContains(t, err, "not found")
}

func Test_DataPlaneSDK_Terminate_AlreadyTerminated(t *testing.T) {

	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onTerminate: func(ctx context.Context, flow *DataFlow) error {
			return nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Terminated, // already terminated
	}, nil)

	// no transition and no save call expected

	err := dsdk.Terminate(ctx, "flow123", "")

	assert.NoError(t, err)
}

func Test_DataPlaneSDK_Terminate_SdkCallbackError(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onTerminate: func(ctx context.Context, flow *DataFlow) error {
			return fmt.Errorf("some error")
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Started,
	}, nil)

	err := dsdk.Terminate(ctx, "flow123", "")

	assert.ErrorContains(t, err, "some error")
}

func Test_DataPlaneSDK_Suspend(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onSuspend: func(ctx context.Context, flow *DataFlow) error {
			return nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Started,
	}, nil)

	store.EXPECT().Save(ctx, mock.MatchedBy(func(df *DataFlow) bool {
		return df.State == Suspended
	})).Return(nil)

	err := dsdk.Suspend(ctx, "flow123", "")

	assert.NoError(t, err)
}

func Test_DataPlaneSDK_Suspend_NotFound(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onSuspend: func(ctx context.Context, flow *DataFlow) error {
			return nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(nil, ErrNotFound)
	err := dsdk.Suspend(ctx, "flow123", "")

	assert.ErrorContains(t, err, "not found")
}

func Test_DataPlaneSDK_Suspend_AlreadySuspended(t *testing.T) {

	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onTerminate: func(ctx context.Context, flow *DataFlow) error {
			return nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Suspended, // already suspended
	}, nil)

	// no transition and no save call expected

	err := dsdk.Suspend(ctx, "flow123", "")

	assert.NoError(t, err)
}

func Test_DataPlaneSDK_Suspend_SdkCallbackError(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onSuspend: func(ctx context.Context, flow *DataFlow) error {
			return fmt.Errorf("some error")
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Started,
	}, nil)

	err := dsdk.Suspend(ctx, "flow123", "")

	assert.ErrorContains(t, err, "some error")
}

func Test_DataPlaneSDK_Completed(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onComplete: func(ctx context.Context, flow *DataFlow) error {
			return nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Started, // already suspended
	}, nil)
	store.EXPECT().Save(ctx, mock.MatchedBy(func(df *DataFlow) bool {
		return df.State == Completed
	})).Return(nil)

	err := dsdk.Complete(ctx, "flow123")

	assert.NoError(t, err)
}

func Test_DataPlaneSDK_Completed_AlreadyCompleted(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Completed, // already suspended
	}, nil)

	err := dsdk.Complete(ctx, "flow123")

	assert.NoError(t, err)
}

func Test_DataPlaneSDK_Completed_SdkError(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onComplete: func(ctx context.Context, flow *DataFlow) error {
			return fmt.Errorf("some error")
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Started, // already suspended
	}, nil)

	err := dsdk.Complete(ctx, "flow123")

	assert.ErrorContains(t, err, "some error")
}

func Test_DataPlaneSDK_Completed_WrongState(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onComplete: func(ctx context.Context, flow *DataFlow) error {
			return nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(&DataFlow{
		ID:    "flow123",
		State: Terminated,
	}, nil)

	err := dsdk.Complete(ctx, "flow123")

	assert.ErrorIs(t, err, ErrInvalidTransition)
}

func Test_DataPlaneSDK_NotFound(t *testing.T) {
	store := NewMockDataplaneStore(t)
	dsdk := DataPlaneSDK{
		Store:      store,
		TrxContext: &mockTrxContext{},
		onComplete: func(ctx context.Context, flow *DataFlow) error {
			return nil
		},
	}

	ctx := context.Background()

	store.EXPECT().FindById(ctx, "flow123").Return(nil, ErrNotFound)

	err := dsdk.Complete(ctx, "flow123")

	assert.ErrorIs(t, err, ErrNotFound)
}

func createPrepareMessage() DataFlowPrepareMessage {
	return DataFlowPrepareMessage{DataFlowBaseMessage: createBaseMessage()}
}
func createStartMessage() DataFlowStartMessage {
	return DataFlowStartMessage{
		DataFlowBaseMessage: createBaseMessage(),
	}
}
func createBaseMessage() DataFlowBaseMessage {
	return DataFlowBaseMessage{
		MessageID:        uuid.New().String(),
		ParticipantID:    uuid.New().String(),
		CounterPartyID:   uuid.New().String(),
		DataspaceContext: uuid.New().String(),
		ProcessID:        "process123",
		AgreementID:      uuid.New().String(),
		DatasetID:        uuid.New().String(),
		CallbackAddress:  CallbackURL{Scheme: "http", Host: "test.com", Path: "/callback"},
		TransferType: TransferType{
			DestinationType: "test-type",
			FlowType:        Pull,
		},
		DataAddress: &DataAddress{},
	}
}
func createStartByIdMessage() DataFlowStartedNotificationMessage {
	return DataFlowStartedNotificationMessage{
		DataAddress: &DataAddress{},
	}
}

type mockTrxContext struct {
}

func (c *mockTrxContext) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}
