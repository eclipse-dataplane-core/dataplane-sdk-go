//  Copyright (c) 2025 Metaform Systems, Inc
//
//  This program and the accompanying materials are made available under the
//  terms of the Apache License, Version 2.0 which is available at
//  https://www.apache.org/licenses/LICENSE-2.0
//
//  SPDX-License-Identifier: Apache-2.0
//
//  Contributors:
//       Metaform Systems, Inc. - initial API and implementation
//

package dsdk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDataFlow_transitionToPreparing(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"valid transition from Initialized": {
			initialState: Initialized,
			expectErr:    false,
		},
		"idempotent transition from Preparing": {
			initialState: Preparing,
			expectErr:    false,
		},
		"invalid transition from Prepared": {
			initialState: Prepared,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from PREPARED to PREPARING",
		},
		"invalid transition from Starting": {
			initialState: Starting,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from STARTING to PREPARING",
		},
		"invalid transition from Started": {
			initialState: Started,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from STARTED to PREPARING",
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from COMPLETED to PREPARING",
		},
		"invalid transition from Suspended": {
			initialState: Suspended,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from SUSPENDED to PREPARING",
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from TERMINATED to PREPARING",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 5,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.TransitionToPreparing()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error, expected %v, got %v", tc.initialState, df.State)
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error, expected %v, got %v", initialStateCount, df.StateCount)
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Preparing {
					t.Errorf("expected state %v, got %v", Preparing, df.State)
				}
				// For idempotent transitions, state count and timestamp should not change
				if tc.initialState == Preparing {
					if df.StateCount != initialStateCount {
						t.Errorf("state count should not change for idempotent transition, expected %v, got %v", initialStateCount, df.StateCount)
					}
					if df.StateTimestamp != initialTimestamp {
						t.Errorf("state timestamp should not change for idempotent transition")
					}
				} else {
					if df.StateCount != initialStateCount+1 {
						t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
					}
					if df.StateTimestamp <= initialTimestamp {
						t.Errorf("state timestamp should be updated")
					}
				}
			}
		})
	}
}

func TestDataFlow_transitionToPrepared(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"valid transition from Initialized": {
			initialState: Initialized,
			expectErr:    false,
		},
		"valid transition from Preparing": {
			initialState: Preparing,
			expectErr:    false,
		},
		"idempotent transition from Prepared": {
			initialState: Prepared,
			expectErr:    false,
		},
		"invalid transition from Starting": {
			initialState: Starting,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from STARTING to PREPARED",
		},
		"invalid transition from Started": {
			initialState: Started,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from STARTED to PREPARED",
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from COMPLETED to PREPARED",
		},
		"invalid transition from Suspended": {
			initialState: Suspended,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from SUSPENDED to PREPARED",
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from TERMINATED to PREPARED",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 3,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.TransitionToPrepared()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Prepared {
					t.Errorf("expected state %v, got %v", Prepared, df.State)
				}
				// For idempotent transitions, state count and timestamp should not change
				if tc.initialState == Prepared {
					if df.StateCount != initialStateCount {
						t.Errorf("state count should not change for idempotent transition, expected %v, got %v", initialStateCount, df.StateCount)
					}
					if df.StateTimestamp != initialTimestamp {
						t.Errorf("state timestamp should not change for idempotent transition")
					}
				} else {
					if df.StateCount != initialStateCount+1 {
						t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
					}
					if df.StateTimestamp <= initialTimestamp {
						t.Errorf("state timestamp should be updated")
					}
				}
			}
		})
	}
}

func TestDataFlow_transitionToStarting(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"valid transition from Initialized": {
			initialState: Initialized,
			expectErr:    false,
		},
		"invalid transition from Preparing": {
			initialState: Preparing,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from PREPARING to STARTING",
		},
		"valid transition from Prepared": {
			initialState: Prepared,
			expectErr:    false,
		},
		"idempotent transition from Starting": {
			initialState: Starting,
			expectErr:    false,
		},
		"invalid transition from Started": {
			initialState: Started,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from STARTED to STARTING",
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from COMPLETED to STARTING",
		},
		"invalid transition from Suspended": {
			initialState: Suspended,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from SUSPENDED to STARTING",
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from TERMINATED to STARTING",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 2,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.TransitionToStarting()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Starting {
					t.Errorf("expected state %v, got %v", Starting, df.State)
				}
				// For idempotent transitions, state count and timestamp should not change
				if tc.initialState == Starting {
					if df.StateCount != initialStateCount {
						t.Errorf("state count should not change for idempotent transition, expected %v, got %v", initialStateCount, df.StateCount)
					}
					if df.StateTimestamp != initialTimestamp {
						t.Errorf("state timestamp should not change for idempotent transition")
					}
				} else {
					if df.StateCount != initialStateCount+1 {
						t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
					}
					if df.StateTimestamp <= initialTimestamp {
						t.Errorf("state timestamp should be updated")
					}
				}
			}
		})
	}
}

func TestDataFlow_transitionToStarted(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"valid transition from Initialized": {
			initialState: Initialized,
			expectErr:    false,
		},
		"invalid transition from Prepared": {
			initialState: Prepared,
			expectErr:    false,
		},
		"invalid transition from Preparing": {
			initialState: Preparing,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from PREPARING to STARTED",
		},
		"valid transition from Starting": {
			initialState: Starting,
			expectErr:    false,
		},
		"idempotent transition from Started": {
			initialState: Started,
			expectErr:    false,
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from COMPLETED to STARTED",
		},
		"invalid transition from Suspended (use TransitionToResumed)": {
			initialState: Suspended,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from SUSPENDED to STARTED",
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from TERMINATED to STARTED",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 1,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.TransitionToStarted()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Started {
					t.Errorf("expected state %v, got %v", Started, df.State)
				}
				// For idempotent transitions, state count and timestamp should not change
				if tc.initialState == Started {
					if df.StateCount != initialStateCount {
						t.Errorf("state count should not change for idempotent transition, expected %v, got %v", initialStateCount, df.StateCount)
					}
					if df.StateTimestamp != initialTimestamp {
						t.Errorf("state timestamp should not change for idempotent transition")
					}
				} else {
					if df.StateCount != initialStateCount+1 {
						t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
					}
					if df.StateTimestamp <= initialTimestamp {
						t.Errorf("state timestamp should be updated")
					}
				}
			}
		})
	}
}

func TestDataFlow_transitionToSuspended(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"invalid transition from Initialized": {
			initialState: Initialized,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from INITIALIZED to SUSPENDED",
		},
		"invalid transition from Preparing": {
			initialState: Preparing,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from PREPARING to SUSPENDED",
		},
		"invalid transition from Prepared": {
			initialState: Prepared,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from PREPARED to SUSPENDED",
		},
		"invalid transition from Starting": {
			initialState: Starting,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from STARTING to SUSPENDED",
		},
		"valid transition from Started": {
			initialState: Started,
			expectErr:    false,
		},
		"invalid transition from Completed": {
			initialState: Completed,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from COMPLETED to SUSPENDED",
		},
		"idempotent transition from Suspended": {
			initialState: Suspended,
			expectErr:    false,
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from TERMINATED to SUSPENDED",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 4,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.TransitionToSuspended("test-reason", SuspendedByRemote)

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Suspended {
					t.Errorf("expected state %v, got %v", Suspended, df.State)
				}
				// For idempotent transitions, state count and timestamp should not change
				if tc.initialState == Suspended {
					if df.StateCount != initialStateCount {
						t.Errorf("state count should not change for idempotent transition, expected %v, got %v", initialStateCount, df.StateCount)
					}
					if df.StateTimestamp != initialTimestamp {
						t.Errorf("state timestamp should not change for idempotent transition")
					}
				} else {
					if df.StateCount != initialStateCount+1 {
						t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
					}
					if df.StateTimestamp <= initialTimestamp {
						t.Errorf("state timestamp should be updated")
					}
				}
			}
		})
	}
}

func TestDataFlow_transitionToCompleted(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
		expectedErr  string
	}{
		"invalid transition from Initialized": {
			initialState: Initialized,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from INITIALIZED to COMPLETED",
		},
		"invalid transition from Preparing": {
			initialState: Preparing,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from PREPARING to COMPLETED",
		},
		"invalid transition from Prepared": {
			initialState: Prepared,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from PREPARED to COMPLETED",
		},
		"invalid transition from Starting": {
			initialState: Starting,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from STARTING to COMPLETED",
		},
		"valid transition from Started": {
			initialState: Started,
			expectErr:    false,
		},
		"idempotent transition from Completed": {
			initialState: Completed,
			expectErr:    false,
		},
		"invalid transition from Suspended": {
			initialState: Suspended,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from SUSPENDED to COMPLETED",
		},
		"invalid transition from Terminated": {
			initialState: Terminated,
			expectErr:    true,
			expectedErr:  "invalid transition: cannot transition from TERMINATED to COMPLETED",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 7,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.TransitionToCompleted()

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if err.Error() != tc.expectedErr {
					t.Errorf("expected error %q, got %q", tc.expectedErr, err.Error())
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Completed {
					t.Errorf("expected state %v, got %v", Completed, df.State)
				}
				// For idempotent transitions, state count and timestamp should not change
				if tc.initialState == Completed {
					if df.StateCount != initialStateCount {
						t.Errorf("state count should not change for idempotent transition, expected %v, got %v", initialStateCount, df.StateCount)
					}
					if df.StateTimestamp != initialTimestamp {
						t.Errorf("state timestamp should not change for idempotent transition")
					}
				} else {
					if df.StateCount != initialStateCount+1 {
						t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
					}
					if df.StateTimestamp <= initialTimestamp {
						t.Errorf("state timestamp should be updated")
					}
				}
			}
		})
	}
}

func TestDataFlow_transitionToTerminated(t *testing.T) {
	tests := map[string]struct {
		initialState DataFlowState
		expectErr    bool
	}{
		"valid transition from Initialized": {
			initialState: Initialized,
			expectErr:    false,
		},
		"valid transition from Preparing": {
			initialState: Preparing,
			expectErr:    false,
		},
		"valid transition from Prepared": {
			initialState: Prepared,
			expectErr:    false,
		},
		"valid transition from Starting": {
			initialState: Starting,
			expectErr:    false,
		},
		"valid transition from Started": {
			initialState: Started,
			expectErr:    false,
		},
		"invalid transition from Completed (final)": {
			initialState: Completed,
			expectErr:    true,
		},
		"valid transition from Suspended": {
			initialState: Suspended,
			expectErr:    false,
		},
		"idempotent transition from Terminated": {
			initialState: Terminated,
			expectErr:    false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			df := &DataFlow{
				State:      tc.initialState,
				StateCount: 10,
			}
			initialStateCount := df.StateCount
			initialTimestamp := df.StateTimestamp

			err := df.TransitionToTerminated("test-reason")

			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				// Verify state and metadata unchanged on error
				if df.State != tc.initialState {
					t.Errorf("state should not change on error")
				}
				if df.StateCount != initialStateCount {
					t.Errorf("state count should not change on error")
				}
				if df.StateTimestamp != initialTimestamp {
					t.Errorf("state timestamp should not change on error")
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				// Verify successful transition
				if df.State != Terminated {
					t.Errorf("expected state %v, got %v", Terminated, df.State)
				}
				// For idempotent transitions, state count and timestamp should not change
				if tc.initialState == Terminated {
					if df.StateCount != initialStateCount {
						t.Errorf("state count should not change for idempotent transition, expected %v, got %v", initialStateCount, df.StateCount)
					}
					if df.StateTimestamp != initialTimestamp {
						t.Errorf("state timestamp should not change for idempotent transition")
					}
				} else {
					if df.StateCount != initialStateCount+1 {
						t.Errorf("expected state count %v, got %v", initialStateCount+1, df.StateCount)
					}
					if df.StateTimestamp <= initialTimestamp {
						t.Errorf("state timestamp should be updated")
					}
				}
			}
		})
	}
}

// Test to ensure timestamps are properly set and state counts increment correctly
func TestDataFlow_TransitionsTimestampAndCounter(t *testing.T) {
	df := &DataFlow{
		State:      Initialized,
		StateCount: 0,
	}

	// Capture time before transition
	timeBefore := time.Now().UnixMilli()

	// Perform a valid transition
	err := df.TransitionToPreparing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Capture time after transition
	timeAfter := time.Now().UnixMilli()

	// Verify timestamp is within reasonable range
	if df.StateTimestamp < timeBefore || df.StateTimestamp > timeAfter {
		t.Errorf("timestamp %d should be between %d and %d", df.StateTimestamp, timeBefore, timeAfter)
	}

	// Verify state count incremented
	if df.StateCount != 1 {
		t.Errorf("expected state count 1, got %d", df.StateCount)
	}

	// Test multiple transitions to ensure counter keeps incrementing
	err = df.TransitionToPrepared()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if df.StateCount != 2 {
		t.Errorf("expected state count 2, got %d", df.StateCount)
	}
}

// Test case with zero initial timestamp
func TestDataFlow_TransitionFromZeroTimestamp(t *testing.T) {
	df := &DataFlow{
		State:          Initialized,
		StateCount:     0,
		StateTimestamp: 0,
	}

	err := df.TransitionToPreparing()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should update timestamp from 0 to current time
	if df.StateTimestamp <= 0 {
		t.Errorf("expected positive timestamp, got %d", df.StateTimestamp)
	}

	// Should increment counter from 0 to 1
	if df.StateCount != 1 {
		t.Errorf("expected state count 1, got %d", df.StateCount)
	}
}

func TestDataFlow_SuspendAndResume(t *testing.T) {
	df := &DataFlow{ID: "flow", State: Started}

	err := df.TransitionToSuspended("maintenance", SuspendedByNone)
	assert.ErrorIs(t, err, ErrInvalidInput)
	assert.Equal(t, Started, df.State)

	assert.NoError(t, df.TransitionToSuspended("maintenance", SuspendedByLocal))
	assert.Equal(t, Suspended, df.State)
	assert.Equal(t, SuspendedByLocal, df.SuspendedBy)
	assert.Equal(t, "maintenance", df.ErrorDetail)

	// only the suspending party may resume
	err = df.TransitionToResumed(SuspendedByRemote)
	assert.ErrorIs(t, err, ErrInvalidTransition)
	assert.Equal(t, Suspended, df.State)

	assert.NoError(t, df.TransitionToResumed(SuspendedByLocal))
	assert.Equal(t, Started, df.State)
	assert.Equal(t, SuspendedByNone, df.SuspendedBy)
	assert.Empty(t, df.ErrorDetail)

	// resuming a flow that is not suspended fails
	assert.ErrorIs(t, df.TransitionToResumed(SuspendedByLocal), ErrInvalidTransition)
}
