package dsdk

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serve sends the request authenticated as testCaller.
func serve(t *testing.T, handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	return serveAs(t, handler, "Test "+testCaller, method, path, body)
}

// serveAs sends the request with the given Authorization header ("" for none).
func serveAs(t *testing.T, handler http.Handler, authorization string, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	switch b := body.(type) {
	case nil:
		reader = strings.NewReader("")
	case string:
		reader = strings.NewReader(b)
	default:
		data, err := json.Marshal(b)
		require.NoError(t, err)
		reader = strings.NewReader(string(data))
	}
	req := httptest.NewRequest(method, path, reader)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

func Test_Router_AllRoutes(t *testing.T) {
	sdk, store := newTestSDK(t)
	handler := NewHandler(sdk)

	// provider pull: start, suspend, resume, complete, status
	start := createStartMessage(pullProfile, nil)
	rr := serve(t, handler, http.MethodPost, "/v1/dataflows/start", start)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	id := start.DataFlowID

	assert.Equal(t, http.StatusOK, serve(t, handler, http.MethodPost, "/v1/dataflows/"+id+"/suspend", DataFlowSuspendMessage{MessageID: "m"}).Code)
	rr = serve(t, handler, http.MethodPost, "/v1/dataflows/"+id+"/resume", DataFlowResumeMessage{MessageID: "m"})
	assert.Equal(t, http.StatusOK, rr.Code)
	var status DataFlowStatusMessage
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &status))
	assert.Equal(t, Started, status.State)

	assert.Equal(t, http.StatusOK, serve(t, handler, http.MethodPost, "/v1/dataflows/"+id+"/completed", nil).Code)
	rr = serve(t, handler, http.MethodGet, "/v1/dataflows/"+id+"/status", nil)
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.JSONEq(t, `{"dataFlowId": "`+id+`", "state": "COMPLETED"}`, rr.Body.String())
	assert.Equal(t, Completed, store.get(t, id).State)

	// terminating a completed flow is rejected
	assert.Equal(t, http.StatusBadRequest, serve(t, handler, http.MethodPost, "/v1/dataflows/"+id+"/terminate", DataFlowTerminateMessage{MessageID: "m"}).Code)

	// consumer push: prepare, started, terminate
	prepare := createPrepareMessage(pushProfile)
	assert.Equal(t, http.StatusOK, serve(t, handler, http.MethodPost, "/v1/dataflows/prepare", prepare).Code)
	rr = serve(t, handler, http.MethodPost, "/v1/dataflows/"+prepare.DataFlowID+"/started", DataFlowStartedNotificationMessage{MessageID: "m"})
	assert.Equal(t, http.StatusOK, rr.Code)
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &status))
	assert.Equal(t, Started, status.State)
	assert.Equal(t, prepare.DataFlowID, status.DataFlowID)
	assert.Equal(t, http.StatusOK, serve(t, handler, http.MethodPost, "/v1/dataflows/"+prepare.DataFlowID+"/terminate", DataFlowTerminateMessage{MessageID: "m"}).Code)
	assert.Equal(t, Terminated, store.get(t, prepare.DataFlowID).State)
}

func Test_Router_AcceptedSetsLocation(t *testing.T) {
	sdk, _ := newTestSDK(t, WithPrepareProcessor(stateProcessor(Preparing)), WithStartProcessor(stateProcessor(Starting)))

	for _, basePath := range []string{"", "/v1", "/api/signaling/v1"} {
		t.Run(basePath, func(t *testing.T) {
			var handler http.Handler
			if basePath == "/v1" {
				handler = NewHandler(sdk)
			} else {
				handler = NewHandler(sdk, WithBasePath(basePath+"/"))
			}

			prepare := createPrepareMessage(pullProfile)
			rr := serve(t, handler, http.MethodPost, basePath+"/dataflows/prepare", prepare)
			assert.Equal(t, http.StatusAccepted, rr.Code)
			assert.Equal(t, basePath+"/dataflows/"+prepare.DataFlowID+"/status", rr.Header().Get("Location"))

			start := createStartMessage(pullProfile, nil)
			rr = serve(t, handler, http.MethodPost, basePath+"/dataflows/start", start)
			assert.Equal(t, http.StatusAccepted, rr.Code)
			assert.Equal(t, basePath+"/dataflows/"+start.DataFlowID+"/status", rr.Header().Get("Location"))

			assert.Equal(t, http.StatusOK, serve(t, handler, http.MethodGet, rr.Header().Get("Location"), nil).Code)
		})
	}
}

func Test_Router_Errors(t *testing.T) {
	sdk, store := newTestSDK(t)
	handler := NewHandler(sdk)
	store.put(&DataFlow{ID: "flow", Profile: pullProfile, State: Started})

	tests := map[string]struct {
		method, path string
		body         any
		code         int
	}{
		"wrong method":              {http.MethodGet, "/v1/dataflows/start", nil, http.StatusMethodNotAllowed},
		"unknown path":              {http.MethodPost, "/dataflows/start", "{}", http.StatusNotFound},
		"malformed json":            {http.MethodPost, "/v1/dataflows/start", "{", http.StatusBadRequest},
		"suspend without body":      {http.MethodPost, "/v1/dataflows/flow/suspend", nil, http.StatusBadRequest},
		"suspend without messageId": {http.MethodPost, "/v1/dataflows/flow/suspend", `{"reason": "x"}`, http.StatusBadRequest},
		"resume without body":       {http.MethodPost, "/v1/dataflows/flow/resume", nil, http.StatusBadRequest},
		"terminate without body":    {http.MethodPost, "/v1/dataflows/flow/terminate", nil, http.StatusBadRequest},
		"started without messageId": {http.MethodPost, "/v1/dataflows/flow/started", `{}`, http.StatusBadRequest},
		"status not found":          {http.MethodGet, "/v1/dataflows/unknown/status", nil, http.StatusNotFound},
		"completed not found":       {http.MethodPost, "/v1/dataflows/unknown/completed", nil, http.StatusNotFound},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rr := serve(t, handler, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.code, rr.Code, rr.Body.String())
			if tc.code == http.StatusBadRequest || (tc.code == http.StatusNotFound && strings.HasPrefix(tc.path, "/v1")) {
				var errorResponse ErrorResponse
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &errorResponse))
				assert.NotEmpty(t, errorResponse.Error)
			}
		})
	}
	assert.Equal(t, Started, store.get(t, "flow").State)

	// a duplicate start is a conflict
	start := createStartMessage(pullProfile, nil)
	assert.Equal(t, http.StatusOK, serve(t, handler, http.MethodPost, "/v1/dataflows/start", start).Code)
	assert.Equal(t, http.StatusConflict, serve(t, handler, http.MethodPost, "/v1/dataflows/start", start).Code)
}
