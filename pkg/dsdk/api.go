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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

const contentType = "Content-Type"
const jsonContentType = "application/json"

// DataPlaneApi implements the data plane endpoints of the Data Plane Signaling API. Its handlers expect the data flow
// id in the "id" path value; use NewHandler to mount them.
//
// The handlers do not authenticate requests: they take the caller id from the request context
// (ContextWithControlPlaneID). NewHandler authenticates every request before calling them.
type DataPlaneApi struct {
	sdk      *DataPlaneSDK
	basePath string
}

func NewDataPlaneApi(sdk *DataPlaneSDK) *DataPlaneApi {
	return &DataPlaneApi{sdk: sdk, basePath: DefaultBasePath}
}

// Prepare handles POST /dataflows/prepare.
func (d *DataPlaneApi) Prepare(w http.ResponseWriter, r *http.Request) {
	var message DataFlowPrepareMessage
	if !d.decode(w, r, &message) {
		return
	}
	if err := message.Validate(); err != nil {
		d.handleError(err, w)
		return
	}

	ctx := r.Context()
	response, err := d.sdk.Prepare(ctx, message)
	if err != nil {
		d.handleError(err, w)
		return
	}

	code := http.StatusOK
	if response.State == Preparing {
		code = http.StatusAccepted
		w.Header().Set("Location", d.statusPath(response.DataFlowID))
	}
	d.writeResponse(w, code, response)
}

// Start handles POST /dataflows/start.
func (d *DataPlaneApi) Start(w http.ResponseWriter, r *http.Request) {
	var message DataFlowStartMessage
	if !d.decode(w, r, &message) {
		return
	}
	if err := message.Validate(); err != nil {
		d.handleError(err, w)
		return
	}

	ctx := r.Context()
	response, err := d.sdk.Start(ctx, message)
	if err != nil {
		d.handleError(err, w)
		return
	}

	code := http.StatusOK
	if response.State == Starting {
		code = http.StatusAccepted
		w.Header().Set("Location", d.statusPath(response.DataFlowID))
	}
	d.writeResponse(w, code, response)
}

// Started handles POST /dataflows/{id}/started. It always answers 200 with a DataFlowStatusMessage on success.
func (d *DataPlaneApi) Started(w http.ResponseWriter, r *http.Request) {
	var message DataFlowStartedNotificationMessage
	if !d.decode(w, r, &message) {
		return
	}
	if err := message.Validate(); err != nil {
		d.handleError(err, w)
		return
	}

	response, err := d.sdk.Started(r.Context(), r.PathValue("id"), message)
	if err != nil {
		d.handleError(err, w)
		return
	}
	d.writeResponse(w, http.StatusOK, response)
}

// Suspend handles POST /dataflows/{id}/suspend.
func (d *DataPlaneApi) Suspend(w http.ResponseWriter, r *http.Request) {
	var message DataFlowSuspendMessage
	if !d.decode(w, r, &message) {
		return
	}
	if err := message.Validate(); err != nil {
		d.handleError(err, w)
		return
	}
	if err := d.sdk.Suspend(r.Context(), r.PathValue("id"), message); err != nil {
		d.handleError(err, w)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Resume handles POST /dataflows/{id}/resume.
func (d *DataPlaneApi) Resume(w http.ResponseWriter, r *http.Request) {
	var message DataFlowResumeMessage
	if !d.decode(w, r, &message) {
		return
	}
	if err := message.Validate(); err != nil {
		d.handleError(err, w)
		return
	}
	response, err := d.sdk.Resume(r.Context(), r.PathValue("id"), message)
	if err != nil {
		d.handleError(err, w)
		return
	}
	d.writeResponse(w, http.StatusOK, response)
}

// Terminate handles POST /dataflows/{id}/terminate.
func (d *DataPlaneApi) Terminate(w http.ResponseWriter, r *http.Request) {
	var message DataFlowTerminateMessage
	if !d.decode(w, r, &message) {
		return
	}
	if err := message.Validate(); err != nil {
		d.handleError(err, w)
		return
	}
	if err := d.sdk.Terminate(r.Context(), r.PathValue("id"), message); err != nil {
		d.handleError(err, w)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Completed handles POST /dataflows/{id}/completed. The request has an empty body.
func (d *DataPlaneApi) Completed(w http.ResponseWriter, r *http.Request) {
	if err := d.sdk.Complete(r.Context(), r.PathValue("id")); err != nil {
		d.handleError(err, w)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Status handles GET /dataflows/{id}/status.
func (d *DataPlaneApi) Status(w http.ResponseWriter, r *http.Request) {
	dataFlow, err := d.sdk.Status(r.Context(), r.PathValue("id"))
	if err != nil {
		d.handleError(err, w)
		return
	}
	response := DataFlowStatusResponseMessage{
		DataFlowID: dataFlow.ID,
		State:      dataFlow.State,
	}
	d.writeResponse(w, http.StatusOK, response)
}

// RegisterControlPlane handles PUT /controlplanes. It answers 200 without a body.
func (d *DataPlaneApi) RegisterControlPlane(w http.ResponseWriter, r *http.Request) {
	var message ControlPlaneRegistrationMessage
	if !d.decode(w, r, &message) {
		return
	}
	if err := d.sdk.RegisterControlPlane(r.Context(), message); err != nil {
		d.handleError(err, w)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// DeleteControlPlane handles DELETE /controlplanes/{controlPlaneId}. It answers 204, or 404 for an unknown id.
func (d *DataPlaneApi) DeleteControlPlane(w http.ResponseWriter, r *http.Request) {
	if err := d.sdk.DeleteControlPlane(r.Context(), r.PathValue("controlPlaneId")); err != nil {
		d.handleError(err, w)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (d *DataPlaneApi) statusPath(id string) string {
	return d.basePath + "/dataflows/" + id + "/status"
}

func (d *DataPlaneApi) decode(w http.ResponseWriter, r *http.Request, target any) bool {
	if err := json.NewDecoder(r.Body).Decode(target); err != nil {
		id := uuid.NewString()
		d.sdk.Monitor.Printf("Error decoding request [%s]: %v\n", id, err)
		d.writeResponse(w, http.StatusBadRequest, &ErrorResponse{Error: fmt.Sprintf("Failed to decode request body [%s]", id)})
		return false
	}
	return true
}

// handleError maps an SDK error to an HTTP status code and writes an ErrorResponse.
func (d *DataPlaneApi) handleError(err error, w http.ResponseWriter) {
	switch {
	case errors.Is(err, ErrValidation), errors.Is(err, ErrInvalidTransition), errors.Is(err, ErrInvalidInput):
		d.writeResponse(w, http.StatusBadRequest, &ErrorResponse{Error: err.Error()})
	case errors.Is(err, ErrUnauthorized):
		d.writeResponse(w, http.StatusUnauthorized, &ErrorResponse{Error: err.Error()})
	case errors.Is(err, ErrForbidden):
		d.writeResponse(w, http.StatusForbidden, &ErrorResponse{Error: err.Error()})
	case errors.Is(err, ErrAuthorizationNotSupported):
		d.writeResponse(w, http.StatusBadRequest, &ErrorResponse{Error: err.Error()})
	case errors.Is(err, ErrNotFound):
		d.writeResponse(w, http.StatusNotFound, &ErrorResponse{Error: err.Error()})
	case errors.Is(err, ErrConflict):
		d.writeResponse(w, http.StatusConflict, &ErrorResponse{Error: err.Error()})
	default:
		message := fmt.Sprintf("Error processing flow: %s", err)
		d.sdk.Monitor.Println(message)
		d.writeResponse(w, http.StatusInternalServerError, &ErrorResponse{Error: message})
	}
}

func (d *DataPlaneApi) writeResponse(w http.ResponseWriter, code int, response any) {
	body, err := json.Marshal(response)
	if err != nil {
		id := uuid.NewString()
		d.sdk.Monitor.Printf("Error encoding response [%s]: %v\n", id, err)
		code = http.StatusInternalServerError
		body, _ = json.Marshal(&ErrorResponse{Error: fmt.Sprintf("Error encoding response [%s]", id)})
	}
	w.Header().Set(contentType, jsonContentType)
	w.WriteHeader(code)
	_, _ = w.Write(append(body, '\n'))
}
