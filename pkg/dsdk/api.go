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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"
)

const contentType = "Content-Type"
const jsonContentType = "application/json"

type DataPlaneApi struct {
	sdk *DataPlaneSDK
}

func NewDataPlaneApi(sdk *DataPlaneSDK) *DataPlaneApi {
	return &DataPlaneApi{sdk: sdk}
}

func (d *DataPlaneApi) Prepare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusBadRequest)
		return
	}
	var prepareMessage DataFlowPrepareMessage

	if err := json.NewDecoder(r.Body).Decode(&prepareMessage); err != nil {
		d.decodingError(w, err)
		return
	}

	if err := prepareMessage.Validate(); err != nil {
		d.handleError(err, w)
	}

	response, err := d.sdk.Prepare(r.Context(), prepareMessage)
	if err != nil {
		d.handleError(err, w)
		return
	}

	var code int
	if response.State == Prepared {
		code = http.StatusOK
	} else {
		code = http.StatusAccepted
	}
	d.writeResponse(w, code, response)
}

func (d *DataPlaneApi) Start(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusBadRequest)
		return
	}
	var startMessage DataFlowStartMessage

	if err := json.NewDecoder(r.Body).Decode(&startMessage); err != nil {
		d.decodingError(w, err)
		return
	}

	if err := startMessage.Validate(); err != nil {
		d.handleError(err, w)
		return
	}

	response, err := d.sdk.Start(r.Context(), startMessage)
	if err != nil {
		d.handleError(err, w)
		return
	}

	var code int
	if response.State == Started {
		code = http.StatusOK
	} else {
		code = http.StatusAccepted
		w.Header().Set("Location", "/dataflows/"+startMessage.ProcessID)
	}
	d.writeResponse(w, code, response)

}

func (d *DataPlaneApi) StartById(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusBadRequest)
		return
	}
	var startMessage DataFlowStartedNotificationMessage

	if err := json.NewDecoder(r.Body).Decode(&startMessage); err != nil {
		d.decodingError(w, err)
		return
	}

	if err := startMessage.Validate(); err != nil {
		d.handleError(err, w)
		return
	}

	response, err := d.sdk.StartById(r.Context(), id, startMessage)
	if err != nil {
		d.handleError(err, w)
		return
	}

	var code int
	if response.State == Started {
		code = http.StatusOK
	} else {
		code = http.StatusAccepted
		w.Header().Set("Location", "/dataflows/"+id)
	}
	d.writeResponse(w, code, response)
}

func (d *DataPlaneApi) Terminate(id string, w http.ResponseWriter, r *http.Request) {
	reason := ""
	// Peek into the body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		d.decodingError(w, err)
		return
	}
	// if a body was sent, parse it, read the reason

	if len(bodyBytes) > 0 {
		var terminateMessage DataFlowTransitionMessage

		if err := json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&terminateMessage); err != nil {
			d.decodingError(w, err)
			return
		}
		if err := terminateMessage.Validate(); err != nil {
			d.handleError(err, w)
			return
		}
		reason = terminateMessage.Reason
	}
	terminateError := d.sdk.Terminate(r.Context(), id, reason)
	if terminateError != nil {
		d.handleError(terminateError, w)
		return
	}

	w.Header().Set(contentType, jsonContentType)
	w.WriteHeader(http.StatusOK)
}

func (d *DataPlaneApi) Suspend(id string, w http.ResponseWriter, r *http.Request) {

	reason := ""
	// Peek into the body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		d.decodingError(w, err)
		return
	}
	// if a body was sent, parse it, read the reason
	if len(bodyBytes) > 0 {
		var suspendMessage DataFlowTransitionMessage

		if err := json.NewDecoder(bytes.NewReader(bodyBytes)).Decode(&suspendMessage); err != nil {
			d.decodingError(w, err)
			return
		}
		if err := suspendMessage.Validate(); err != nil {
			d.handleError(err, w)
			return
		}
		reason = suspendMessage.Reason
	}

	suspensionError := d.sdk.Suspend(r.Context(), id, reason)
	if suspensionError != nil {
		d.handleError(suspensionError, w)
		return
	}

	w.Header().Set(contentType, jsonContentType)
	w.WriteHeader(http.StatusOK)

}

func (d *DataPlaneApi) Status(processID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusBadRequest)
		return
	}
	dataFlow, err := d.sdk.Status(r.Context(), processID)
	if err != nil {
		d.handleError(err, w)
		return
	}
	w.Header().Set(contentType, jsonContentType)
	response := DataFlowStatusResponseMessage{
		State:      dataFlow.State,
		DataFlowID: dataFlow.ID,
	}
	d.writeResponse(w, http.StatusOK, response)
}

func (d *DataPlaneApi) Complete(processID string, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusBadRequest)
		return
	}
	err := d.sdk.Complete(r.Context(), processID)
	if err != nil {
		d.handleError(err, w)
		return
	}
	d.writeResponse(w, http.StatusOK, nil)
}

func (d *DataPlaneApi) decodingError(w http.ResponseWriter, err error) {
	id := uuid.NewString()
	d.sdk.Monitor.Printf("Error decoding flow [%s]: %v\n", id, err)
	d.writeResponse(w, http.StatusBadRequest, &DataFlowResponseMessage{Error: fmt.Sprintf("Failed to decode request body [%s]", id)})
}

// handleError writes an error message to the HTTP response that indicates "any other" error, such as 409, 500, etc.
func (d *DataPlaneApi) handleError(err error, w http.ResponseWriter) {

	switch {
	case errors.Is(err, ErrValidation), errors.Is(err, ErrInvalidTransition), errors.Is(err, ErrInvalidInput):
		d.badRequest(err.Error(), w)
	case errors.Is(err, ErrNotFound):
		d.writeResponse(w, http.StatusNotFound, &DataFlowResponseMessage{Error: err.Error()})
	case errors.Is(err, ErrConflict):
		message := fmt.Sprintf("%s", err)
		d.writeResponse(w, http.StatusConflict, &DataFlowResponseMessage{Error: message})
	default:
		message := fmt.Sprintf("Error processing flow: %s", err)
		d.sdk.Monitor.Println(message)
		d.writeResponse(w, http.StatusInternalServerError, &DataFlowResponseMessage{Error: message})
	}
}

func (d *DataPlaneApi) badRequest(errMsg string, w http.ResponseWriter) {
	d.writeResponse(w, http.StatusBadRequest, &DataFlowResponseMessage{Error: errMsg})
}

func (d *DataPlaneApi) writeResponse(w http.ResponseWriter, code int, response any) {
	w.Header().Set(contentType, jsonContentType)
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		id := uuid.NewString()
		message := fmt.Sprintf("Error encoding response [%s]", id)
		d.sdk.Monitor.Println(message)
		d.writeResponse(w, http.StatusInternalServerError, &DataFlowResponseMessage{Error: message})
		return
	}
}
