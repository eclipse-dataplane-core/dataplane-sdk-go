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

package natsservices

import (
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/nats-io/nats.go"
	"log"
)

// EventSubscriber mocks a service that subscribes to event streams published by the provider.
type EventSubscriber struct {
	connectionStore *common.Store[*nats.Conn]
}

func NewEventSubscriber() *EventSubscriber {
	return &EventSubscriber{connectionStore: common.NewStore[*nats.Conn]()}
}

func (d *EventSubscriber) Subscribe(ID string, endpoint string, channel string, token string) error {
	nc, err := nats.Connect(endpoint,
		nats.Token(token),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Println("[Consumer Data Plane] Client disconnected from NATS")
			d.CloseConnection(ID)
		}),
	)
	if err != nil {
		return err
	}
	d.connectionStore.Create(ID, nc)
	_, err = nc.Subscribe(channel, func(msg *nats.Msg) { // FIXME close sub and ID
		log.Println("[Event Subscriber] Received event: " + string(msg.Data))
	})
	if err != nil {
		return err
	}
	log.Println("[Consumer Data Plane] Client connected to provider NATS")
	return nil
}

func (d *EventSubscriber) CloseConnection(ID string) {
	conn, exists := d.connectionStore.Find(ID)
	if exists {
		conn.Close()
		d.connectionStore.Delete(ID)
	}
}
