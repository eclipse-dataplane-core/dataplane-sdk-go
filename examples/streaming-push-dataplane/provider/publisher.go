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

package provider

import (
	"context"
	"fmt"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/common"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

// EventPublisherService mocks a service that publishes event streams intended for clients. Event streams are managed by
// the provider data plane.
type EventPublisherService struct {
	publisherStore *common.Store[*storeEntry]
}

func NewEventPublisherService() *EventPublisherService {
	return &EventPublisherService{publisherStore: common.NewStore[*storeEntry]()}
}

func (m *EventPublisherService) Start(id string, endpoint string, channel string, token string) {
	ctx, cancellation := context.WithCancel(context.Background())
	entry := &storeEntry{id: id, channel: channel, endpoint: endpoint, token: token, cancellation: &cancellation}
	m.publisherStore.Create(id, entry)
	go m.startInternal(ctx, entry)
}

func (m *EventPublisherService) Terminate(id string) {
	cancellation, found := m.publisherStore.Find(id)
	if !found {
		return
	}
	m.publisherStore.Delete(id)
	(*cancellation.cancellation)()
}

func (m *EventPublisherService) startInternal(ctx context.Context, entry *storeEntry) {
	nc, err := connect(entry)
	if err != nil {
		log.Printf("[Event Publisher] Failed to connect to NATS: %v", err)
		return
	}
	defer nc.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	i := 0
	for {
		select {
		case <-ctx.Done():
			log.Printf("[Event Publisher] Event publishing cancelled: %v", ctx.Err())
			return
		case <-ticker.C:
			log.Printf("[Event Publisher] Sending event: %d\n", i)
			err := nc.Publish(entry.channel, []byte(fmt.Sprintf(`{"data": "Event %d"}`, i)))
			if err != nil {
				log.Printf("[Event Publisher] Failed to publish event: %v", err)
				return
			}
			i++
		}
	}
}

func connect(entry *storeEntry) (*nats.Conn, error) {
	nc, err := nats.Connect(entry.endpoint,
		nats.Token(entry.token),
	)
	if err != nil {
		return nil, err
	}
	log.Println("[Event Publisher] connected to provider NATS")
	return nc, nil
}

type storeEntry struct {
	id           string
	channel      string
	endpoint     string
	token        string
	cancellation *context.CancelFunc
}
