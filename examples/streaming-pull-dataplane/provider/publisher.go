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
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/natsservices"
	"github.com/nats-io/nats.go"
	"log"
	"time"
)

// EventPublisherService mocks a service that publishes event streams intended for clients. Event streams are managed by
// the provider data plane.
type EventPublisherService struct {
	publisherStore *common.Store[*context.CancelFunc]
}

func NewEventPublisherService() *EventPublisherService {
	return &EventPublisherService{publisherStore: common.NewStore[*context.CancelFunc]()}
}

func (m *EventPublisherService) Start(channel string) {
	ctx, cancellation := context.WithCancel(context.Background())
	m.publisherStore.Create(channel, &cancellation)
	go m.startInternal(ctx, channel)
}

func (m *EventPublisherService) Terminate(channel string) {
	cancellation, found := m.publisherStore.Find(channel)
	if !found {
		return
	}
	m.publisherStore.Delete(channel)
	(*cancellation)()
}

func (m *EventPublisherService) startInternal(ctx context.Context, channel string) {
	defer ctx.Done()
	nc, err := connect()
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
			err := nc.Publish(channel, []byte(fmt.Sprintf(`{"data": "Event %d"}`, i)))
			if err != nil {
				log.Printf("[Event Publisher] Failed to publish event: %v", err)
				return
			}
			i++
		}
	}
}

func connect() (*nats.Conn, error) {
	nc, err := nats.Connect(natsservices.NatsUrl,
		nats.UserInfo("provider", "provider"),
	)

	if err != nil {
		return nil, err
	}
	log.Println("[Event Publisher] connected to provider NATS")
	return nc, nil
}
