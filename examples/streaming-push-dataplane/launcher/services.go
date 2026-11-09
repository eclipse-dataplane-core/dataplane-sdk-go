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

package launcher

import (
	"log"

	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/natsservices"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/streaming-push-dataplane/consumer"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/streaming-push-dataplane/provider"
)

func LaunchServices() (*provider.ProviderDataPlane, *consumer.ConsumerDataPlane) {
	ns := natsservices.NewNatsServer()
	err := ns.Init()
	if err != nil {
		log.Fatalf("Failed to initialize NATS Server: %v\n", err)
	}

	providerDataplane, err := provider.NewDataPlane(provider.NewEventPublisherService())
	if err != nil {
		log.Fatalf("Failed to launch Provider Data Plane: %v\n", err)
	}
	providerDataplane.Init()

	as := natsservices.NewAuthService()
	err = as.Init()
	if err != nil {
		log.Fatalf("Failed to initialize Auth Service: %v\n", err)
	}

	subscriber := natsservices.NewEventSubscriber()
	consumerDataplane, err := consumer.NewDataPlane(as, ns, natsservices.NatsUrl, subscriber)
	if err != nil {
		log.Fatalf("Failed to launch Consumer Data Plane: %v\n", err)
	}
	consumerDataplane.Init()

	return providerDataplane, consumerDataplane
}
