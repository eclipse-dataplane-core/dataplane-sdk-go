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
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/sync-pull-dataplane/consumer"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/sync-pull-dataplane/provider"
)

// LaunchServices starts the provider and consumer data planes. Their servers accept requests when it returns.
func LaunchServices() (*provider.ProviderDataPlane, *consumer.ConsumerDataPlane) {
	providerDataPlane, err := provider.NewDataPlane()
	if err != nil {
		panic(err)
	}
	providerDataPlane.Init()

	consumerDataPlane, err := consumer.NewDataPlane()
	if err != nil {
		panic(err)
	}
	consumerDataPlane.Init()

	return providerDataPlane, consumerDataPlane
}
