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

package main

import (
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/streaming"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/streaming-push-dataplane/launcher"
)

func main() {
	launcher.LaunchServices()

	streaming.TerminateScenario()
}
