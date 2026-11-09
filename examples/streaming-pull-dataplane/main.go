package main

import (
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/streaming"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/streaming-pull-dataplane/launcher"
)

func main() {
	launcher.LaunchServices()

	streaming.TerminateScenario()
}
