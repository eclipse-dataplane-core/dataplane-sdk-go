package main

import (
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/natsservices"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/streaming"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/examples/streaming-pull-dataplane/launcher"
)

func main() {
	provider, consumer := launcher.LaunchServices()

	streaming.TerminateScenario(natsservices.PullProfile, provider.SDK(), consumer.SDK())
}
