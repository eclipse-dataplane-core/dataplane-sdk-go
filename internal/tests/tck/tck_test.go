//go:build tck

// Package tck runs the data plane verification tests of the Data Plane Signaling TCK
// (https://github.com/eclipse-dataspacetck/dps-tck) against the SDK.
package tck

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/eclipse-dataplane-core/dataplane-sdk-go/pkg/dsdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	tckImage = "eclipsedataspacetck/dps-tck-runtime:1.3.3"
	// tckCallbackPort is the port the TCK's control plane listens on for the data plane's callbacks.
	tckCallbackPort = "8083"
	// tckConfigFile is where the TCK runtime reads its configuration from.
	tckConfigFile = "/etc/tck/config.properties"
	// tckToken is the Authorization header value the TCK sends with every request.
	tckToken = "tck-token"
	// dockerHost is the name under which the TCK container reaches the data plane under test on the host.
	dockerHost = "host.docker.internal"
	tckTimeout = 5 * time.Minute
)

// the TCK logs "Started: <id>: <name>" and then "SUCCESSFUL: <id>" or "FAILED: <id>" for every test
var (
	startedPattern    = regexp.MustCompile(`Started: (\w+:\d+-\d+)`)
	successfulPattern = regexp.MustCompile(`SUCCESSFUL: (\w+:\d+-\d+)`)
)

// TestDataPlaneSignalingTCK starts a data plane on the host and runs the TCK, which acts as the control plane, in a
// container against it. The TCK reports its results on stdout only, so they are read from the container's log.
func TestDataPlaneSignalingTCK(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), tckTimeout)
	defer cancel()

	// the data plane has to know its control plane before the TCK starts, so the callback port is reserved up front
	callbackPort := freePort(t)
	dataPlane := newDataPlane(t, "http://localhost:"+callbackPort)

	// listen on all interfaces: on Linux the container reaches the host through the Docker bridge
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	server := &http.Server{Handler: logFailedRequests(t, dsdk.NewHandler(dataPlane.sdk))}
	go func() { _ = server.Serve(listener) }()
	defer func() { _ = server.Close() }()

	dataPlaneURL := fmt.Sprintf("http://%s:%d%s", dockerHost, listener.Addr().(*net.TCPAddr).Port, dsdk.DefaultBasePath)

	tck, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        tckImage,
			ExposedPorts: []string{callbackPort + ":" + tckCallbackPort + "/tcp"},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      "dps.tck.properties",
				ContainerFilePath: tckConfigFile,
				FileMode:          0o644,
			}},
			// the runtime reads every DATASPACETCK_* variable as the property dataspacetck.*
			Env: map[string]string{
				"DATASPACETCK_DPS_DATAPLANE_URL":           dataPlaneURL,
				"DATASPACETCK_DPS_DATAPLANE_AUTHORIZATION": tckToken,
			},
			HostConfigModifier: func(hostConfig *container.HostConfig) {
				hostConfig.ExtraHosts = append(hostConfig.ExtraHosts, dockerHost+":host-gateway")
			},
			// the runtime exits once all tests ran, with exit code 0 even if tests failed
			WaitingFor: wait.ForExit().WithExitTimeout(tckTimeout),
		},
		Started: true,
	})
	if tck != nil {
		defer func() { _ = tck.Terminate(context.Background()) }()
	}
	require.NoError(t, err)

	output := containerLog(t, ctx, tck)
	t.Logf("TCK output:\n%s", output)

	dataPlane.wait()
	require.Contains(t, output, "Test run complete")
	started := matches(startedPattern, output)
	require.NotEmpty(t, started, "the TCK did not run any test")
	assert.Empty(t, without(started, matches(successfulPattern, output)), "TCK tests that were not successful")
	assert.Empty(t, dataPlane.callbackErrors(), "failed callbacks to the TCK")
}

// freePort returns a TCP port that is currently unused on the host.
func freePort(t *testing.T) string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	return strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
}

func containerLog(t *testing.T, ctx context.Context, c testcontainers.Container) string {
	reader, err := c.Logs(ctx)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	output, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(output)
}

// matches returns the first capture group of every match of pattern in s.
func matches(pattern *regexp.Regexp, s string) []string {
	var result []string
	for _, match := range pattern.FindAllStringSubmatch(s, -1) {
		result = append(result, match[1])
	}
	return result
}

// without returns the elements of all that are not in excluded.
func without(all []string, excluded []string) []string {
	var result []string
	for _, element := range all {
		if !slices.Contains(excluded, element) {
			result = append(result, element)
		}
	}
	return result
}

// logFailedRequests logs every request of the TCK that the data plane did not answer with a 2xx status.
func logFailedRequests(t *testing.T, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		if recorder.status >= http.StatusMultipleChoices {
			t.Logf("%s %s\n  request:  %s\n  response: %d %s", r.Method, r.URL.Path, body, recorder.status, recorder.body.String())
		}
	})
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}
