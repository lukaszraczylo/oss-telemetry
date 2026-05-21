// Package telemetry sends anonymous usage pings for open-source Go projects.
//
// Wire format (POST application/json):
//
//	{"project":"<name>","version":"<ver>","ts":<unix-seconds>}
//
// Design contract (failproof):
//   - never blocks the caller (work happens in a goroutine)
//   - never panics (background goroutine recovers internally)
//   - never returns errors (silently no-ops on bad input or network failure)
//   - never retries, never deduplicates, never persists state — the client
//     fires a single ping and forgets; the server is responsible for
//     deduplication, abuse protection, and aggregation
//
// Typical usage at program startup:
//
//	telemetry.Send("my-tool", "1.2.3")
//
// For short-lived CLI processes that may exit before the goroutine finishes:
//
//	telemetry.Send("my-tool", "1.2.3")
//	defer telemetry.Wait(2 * time.Second)
//
// Disablement (any one of these suppresses pings):
//   - environment variable DO_NOT_TRACK=1
//   - environment variable OSS_TELEMETRY_DISABLED=1
//   - environment variable <UPPER_PROJECT>_DISABLE_TELEMETRY=1
//     (project name uppercased, dashes replaced with underscores)
//   - calling telemetry.Disable() at runtime
package telemetry

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultEndpoint = "https://oss.raczylo.com/v1/ping"
	httpTimeout     = 2 * time.Second
	maxProjectLen   = 64
	maxVersionLen   = 32
)

// endpoint holds the ingest URL. Production code never mutates it; it is
// atomic only so the package's own test suite can safely retarget it at
// httptest servers while goroutines started by Send are still in flight.
var endpoint atomic.Pointer[string]

var (
	disabled atomic.Bool
	inflight sync.WaitGroup

	client = &http.Client{Timeout: httpTimeout}
)

func init() {
	s := defaultEndpoint
	endpoint.Store(&s)
}

// Send fires a single anonymous telemetry ping in the background and returns
// immediately. It never blocks, never panics, and never reports errors.
// Invalid inputs, disabled state, and network failures are silently dropped.
//
// Call once at program startup. Calling repeatedly will send repeated pings;
// the server is responsible for deduplication.
func Send(project, version string) {
	if disabled.Load() {
		return
	}
	if isDisabledByEnv(project) {
		return
	}
	if !validProject(project) || !validVersion(version) {
		return
	}

	inflight.Add(1)
	go func() {
		defer inflight.Done()
		defer func() { _ = recover() }()
		dispatch(project, version)
	}()
}

// Disable suppresses all subsequent Send calls in this process.
// Idempotent and safe to call from any goroutine.
func Disable() {
	disabled.Store(true)
}

// Wait blocks until all in-flight pings have completed, or until timeout
// elapses — whichever comes first. Useful for short-lived CLI processes
// that may otherwise exit before the background goroutine finishes its POST.
//
// A non-positive timeout returns immediately.
func Wait(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	done := make(chan struct{})
	go func() {
		inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

func dispatch(project, version string) {
	body := buildPayload(project, version, time.Now().Unix())

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *endpoint.Load(), bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

// buildPayload writes the JSON body without encoding/json. The validators
// restrict project and version to characters that never require JSON
// escaping, so direct concatenation is safe.
func buildPayload(project, version string, ts int64) []byte {
	// Wrapper text plus 20 chars for a signed int64.
	const overhead = len(`{"project":"","version":"","ts":}`) + 20
	buf := make([]byte, 0, len(project)+len(version)+overhead)
	buf = append(buf, `{"project":"`...)
	buf = append(buf, project...)
	buf = append(buf, `","version":"`...)
	buf = append(buf, version...)
	buf = append(buf, `","ts":`...)
	buf = strconv.AppendInt(buf, ts, 10)
	buf = append(buf, '}')
	return buf
}

func validProject(p string) bool {
	n := len(p)
	if n == 0 || n > maxProjectLen {
		return false
	}
	for i := range n {
		c := p[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '-':
		default:
			return false
		}
	}
	return true
}

func validVersion(v string) bool {
	n := len(v)
	if n == 0 || n > maxVersionLen {
		return false
	}
	for i := range n {
		c := v[i]
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '.', c == '+', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

func isDisabledByEnv(project string) bool {
	if truthy(os.Getenv("DO_NOT_TRACK")) {
		return true
	}
	if truthy(os.Getenv("OSS_TELEMETRY_DISABLED")) {
		return true
	}
	if project == "" {
		return false
	}
	key := projectEnvKey(project)
	return truthy(os.Getenv(key))
}

// projectEnvKey returns "<UPPER_PROJECT>_DISABLE_TELEMETRY" using a single
// allocation rather than chained strings.ToUpper(strings.ReplaceAll(...)).
func projectEnvKey(project string) string {
	const suffix = "_DISABLE_TELEMETRY"
	buf := make([]byte, 0, len(project)+len(suffix))
	for i := range len(project) {
		c := project[i]
		switch {
		case c == '-':
			c = '_'
		case c >= 'a' && c <= 'z':
			c -= 'a' - 'A'
		}
		buf = append(buf, c)
	}
	buf = append(buf, suffix...)
	return string(buf)
}

func truthy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
