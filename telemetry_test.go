package telemetry

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pingPayload mirrors the wire format produced by buildPayload, for use in
// test-side unmarshalling. Production code constructs JSON manually to avoid
// an encoding/json dependency.
type pingPayload struct {
	Project string `json:"project"`
	Version string `json:"version"`
	Ts      int64  `json:"ts"`
}

// resetState restores package-level mutable state so tests do not contaminate
// each other. Tests share `disabled`, `endpoint`, and `inflight`.
func resetState(t *testing.T) {
	t.Helper()
	disabled.Store(false)
	t.Cleanup(func() {
		disabled.Store(false)
	})
}

// setEndpoint atomically retargets the ingest URL for the duration of a test
// and restores it on cleanup. Safe even if Send goroutines outlive the test.
func setEndpoint(t *testing.T, url string) {
	t.Helper()
	prev := *endpoint.Load()
	s := url
	endpoint.Store(&s)
	t.Cleanup(func() {
		p := prev
		endpoint.Store(&p)
	})
}

// captureServer spins up a test server that records every received body.
// It restores the package endpoint when the test finishes.
type captureServer struct {
	srv      *httptest.Server
	mu       sync.Mutex
	requests []pingPayload
	hits     int32
}

func newCaptureServer(t *testing.T, status int) *captureServer {
	t.Helper()
	c := &captureServer{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&c.hits, 1)
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		var p pingPayload
		_ = json.Unmarshal(body, &p)
		c.mu.Lock()
		c.requests = append(c.requests, p)
		c.mu.Unlock()
		w.WriteHeader(status)
	}))

	setEndpoint(t, c.srv.URL)
	t.Cleanup(c.srv.Close)
	return c
}

func (c *captureServer) Hits() int {
	return int(atomic.LoadInt32(&c.hits))
}

func (c *captureServer) Last() (pingPayload, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.requests) == 0 {
		return pingPayload{}, false
	}
	return c.requests[len(c.requests)-1], true
}

func TestSend_ValidPayloadReachesServer(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	Send("my-tool", "1.2.3")
	Wait(2 * time.Second)

	if srv.Hits() != 1 {
		t.Fatalf("expected exactly 1 hit, got %d", srv.Hits())
	}
	got, ok := srv.Last()
	if !ok {
		t.Fatal("no recorded request")
	}
	if got.Project != "my-tool" || got.Version != "1.2.3" {
		t.Fatalf("unexpected payload: %+v", got)
	}
	if got.Ts <= 0 {
		t.Fatalf("expected positive ts, got %d", got.Ts)
	}
	now := time.Now().Unix()
	if got.Ts > now+5 || got.Ts < now-300 {
		t.Fatalf("ts %d outside acceptable window around %d", got.Ts, now)
	}
}

func TestSend_InvalidProjectIsDropped(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	bad := []string{
		"",
		"UPPER",
		"with space",
		"under_score",
		"slash/forbidden",
		strings.Repeat("a", 65),
	}
	for _, p := range bad {
		Send(p, "1.0.0")
	}
	Wait(500 * time.Millisecond)

	if srv.Hits() != 0 {
		t.Fatalf("expected 0 hits for invalid projects, got %d", srv.Hits())
	}
}

func TestSend_InvalidVersionIsDropped(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	bad := []string{
		"",
		"has space",
		"semi;colon",
		strings.Repeat("1", 33),
	}
	for _, v := range bad {
		Send("good-name", v)
	}
	Wait(500 * time.Millisecond)

	if srv.Hits() != 0 {
		t.Fatalf("expected 0 hits for invalid versions, got %d", srv.Hits())
	}
}

func TestSend_AcceptsCommonVersionShapes(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	good := []string{"1.2.3", "1.4.0-beta1", "2.0", "v1.0.0", "1.0.0+meta"}
	for _, v := range good {
		Send("tool", v)
	}
	Wait(2 * time.Second)

	if srv.Hits() != len(good) {
		t.Fatalf("expected %d hits, got %d", len(good), srv.Hits())
	}
}

func TestSend_RejectsNonSemverJunk(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	// These match the old permissive regex but the receiver always
	// rejected them with HTTP 400. The library now drops them client-side
	// so callers like GMP's unset `appVersion = "dev"` produce zero traffic
	// rather than a stream of HTTP 400s.
	junk := []string{"dev", "next", "latest", "snapshot", "git-2026-05-22", "alpha", "abc.def"}
	for _, v := range junk {
		Send("tool", v)
	}
	Wait(500 * time.Millisecond)

	if srv.Hits() != 0 {
		t.Fatalf("expected 0 hits for non-semver junk, got %d", srv.Hits())
	}
}

func TestSend_NormalizesLeadingV(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	Send("tool", "v1.2.3")
	Send("tool", "V9.0.0")
	Wait(2 * time.Second)

	if srv.Hits() != 2 {
		t.Fatalf("expected 2 hits, got %d", srv.Hits())
	}

	// Concurrent goroutines may arrive in any order, so compare the SET.
	srv.mu.Lock()
	defer srv.mu.Unlock()
	got := make(map[string]struct{}, len(srv.requests))
	for _, req := range srv.requests {
		if strings.HasPrefix(req.Version, "v") || strings.HasPrefix(req.Version, "V") {
			t.Fatalf("wire version still has leading v: %q", req.Version)
		}
		got[req.Version] = struct{}{}
	}
	for _, want := range []string{"1.2.3", "9.0.0"} {
		if _, ok := got[want]; !ok {
			t.Fatalf("missing normalized %q in wire versions %v", want, got)
		}
	}
}

func TestResolveModuleVersion_FallbackWhenUnknownModule(t *testing.T) {
	// "nonexistent.example/none" will never appear in debug.BuildInfo of the
	// test binary, so resolution must fall through to the fallback parameter.
	got := ResolveModuleVersion("nonexistent.example/none", "0.0.0-fallback")
	if got != "0.0.0-fallback" {
		t.Fatalf("expected fallback, got %q", got)
	}
}

func TestResolveModuleVersion_StripsLeadingVFromBuildInfo(t *testing.T) {
	// During `go test`, the Main module's path is the package under test.
	// Its build info Version is typically "(devel)" — which we treat as
	// unusable and fall back. Verify that branch: pass the current module
	// path; expect the fallback rather than a literal "(devel)" leak.
	got := ResolveModuleVersion("github.com/lukaszraczylo/oss-telemetry", "0.0.0-fallback")
	if got == "(devel)" {
		t.Fatalf("resolved to (devel) — should fall back to caller-supplied value")
	}
	if got == "v0.0.0-fallback" {
		t.Fatalf("leading v not stripped from fallback path: %q", got)
	}
}

func TestSendForModule_FallbackPath(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	// Unknown modulePath → resolver returns the fallback, which validVersion
	// must accept and Send forward.
	SendForModule("tool", "nonexistent.example/none", "1.2.3")
	Wait(2 * time.Second)

	if srv.Hits() != 1 {
		t.Fatalf("expected 1 hit, got %d", srv.Hits())
	}
	got, _ := srv.Last()
	if got.Version != "1.2.3" {
		t.Fatalf("expected wire version 1.2.3, got %q", got.Version)
	}
	if got.Project != "tool" {
		t.Fatalf("expected project tool, got %q", got.Project)
	}
}

func TestDisable_StopsSubsequentSends(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	Disable()
	Send("tool", "1.0.0")
	Wait(500 * time.Millisecond)

	if srv.Hits() != 0 {
		t.Fatalf("expected 0 hits after Disable(), got %d", srv.Hits())
	}
}

func TestEnv_DO_NOT_TRACK_DisablesSends(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	t.Setenv("DO_NOT_TRACK", "1")
	Send("tool", "1.0.0")
	Wait(500 * time.Millisecond)

	if srv.Hits() != 0 {
		t.Fatalf("expected 0 hits with DO_NOT_TRACK=1, got %d", srv.Hits())
	}
}

func TestEnv_OSS_TELEMETRY_DISABLED_DisablesSends(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	t.Setenv("OSS_TELEMETRY_DISABLED", "true")
	Send("tool", "1.0.0")
	Wait(500 * time.Millisecond)

	if srv.Hits() != 0 {
		t.Fatalf("expected 0 hits with OSS_TELEMETRY_DISABLED=true, got %d", srv.Hits())
	}
}

func TestEnv_ProjectSpecific_DisablesSends(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	t.Setenv("MY_TOOL_DISABLE_TELEMETRY", "yes")
	Send("my-tool", "1.0.0")
	Send("other-tool", "1.0.0")
	Wait(2 * time.Second)

	if srv.Hits() != 1 {
		t.Fatalf("expected 1 hit (other-tool only), got %d", srv.Hits())
	}
	got, _ := srv.Last()
	if got.Project != "other-tool" {
		t.Fatalf("expected other-tool to be sent, got %q", got.Project)
	}
}

func TestEnv_FalsyValuesDoNotDisable(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusNoContent)

	t.Setenv("DO_NOT_TRACK", "0")
	t.Setenv("OSS_TELEMETRY_DISABLED", "false")
	Send("tool", "1.0.0")
	Wait(2 * time.Second)

	if srv.Hits() != 1 {
		t.Fatalf("expected 1 hit with falsy env, got %d", srv.Hits())
	}
}

func TestSend_DoesNotBlock(t *testing.T) {
	resetState(t)
	// Server that never responds — verifies caller is not blocked on slow networks.
	hung := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	setEndpoint(t, hung.URL)
	t.Cleanup(hung.Close)

	start := time.Now()
	Send("tool", "1.0.0")
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Fatalf("Send blocked for %v, expected near-instant return", elapsed)
	}
}

func TestSend_SurvivesServerError(t *testing.T) {
	resetState(t)
	srv := newCaptureServer(t, http.StatusInternalServerError)

	// Must not panic, must complete cleanly even though server returns 500.
	Send("tool", "1.0.0")
	Wait(2 * time.Second)

	if srv.Hits() != 1 {
		t.Fatalf("expected the request to still reach server (got %d)", srv.Hits())
	}
}

func TestSend_SurvivesUnreachableEndpoint(t *testing.T) {
	resetState(t)
	// Reserved TEST-NET-1 address per RFC 5737 — guaranteed unroutable.
	setEndpoint(t, "http://192.0.2.1:1/v1/ping")

	// Should not panic and should complete within the timeout budget.
	Send("tool", "1.0.0")
	Wait(3 * time.Second)
}

func TestWait_ReturnsImmediatelyWhenIdle(t *testing.T) {
	resetState(t)
	start := time.Now()
	Wait(5 * time.Second)
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("Wait blocked %v while idle", elapsed)
	}
}

func TestWait_NonPositiveTimeoutReturnsImmediately(t *testing.T) {
	resetState(t)
	start := time.Now()
	Wait(0)
	Wait(-1 * time.Second)
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Fatalf("Wait with non-positive timeout blocked %v", elapsed)
	}
}

func TestValidationHelpers(t *testing.T) {
	resetState(t)

	cases := []struct {
		project string
		want    bool
	}{
		{"a", true},
		{"my-tool-7", true},
		{"123", true},
		{"", false},
		{"Foo", false},
		{"under_score", false},
		{strings.Repeat("a", 64), true},
		{strings.Repeat("a", 65), false},
	}
	for _, c := range cases {
		if got := validProject(c.project); got != c.want {
			t.Errorf("validProject(%q) = %v, want %v", c.project, got, c.want)
		}
	}

	vcases := []struct {
		version string
		want    bool
	}{
		{"1.2.3", true},
		{"1.4.0-beta1", true},
		{"v1.0.0", true},
		{"V1.0.0", true},
		{"1.0.0+meta", true},
		{"1.0.0-rc.1+build.7", true},
		{"1", true},
		{"1.2", true},
		{"", false},
		{"has space", false},
		{"dev", false},
		{"next", false},
		{"v", false},
		{"v.1.2", false},
		{"1.2.3.4", false},
		{"-1.2.3", false},
		{"1.2.3-", false},
		{"1.2.3+", false},
		{strings.Repeat("1", 32), true},
		{strings.Repeat("1", 33), false},
	}
	for _, c := range vcases {
		if got := validVersion(c.version); got != c.want {
			t.Errorf("validVersion(%q) = %v, want %v", c.version, got, c.want)
		}
	}
}

func TestBuildPayload(t *testing.T) {
	resetState(t)

	body := buildPayload("my-tool", "1.2.3", 1747782200)
	want := `{"project":"my-tool","version":"1.2.3","ts":1747782200}`
	if string(body) != want {
		t.Fatalf("buildPayload mismatch:\n got %q\nwant %q", string(body), want)
	}

	var p pingPayload
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("buildPayload produced invalid JSON: %v", err)
	}
	if p.Project != "my-tool" || p.Version != "1.2.3" || p.Ts != 1747782200 {
		t.Fatalf("round-trip mismatch: %+v", p)
	}
}

func TestProjectEnvKey(t *testing.T) {
	resetState(t)

	cases := map[string]string{
		"my-tool":  "MY_TOOL_DISABLE_TELEMETRY",
		"abc":      "ABC_DISABLE_TELEMETRY",
		"a-b-c-d":  "A_B_C_D_DISABLE_TELEMETRY",
		"123-tool": "123_TOOL_DISABLE_TELEMETRY",
	}
	for in, want := range cases {
		if got := projectEnvKey(in); got != want {
			t.Errorf("projectEnvKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSend_DoesNotAllocateWhenDisabled(t *testing.T) {
	resetState(t)
	Disable()

	allocs := testing.AllocsPerRun(1000, func() {
		Send("tool", "1.0.0")
	})
	if allocs != 0 {
		t.Fatalf("Send while disabled should be zero-alloc, got %.1f allocs/op", allocs)
	}
}

func TestTruthy(t *testing.T) {
	resetState(t)

	truthyCases := []string{"1", "true", "TRUE", "Yes", "on", " 1 "}
	for _, s := range truthyCases {
		if !truthy(s) {
			t.Errorf("truthy(%q) = false, want true", s)
		}
	}
	falsyCases := []string{"", "0", "false", "no", "off", "anything else"}
	for _, s := range falsyCases {
		if truthy(s) {
			t.Errorf("truthy(%q) = true, want false", s)
		}
	}
}
