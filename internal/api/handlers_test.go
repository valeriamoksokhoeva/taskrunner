package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"task_runner/internal/domain"

	"github.com/gorilla/mux"
)

func newTestHandler(t *testing.T, sample domain.ServiceSample) (*Handler, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	if sample.Capacity == 0 {
		sample.Capacity = 100
	}
	if sample.TickerTime == 0 {
		sample.TickerTime = time.Millisecond
	}
	if sample.ChanCap == 0 {
		sample.ChanCap = 8
	}
	if sample.SemCap == 0 {
		sample.SemCap = 2
	}
	if sample.TokenWait == 0 {
		sample.TokenWait = 100 * time.Millisecond
	}
	if sample.TaskTimeout == 0 {
		sample.TaskTimeout = time.Second
	}
	if sample.SimulateFor == 0 {
		sample.SimulateFor = 20 * time.Millisecond
	}
	if sample.Overflow == "" {
		sample.Overflow = "reject"
	}
	if sample.Mode == "" {
		sample.Mode = "sleep"
	}

	return NewHandler(domain.NewService(ctx, sample), 2*time.Second), cancel
}

func post(h *Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(body))
	rr := httptest.NewRecorder()
	h.CreateTaskHandler(rr, req)
	return rr
}

func TestCreateTaskValidation(t *testing.T) {
	h, cancel := newTestHandler(t, domain.ServiceSample{NWorkers: 1})
	defer cancel()

	cases := []struct {
		name string
		body string
		want int
		code string
	}{
		{"broken json", `{`, http.StatusBadRequest, "invalid_json"},
		{"no url", `{"priority":"high"}`, http.StatusBadRequest, "missing_url"},
		{"empty priority", `{"url":"http://x"}`, http.StatusBadRequest, "unknown_priority"},
		{"unknown priority", `{"url":"http://x","priority":"medium"}`, http.StatusBadRequest, "unknown_priority"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rr := post(h, c.body)
			if rr.Code != c.want {
				t.Fatalf("got %d, want %d (body: %s)", rr.Code, c.want, rr.Body.String())
			}
			var e ErrorResponse
			if err := json.Unmarshal(rr.Body.Bytes(), &e); err != nil {
				t.Fatalf("body is not an ErrorResponse: %s", rr.Body.String())
			}
			if e.Code != c.code {
				t.Errorf("code=%q, want %q", e.Code, c.code)
			}
			if e.Error == "" {
				t.Error("empty error message")
			}
		})
	}
}

func TestCreateTaskAccepted(t *testing.T) {
	h, cancel := newTestHandler(t, domain.ServiceSample{NWorkers: 1})
	defer cancel()

	rr := post(h, `{"url":"http://example.invalid","priority":"high"}`)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("got %d, want 202 (body: %s)", rr.Code, rr.Body.String())
	}

	var res CreateTaskResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.TaskID == "" {
		t.Fatal("empty task_id")
	}
}

func TestCreateTaskQueueFullReturns503(t *testing.T) {
	h, cancel := newTestHandler(t, domain.ServiceSample{
		NWorkers: 0, ChanCap: 1, Overflow: "reject",
	})
	defer cancel()

	if rr := post(h, `{"url":"http://x","priority":"high"}`); rr.Code != http.StatusAccepted {
		t.Fatalf("first task: got %d", rr.Code)
	}

	rr := post(h, `{"url":"http://x","priority":"high"}`)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503 (body: %s)", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header")
	}

	var e ErrorResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &e)
	if e.Code != "queue_full" {
		t.Errorf("code=%q, want queue_full", e.Code)
	}
}

func TestRejectDoesNotDisturbAcceptedTask(t *testing.T) {
	h, cancel := newTestHandler(t, domain.ServiceSample{
		NWorkers: 0, ChanCap: 1, Overflow: "reject",
	})
	defer cancel()

	first := post(h, `{"url":"http://x","priority":"high"}`)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first task: got %d", first.Code)
	}
	var accepted CreateTaskResponse
	if err := json.Unmarshal(first.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}

	if rr := post(h, `{"url":"http://x","priority":"high"}`); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", rr.Code)
	}

	res, exists := h.serv.GetFromCache(accepted.TaskID)
	if !exists {
		t.Fatal("accepted task disappeared after another was rejected")
	}
	if res.Status != domain.StatusPending {
		t.Errorf("status=%q, want pending", res.Status)
	}
}

func TestGetTaskNotFound(t *testing.T) {
	h, cancel := newTestHandler(t, domain.ServiceSample{NWorkers: 1})
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/tasks/nope", nil)
	req = mux.SetURLVars(req, map[string]string{"id": "nope"})
	rr := httptest.NewRecorder()
	h.GetTaskFromCache(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404", rr.Code)
	}
}

func TestGetTaskJSONShape(t *testing.T) {
	h, cancel := newTestHandler(t, domain.ServiceSample{NWorkers: 1})
	defer cancel()

	h.serv.SetToCache("abc", domain.TaskResult{Status: domain.StatusDone, Result: "r"})

	req := httptest.NewRequest(http.MethodGet, "/tasks/abc", nil)
	req = mux.SetURLVars(req, map[string]string{"id": "abc"})
	rr := httptest.NewRecorder()
	h.GetTaskFromCache(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("got %d", rr.Code)
	}
	var raw map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["status"]; !ok {
		t.Errorf("no \"status\" key, body: %s", rr.Body.String())
	}
	if _, ok := raw["Status"]; ok {
		t.Errorf("response leaks the Go field name \"Status\", body: %s", rr.Body.String())
	}
}

func TestMetricsShape(t *testing.T) {
	h, cancel := newTestHandler(t, domain.ServiceSample{NWorkers: 2})
	defer cancel()

	rr := httptest.NewRecorder()
	h.GetMetrics(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("got %d", rr.Code)
	}
	var m MetricsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
		t.Fatalf("body did not parse: %s", rr.Body.String())
	}
}
