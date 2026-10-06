package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"task_runner/internal/domain"
	custom_errors "task_runner/internal/errors"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

const maxBodyBytes = 64 << 10

func (h *Handler) CreateTaskHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body is not valid JSON")
		return
	}
	if req.URL == "" {
		writeError(w, http.StatusBadRequest, "missing_url", "field \"url\" is required")
		return
	}
	if req.Priority != domain.PriorityHigh && req.Priority != domain.PriorityLow {
		writeError(w, http.StatusBadRequest, "unknown_priority", "field \"priority\" must be \"high\" or \"low\"")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeoutQueue)
	defer cancel()

	id := uuid.NewString()
	task := domain.Task{ID: id, Payload: req.URL}

	// Written before the enqueue so a worker cannot set the final status first
	// and have it overwritten back to pending.
	h.serv.SetToCache(id, domain.TaskResult{Status: domain.StatusPending})

	if err := h.serv.PutInQueue(ctx, req.Priority, task); err != nil {
		h.serv.DeleteFromCache(id)
		switch {
		case errors.Is(err, custom_errors.ErrUnknownPriority):
			writeError(w, http.StatusBadRequest, "unknown_priority", err.Error())
		case errors.Is(err, custom_errors.ErrFullQueue), errors.Is(err, context.DeadlineExceeded):
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, "queue_full", "service is overloaded, retry later")
		case errors.Is(err, context.Canceled):
			return
		default:
			slog.Error("enqueue failed", "task_id", id, "err", err)
			writeError(w, http.StatusInternalServerError, "internal", "internal error")
		}
		return
	}

	slog.Info("task accepted", "task_id", id, "priority", string(req.Priority))
	writeJSON(w, http.StatusAccepted, CreateTaskResponse{TaskID: id})
}

func (h *Handler) GetTaskFromCache(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing_id", "task id is required")
		return
	}

	res, ok := h.serv.GetFromCache(id)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "task not found")
		return
	}
	writeJSON(w, http.StatusOK, GetTaskResponse{Status: res.Status, Result: res.Result})
}

func (h *Handler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	m := h.serv.Metrics()
	writeJSON(w, http.StatusOK, MetricsResponse{
		Processed:          m.Processed,
		Failed:             m.Failed,
		Cancelled:          m.Cancelled,
		ActiveWorkers:      m.ActiveWorkers,
		AverageProcessTime: m.AverageProcessTime,
	})
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("failed to encode response", "err", err)
	}
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg, Code: code})
}
