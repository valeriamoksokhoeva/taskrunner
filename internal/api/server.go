package api

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"task_runner/internal/domain"

	"github.com/gorilla/mux"
)

type Handler struct {
	serv         *domain.Service
	timeoutQueue time.Duration
}

func NewHandler(serv *domain.Service, enqueueTimeout time.Duration) *Handler {
	return &Handler{serv: serv, timeoutQueue: enqueueTimeout}
}

// StartServer runs the server in a goroutine. The returned channel delivers a
// fatal startup error, so main does not wait for a signal on a dead server.
func StartServer(port int, r *mux.Router) (*http.Server, <-chan error) {
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	return server, errCh
}

func Router(h *Handler) *mux.Router {
	m := mux.NewRouter()
	m.HandleFunc("/tasks", h.CreateTaskHandler).Methods(http.MethodPost)
	m.HandleFunc("/tasks/{id}", h.GetTaskFromCache).Methods(http.MethodGet)
	m.HandleFunc("/metrics", h.GetMetrics).Methods(http.MethodGet)
	m.HandleFunc("/_info", h.HealthCheck).Methods(http.MethodGet)
	return m
}
