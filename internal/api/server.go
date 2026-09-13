package api

import (
	"net/http"
	"task_runner/internal/domain"

	"time"
	"log"
	"github.com/gorilla/mux"
)

type Handler struct {
	serv *domain.Service
	timeoutQueue time.Duration
}

func NewHandler(serv *domain.Service, t time.Duration) *Handler {
	 return &Handler{serv: serv, timeoutQueue: t}
}

func StartServer(r *mux.Router) (*http.Server, <-chan error) {
	server := &http.Server{
		Addr: ":8080",
		Handler: r,

	}
	errCh := make(chan error, 1)
	go func() {
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Printf("Server failed: %v", err)
			errCh <- err
        }
    }()

	return server, errCh
}
func Router(s *Handler) *mux.Router  {
    m := mux.NewRouter()

	m.HandleFunc("/tasks", s.CreateTaskHandler).Methods("POST")
	m.HandleFunc("/tasks/{id}", s.GetTaskFromCache).Methods("GET")
	m.HandleFunc("/metrics", s.GetMetrics).Methods("GET")
	m.HandleFunc("/_info", s.HealthCheck).Methods("GET")
	return m
}



// @Summary      Health check
// @Tags         System
// @Success      200
// @Router       /_info [get]
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}