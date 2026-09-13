package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"task_runner/internal/domain"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)



func (s *Handler) CreateTaskHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateTaskRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil || req.URL == "" || req.Priority == ""{
		writeJSON(w, 500, err)
		return
	}
	
	var res CreateTaskResponse
	res.TaskID = uuid.NewString()
	task := domain.Task{ID: res.TaskID, Payload: req.URL}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeoutQueue)
	defer cancel()

	s.serv.SetToCache(res.TaskID, domain.TaskResult{Status: "pending"})

	err = s.serv.PutInQueue(ctx, req.Priority, task)
	if err != nil {
		writeJSON(w, 500, err)
		return
	}

	
	writeJSON(w, 200, res)
}

func (s *Handler) GetTaskFromCache(w http.ResponseWriter, r *http.Request) {
	req := mux.Vars(r)["id"]
	if req == ""{
		writeJSON(w, 500, errors.New("no id written"))
		return
	}

	resDomain, ok := s.serv.GetFromCache(req)
	res := GetTaskResponce{Status: resDomain.Status, Result: resDomain.Result}
	if !ok {
		writeJSON(w, 404, errors.New("TaskID not found"))
		return
	}
	writeJSON(w, 200, res)
}

func (s *Handler) GetMetrics(w http.ResponseWriter, r *http.Request) {
	var resp MetricsResponse
	resp.Processed, resp.Failed, resp.ActiveWorkers = s.serv.GetMetrics()
	writeJSON(w, 200, resp)
}
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    if err := json.NewEncoder(w).Encode(data); err != nil {
        
        log.Printf("Failed to encode JSON: %v", err)
    }
}
