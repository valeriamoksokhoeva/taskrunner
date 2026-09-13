package api

import "task_runner/internal/domain"

type CreateTaskRequest struct {
	URL string `json:"url"`
	Priority domain.Priority `json:"priority"`
}

type CreateTaskResponse struct {
	TaskID string `json:"task_id"`
}

type GetTaskRequest struct {
	TaskID string `json:"task_id"`
}

type GetTaskResponce struct {
	Status string `json:"status"`
	Result string `json:"result"`
}
type MetricsResponse struct {
	Processed int64 `json:"processed"`
	Failed int64 `json:"failed"`
	ActiveWorkers int64 `json:"active_workers"`
}