package api

import "task_runner/internal/domain"

type CreateTaskRequest struct {
	URL      string          `json:"url"`
	Priority domain.Priority `json:"priority"`
}

type CreateTaskResponse struct {
	TaskID string `json:"task_id"`
}

type GetTaskResponse struct {
	Status string `json:"status"`
	Result string `json:"result"`
}

type MetricsResponse struct {
	Processed          int64 `json:"processed"`
	Failed             int64 `json:"failed"`
	Cancelled          int64 `json:"cancelled"`
	ActiveWorkers      int64 `json:"active_workers"`
	AverageProcessTime int64 `json:"average_process_time_ms"`
}

type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}
