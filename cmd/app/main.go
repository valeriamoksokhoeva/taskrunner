package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"task_runner/internal/api"
	config "task_runner/internal/cfg"
	"task_runner/internal/domain"
)

func main() {
	cfg := config.MustLoad()

	level := slog.LevelInfo
	if cfg.Env == "dev" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	sigCtx, stop := signal.NotifyContext(appCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serv := domain.NewService(sigCtx, domain.ServiceSample{
		Capacity:    cfg.Capacity,
		TickerTime:  cfg.TickerTime,
		ChanCap:     cfg.ChanCap,
		SemCap:      cfg.SemCap,
		NWorkers:    cfg.NWorkers,
		TokenWait:   cfg.TokenWait,
		TaskTimeout: cfg.TaskTimeout,
		SimulateFor: cfg.SimulateFor,
		Overflow:    cfg.Overflow,
		Mode:        cfg.Mode,
		Store:       cfg.Store,
	})
	serv.Run(sigCtx)

	handler := api.NewHandler(serv, cfg.EnqueueTimeout)
	server, errCh := api.StartServer(cfg.Port, api.Router(handler))
	slog.Info("server listening", "port", cfg.Port, "env", cfg.Env)

	select {
	case err := <-errCh:
		slog.Error("server failed to start", "err", err)
	case <-sigCtx.Done():
		slog.Info("signal received, shutting down")
	}

	// Order matters: HTTP first, then the queues, then the workers.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownHTTP)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown", "err", err)
	}

	serv.CloseChannels()

	if err := serv.WaitTimeout(cfg.ShutdownWorkers); err != nil {
		slog.Error("workers did not finish in time, forcing", "err", err)
	}

	appCancel()

	m := serv.Metrics()
	slog.Info("stopped", "processed", m.Processed, "failed", m.Failed,
		"cancelled", m.Cancelled, "avg_ms", m.AverageProcessTime)
}
