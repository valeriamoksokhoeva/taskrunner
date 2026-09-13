package main

import (
	"context"
	"log"
	"syscall"

	"os/signal"
	"task_runner/internal/api"
	config "task_runner/internal/cfg"
	"task_runner/internal/domain"
	"time"
)

func main(){
	cfg := config.LoadConfig()
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syg, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	sample := domain.ServiceSample{
		Capacity: cfg.Capacity,
		TikerTime: cfg.TikerTime,
		Chan_cap: cfg.Chan_cap,
		Sem_cap: cfg.Sem_cap,
		N_workers: cfg.NWorkers,
		Timeout: 	cfg.Timeout,
	}
	serv := domain.NewService(syg, sample)
	
	serv.Run(syg)
	handler := api.NewHandler(serv, 5*time.Second)
	r := api.Router(handler)
	server, errCh := api.StartServer(r)
	select {
	case err:=<-errCh:
		log.Printf("error with starting server, %s", err.Error())
	case <-syg.Done():
		log.Printf("shutting down gracefully by signal")
	}
	
	shutdown_ctx, shut_cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer shut_cancel()

	if err := server.Shutdown(shutdown_ctx); err != nil {
		log.Printf("http shutdown, err: %s", err.Error())
	}
	serv.CloseChannels()

	if err := serv.WaitTimeout(15 * time.Second); err != nil {
		log.Printf("forcefully shutting down")
	}
	cancel()
}
