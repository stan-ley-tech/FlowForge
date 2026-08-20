// Command flowforged runs the FlowForge engine: the HTTP API and the
// background scheduler that drives lease reclaim, timeout enforcement,
// and queued-run admission.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/stan-ley-tech/flowforge/engine/internal/api"
	"github.com/stan-ley-tech/flowforge/engine/internal/engine"
	"github.com/stan-ley-tech/flowforge/engine/internal/store"
)

func main() {
	dbPath := envOr("FLOWFORGE_DB_PATH", "flowforge.db")
	addr := envOr("FLOWFORGE_ADDR", ":8080")
	leaseSeconds := envIntOr("FLOWFORGE_LEASE_SECONDS", 30)
	schedulerIntervalMs := envIntOr("FLOWFORGE_SCHEDULER_INTERVAL_MS", 1000)

	s, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("open store at %s: %v", dbPath, err)
	}
	defer s.Close()

	e := engine.New(s, engine.WithLeaseDuration(time.Duration(leaseSeconds)*time.Second))
	sched := engine.NewScheduler(e, s, time.Duration(schedulerIntervalMs)*time.Millisecond)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go sched.Run(ctx)

	server := &http.Server{
		Addr:    addr,
		Handler: api.NewServer(e),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
	}()

	log.Printf("flowforged listening on %s (db=%s, lease=%ds)", addr, dbPath, leaseSeconds)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server: %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
