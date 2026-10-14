// Command river-service runs Ogen's background jobs. It owns the River queue
// (in its own schema of ogen's Postgres cluster), takes jobs from ogen's outbox
// relay over jobs.v1.JobService, and works the queues listed in RIVER_QUEUES.
// It is internal-only: ogen reaches it over Railway's private network.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	jobsv1 "github.com/ogen-app/river-service/gen/jobs/v1"
	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/grpcauth"
	healthcheck "github.com/ogen-app/river-service/internal/health"
	"github.com/ogen-app/river-service/internal/jobs"
	"github.com/ogen-app/river-service/internal/jobservice"
	"github.com/ogen-app/river-service/internal/logging"
	"github.com/ogen-app/river-service/internal/queue"
	"github.com/ogen-app/river-service/internal/telemetry"
)

const (
	// grpcStopGrace bounds the wait for in-flight Enqueue calls on shutdown.
	grpcStopGrace = 10 * time.Second
	// maxConnectionAge makes clients reconnect periodically, so a scale-up
	// spreads load across the new replicas.
	maxConnectionAge = 5 * time.Minute
	// telemetryFlush bounds the final span and error flush.
	telemetryFlush = 5 * time.Second
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		// The logger is built from config, so this predates it.
		log.Fatalf("river-service: config: %v", err)
	}
	logger := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err := run(cfg, logger); err != nil {
		logger.Error("river-service exited", logging.AttrComponent, "boot", logging.AttrError, err)
		os.Exit(1)
	}
}

func run(cfg *config.Config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTelemetry, err := telemetry.Init(ctx, cfg)
	if err != nil {
		logger.Warn("telemetry init failed; running without it", logging.AttrComponent, "boot", logging.AttrError, err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), telemetryFlush)
		defer cancel()
		if err := shutdownTelemetry(flushCtx); err != nil {
			logger.Warn("telemetry shutdown", logging.AttrComponent, "boot", logging.AttrError, err)
		}
	}()

	pool, err := newPool(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := queue.Migrate(ctx, pool, cfg.RiverSchema); err != nil {
		return err
	}

	reg := jobs.Registry()
	client, err := queue.NewClient(pool, cfg, reg, logger)
	if err != nil {
		return fmt.Errorf("river client: %w", err)
	}

	checks := healthcheck.New(pool)
	healthSrv := &http.Server{Addr: cfg.HealthAddr, Handler: checks.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server exited", logging.AttrComponent, "boot", logging.AttrError, err)
		}
	}()

	works := queue.Works(cfg, reg)
	if works {
		// Stop is called explicitly on shutdown, so the client's work outlives
		// the signal context until it has drained.
		if err := client.Start(context.WithoutCancel(ctx)); err != nil {
			return fmt.Errorf("start river: %w", err)
		}
	} else {
		logger.Info("no job kinds registered for the configured queues; enqueue only",
			logging.AttrComponent, "boot", "queues", cfg.RiverQueues, "kinds", reg.Len())
	}

	gs, err := startGRPC(cfg, logger, pool, client, reg)
	if err != nil {
		return err
	}

	checks.SetReady(true)
	logger.Info("river-service ready", logging.AttrComponent, "boot",
		"queues", cfg.RiverQueues, "kinds", reg.Len(), "grpc", cfg.GRPCAddr, "health", cfg.HealthAddr)

	<-ctx.Done()
	stop() // a second signal now force-quits
	logger.Info("shutdown signal received; draining", logging.AttrComponent, "boot")
	checks.SetReady(false)
	shutdown(logger, cfg, gs, client, works, healthSrv)
	return nil
}

func newPool(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.DatabaseDSN)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_DSN: %w", err)
	}
	pc.MaxConns = cfg.DBMaxConns
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// startGRPC serves JobService when GRPC_ADDR is set. The heavy service leaves
// it unset and only works queues.
func startGRPC(cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, client *river.Client[pgx.Tx], reg *queue.Registry) (*grpc.Server, error) {
	if cfg.GRPCAddr == "" {
		return nil, nil
	}
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", cfg.GRPCAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.GRPCAddr, err)
	}
	gs := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(grpcauth.Unary(cfg.GRPCAuthToken)),
		grpc.ChainStreamInterceptor(grpcauth.Stream(cfg.GRPCAuthToken)),
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionAge: maxConnectionAge, MaxConnectionAgeGrace: grpcStopGrace}),
	)
	jobsv1.RegisterJobServiceServer(gs, jobservice.New(pool, client, reg, cfg.RiverSchema))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(gs, hs)
	hs.SetServingStatus("jobs.v1.JobService", healthpb.HealthCheckResponse_SERVING)
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	reflection.Register(gs)
	go func() {
		if err := gs.Serve(lis); err != nil {
			logger.Error("grpc server exited", logging.AttrComponent, "boot", logging.AttrError, err)
		}
	}()
	return gs, nil
}

// shutdown stops taking new jobs, then drains River: running jobs get the
// soft-stop time to finish, then are cancelled and rescheduled, and the stop
// gives up at SHUTDOWN_TIMEOUT.
func shutdown(logger *slog.Logger, cfg *config.Config, gs *grpc.Server, client *river.Client[pgx.Tx], works bool, healthSrv *http.Server) {
	if gs != nil {
		stopGRPC(gs)
	}
	if works {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		if err := client.Stop(ctx); err != nil {
			logger.Warn("river did not stop within SHUTDOWN_TIMEOUT; jobs still running are rescued after their timeout",
				logging.AttrComponent, "boot", logging.AttrError, err)
		}
		cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = healthSrv.Shutdown(ctx)
}

func stopGRPC(gs *grpc.Server) {
	done := make(chan struct{})
	go func() {
		gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grpcStopGrace):
		gs.Stop()
	}
}
