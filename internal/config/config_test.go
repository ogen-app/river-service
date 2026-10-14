package config

import (
	"strings"
	"testing"
	"time"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_DSN", "postgres://river_service@localhost/ogen")
	t.Setenv("GRPC_AUTH_TOKEN", "secret")
}

func TestLoadDefaults(t *testing.T) {
	setRequired(t)
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.QueueWorkers(); len(got) != 1 || got[QueueDefault] != 4 {
		t.Errorf("QueueWorkers() = %v, want default:4", got)
	}
	if got := cfg.EffectiveSoftStop(); got != 240*time.Second {
		t.Errorf("EffectiveSoftStop() = %v, want 240s", got)
	}
	if cfg.RiverSchema != "river_service" || cfg.GRPCAddr != ":9092" {
		t.Errorf("schema %q, grpc %q", cfg.RiverSchema, cfg.GRPCAddr)
	}
}

func TestLoadHeavyService(t *testing.T) {
	setRequired(t)
	t.Setenv("RIVER_QUEUES", " Audio, image ,ingest,audio")
	t.Setenv("GRPC_ADDR", "")
	t.Setenv("GRPC_AUTH_TOKEN", "")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.QueueWorkers()
	if len(got) != 3 || got[QueueAudio] != 2 || got[QueueImage] != 2 || got[QueueIngest] != 2 {
		t.Errorf("QueueWorkers() = %v, want audio, image and ingest at 2", got)
	}
}

func TestLoadRejects(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "unknown queue", env: map[string]string{"RIVER_QUEUES": "default,emails"}, want: `unknown queue "emails"`},
		{name: "grpc without token", env: map[string]string{"GRPC_AUTH_TOKEN": ""}, want: "GRPC_AUTH_TOKEN is required"},
		{name: "shutdown shorter than the margin", env: map[string]string{"SHUTDOWN_TIMEOUT": "10s"}, want: "SHUTDOWN_TIMEOUT must exceed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequired(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load() error = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestEffectiveSoftStopClamps(t *testing.T) {
	cfg := Config{SoftStopTimeout: 10 * time.Minute, ShutdownTimeout: 270 * time.Second}
	if got := cfg.EffectiveSoftStop(); got != 255*time.Second {
		t.Errorf("EffectiveSoftStop() = %v, want 255s", got)
	}
}
