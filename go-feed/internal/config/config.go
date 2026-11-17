package config

import (
	"flag"
	"os"
	"strconv"
	"time"

	"github.com/ndrandal/feed-simulator/go-feed/internal/orderbook"
)

// Config holds all simulator configuration.
type Config struct {
	// Server
	WSPort int
	Host   string

	// Database
	DatabaseURL string

	// Trade retention
	TradeRetentionDays int

	// Simulation
	Seed             int64
	TickInterval     time.Duration
	SnapshotInterval time.Duration
	SendBufferSize   int

	// Trade archiver (opt-in: only active when ArchiveDir is set)
	ArchiveDir           string
	ArchiveMaxGB         int
	ArchiveIntervalHours int
	ArchiveAfterHours    int

	// Behaviour regimes: per-Step trigger probabilities (0 = that regime off).
	// RegimesEnabled=false switches all four off regardless of the rates.
	RegimesEnabled bool
	Regimes        orderbook.RegimeConfig

	// Stress
	StressCalmMinMs   int
	StressCalmMaxMs   int
	StressActiveMinMs int
	StressActiveMaxMs int
	StressBurstMinMs  int
	StressBurstMaxMs  int
}

func Load() *Config {
	c := &Config{}

	flag.IntVar(&c.WSPort, "port", envInt("FEED_PORT", 8100), "WebSocket server port")
	flag.StringVar(&c.Host, "host", envStr("FEED_HOST", "0.0.0.0"), "Listen host")

	flag.StringVar(&c.DatabaseURL, "database-url", envStr("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/feedsim?sslmode=disable"), "PostgreSQL connection URL")
	flag.IntVar(&c.TradeRetentionDays, "trade-retention", envInt("TRADE_RETENTION_DAYS", 2), "Trade log retention in days, tuned to the 2GB budget (0 = keep forever)")

	flag.StringVar(&c.ArchiveDir, "archive-dir", envStr("ARCHIVE_DIR", ""), "Directory for trade archives (empty = disabled)")
	flag.IntVar(&c.ArchiveMaxGB, "archive-max-gb", envInt("ARCHIVE_MAX_GB", 4), "Max archive disk usage in GB")
	flag.IntVar(&c.ArchiveIntervalHours, "archive-interval", envInt("ARCHIVE_INTERVAL_HOURS", 6), "Hours between archive runs")
	flag.IntVar(&c.ArchiveAfterHours, "archive-after", envInt("ARCHIVE_AFTER_HOURS", 24), "Archive trades older than this many hours")

	flag.Int64Var(&c.Seed, "seed", envInt64("FEED_SEED", 0), "PRNG seed (0 = random)")
	flag.IntVar(&c.SendBufferSize, "send-buffer", envInt("SEND_BUFFER", 4096), "Per-client send buffer size")

	def := orderbook.DefaultRegimes()
	flag.BoolVar(&c.RegimesEnabled, "regimes", envBool("FEED_REGIMES", true), "Enable behaviour regimes (sweeps, icebergs, fleeting orders, large orders)")
	flag.Float64Var(&c.Regimes.SweepRate, "regime-sweep-rate", envFloat("REGIME_SWEEP_RATE", def.SweepRate), "Per-step probability of a multi-level sweep (0 = off)")
	flag.Float64Var(&c.Regimes.IcebergRate, "regime-iceberg-rate", envFloat("REGIME_ICEBERG_RATE", def.IcebergRate), "Per-step probability of starting an iceberg (0 = off)")
	flag.Float64Var(&c.Regimes.FleetingRate, "regime-fleeting-rate", envFloat("REGIME_FLEETING_RATE", def.FleetingRate), "Per-step probability of a fleeting add/cancel burst (0 = off)")
	flag.Float64Var(&c.Regimes.LargeRate, "regime-large-rate", envFloat("REGIME_LARGE_RATE", def.LargeRate), "Per-step probability of placing a large resting order (0 = off)")

	flag.IntVar(&c.StressCalmMinMs, "stress-calm-min", 10, "Stress calm phase min tick ms")
	flag.IntVar(&c.StressCalmMaxMs, "stress-calm-max", 50, "Stress calm phase max tick ms")
	flag.IntVar(&c.StressActiveMinMs, "stress-active-min", 2, "Stress active phase min tick ms")
	flag.IntVar(&c.StressActiveMaxMs, "stress-active-max", 10, "Stress active phase max tick ms")
	flag.IntVar(&c.StressBurstMinMs, "stress-burst-min", 1, "Stress burst phase min tick ms")
	flag.IntVar(&c.StressBurstMaxMs, "stress-burst-max", 2, "Stress burst phase max tick ms")

	flag.Parse()

	c.TickInterval = 100 * time.Millisecond
	c.SnapshotInterval = 30 * time.Second

	if !c.RegimesEnabled {
		c.Regimes = orderbook.RegimeConfig{}
	}

	return c
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		if v == "off" {
			return false
		}
		if v == "on" {
			return true
		}
	}
	return def
}
