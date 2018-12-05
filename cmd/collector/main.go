package main

import (
	"context"
	"flag"
	"github.com/sonm-io/core/util/debug"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jinzhu/configor"
	"github.com/sonm-io/core/accounts"
	"github.com/sonm-io/core/cmd"
	"github.com/sonm-io/core/insonmnia/logging"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/core/toolz/sonm-monitoring/collector"
	"github.com/sonm-io/core/toolz/sonm-monitoring/discovery"
	"github.com/sonm-io/core/toolz/sonm-monitoring/exporter"
	"github.com/sonm-io/core/util"
	"github.com/sonm-io/core/util/xgrpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

type Config struct {
	Exporter exporter.Config    `yaml:"exporter"`
	NPP      npp.Config         `yaml:"npp"`
	Eth      accounts.EthConfig `yaml:"ethereum"`
}

const (
	metricsPointName = "worker_metrics"
)

var configPath string

func init() {
	flag.StringVar(&configPath, "config", "collector.yaml", "path to config file")
	flag.Parse()
}

func main() {
	log, err := logging.BuildLogger(logging.Config{Output: "stdout", Level: logging.NewLevel(zapcore.DebugLevel)})
	if err != nil {
		panic(err)
	}

	cfg := Config{}
	err = configor.Load(&cfg, configPath)
	if err != nil {
		log.Fatal("failed to load config file", zap.String("path", configPath), zap.Error(err))
		return
	}

	pkey, err := cfg.Eth.LoadKey()
	if err != nil {
		log.Fatal("failed to load ethereum key", zap.String("keystore", cfg.Eth.Keystore), zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, tlsConfig, err := util.NewHitlessCertRotator(ctx, pkey)
	if err != nil {
		log.Fatal("failed to create certificate rotator")
	}
	creds := xgrpc.NewTransportCredentials(tlsConfig)

	exporto, err := exporter.NewExporter(&cfg.Exporter)
	if err != nil {
		log.Fatal("failed to create exporter instance", zap.Error(err))
	}
	defer exporto.Close()

	disco, err := discovery.NewRendezvousDiscovery(ctx, log, creds, cfg.NPP)
	if err != nil {
		log.Fatal("failed to create discovery service", zap.Error(err))
	}

	collectro, err := collector.NewMetricsCollector(log, pkey, creds, cfg.NPP)
	if err != nil {
		log.Fatal("failed to create collector service", zap.Error(err))
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return cmd.WaitInterrupted(ctx)
	})
	wg.Go(func() error {
		return debug.ServePProf(ctx, debug.Config{Port: 6060}, log)
	})

	log.Info("starting metrics collector")

	tk := util.NewImmediateTicker(time.Minute)
	defer tk.Stop()

x1:
	for {
		select {
		case <-ctx.Done():
			log.Warn("context done", zap.Error(ctx.Err()))
			break x1
		case <-tk.C:
			workers, err := disco.List(ctx)
			if err != nil {
				log.Warn("failed to get workers from discovery", zap.Error(err))
				continue
			}

			log.Info("workers collected", zap.Int("count", len(workers)))

			for _, worker := range workers {
				go func(w common.Address) {
					var noStatus, noMetrics bool

					status, err := collectro.Status(ctx, w)
					if err != nil {
						log.Warn("failed to collect status", zap.Stringer("worker", w), zap.Error(err))
						noStatus = true
					}

					metrics := make(map[string]float64)
					if !noStatus {
						metrics, err = collectro.Metrics(ctx, w, status["version"])
						if err != nil {
							log.Warn("failed to collect metrics", zap.Stringer("worker", w), zap.Error(err))
							noMetrics = true
						}
					}

					if noMetrics || noStatus {
						metrics = map[string]float64{"error": 1}
						status = map[string]string{}
					}

					if err := exporto.Write(metricsPointName, w, metrics, status); err != nil {
						log.Warn("failed to write metrics", zap.Stringer("worker", w), zap.Error(err))
					}
				}(worker)
			}
		}
	}

	log.Debug("termination")
	wg.Wait()
}
