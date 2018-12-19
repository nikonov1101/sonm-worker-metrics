package main

import (
	"context"
	"flag"
	"time"

	"github.com/jinzhu/configor"
	"github.com/opentracing/opentracing-go"
	"github.com/sonm-io/core/accounts"
	"github.com/sonm-io/core/cmd"
	"github.com/sonm-io/core/insonmnia/logging"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/core/toolz/sonm-monitoring/aggregator"
	"github.com/sonm-io/core/toolz/sonm-monitoring/collector"
	"github.com/sonm-io/core/toolz/sonm-monitoring/discovery"
	"github.com/sonm-io/core/toolz/sonm-monitoring/exporter"
	"github.com/sonm-io/core/toolz/sonm-monitoring/types"
	"github.com/sonm-io/core/util"
	"github.com/sonm-io/core/util/debug"
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
	opentracing.SetGlobalTracer(types.NewBasicTracer())

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

	// exporto is InfluxDB adapter
	exporto, err := exporter.NewExporter(&cfg.Exporter)
	if err != nil {
		log.Fatal("failed to create exporter instance", zap.Error(err))
	}
	defer exporto.Close()

	// aggregator reading data from InfluxDB and aggregating
	// various metrics
	aggr := aggregator.NewAggregator(log, exporto)

	// discovery service is obtaining info about online peers in SONM network
	disco, err := discovery.NewRendezvousDiscovery(ctx, log, creds, cfg.NPP)
	if err != nil {
		log.Fatal("failed to create discovery service", zap.Error(err))
	}

	// collector querying peers for various hardware (and software in closest future) metrics
	collectro, err := collector.NewMetricsCollector(log, pkey, creds, cfg.NPP)
	if err != nil {
		log.Fatal("failed to create collector service", zap.Error(err))
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return cmd.WaitInterrupted(ctx)
	})
	wg.Go(func() error {
		return debug.ServePProf(ctx, debug.Config{Port: 6065}, log)
	})
	wg.Go(func() error {
		aggr.Run(ctx)
		return nil
	})

	log.Info("starting metrics collector")
	tk := util.NewImmediateTicker(time.Minute)
	defer tk.Stop()

	mtk := time.NewTicker(time.Minute)
	defer mtk.Stop()

x1:
	for {
		select {
		case <-ctx.Done():
			log.Warn("context done", zap.Error(ctx.Err()))
			break x1
		case <-tk.C:
			// load peer list to monitor
			workers, protocols, err := disco.List(ctx)
			if err != nil {
				log.Warn("failed to get workers from discovery", zap.Error(err))
				continue
			}

			if err := exporto.WriteRaw("protocols", nil, protocols); err != nil {
				log.Warn("failed to write protocols metrics", zap.Error(err))
			}

			log.Info("workers collected", zap.Int("count", len(workers)))
			// loop over peers, connect via NPP and collect metrics

			wwg, cctx := errgroup.WithContext(ctx)
			for _, worker := range workers {
				w := worker
				wwg.Go(func() error {
					// todo: will be better to split those statuses
					//  no_status and no_metrics
					metrics := map[string]float64{"error": 0}
					noStatus := false

					// the `status` handle returns version and country
					status, err := collectro.Status(cctx, w)
					if err != nil {
						log.Warn("failed to collect status", zap.Stringer("worker", w), zap.Error(err))
						metrics = map[string]float64{"error": 1}
						status = map[string]string{}
						noStatus = true
					}

					if !noStatus {
						// we can ask for metrics if worker is online and version supports metrics
						metrics, err = collectro.Metrics(cctx, w, status["version"])
						if err != nil {
							log.Warn("failed to collect metrics", zap.Stringer("worker", w), zap.Error(err))
						} else {
							metrics["error"] = 0
						}
					}

					// write scrapped info into influxDB for alerting and future processing
					if err := exporto.Write(metricsPointName, w, metrics, status); err != nil {
						log.Warn("failed to write metrics", zap.Stringer("worker", w), zap.Error(err))
					}

					return nil
				})
			}

			if err := wwg.Wait(); err != nil {
				log.Warn("хуйня какая-то", zap.Error(err))
			}

			if err := exporto.WriteRaw("connections", nil, collectro.DialerMetrics()); err != nil {
				log.Warn("failed to write dialer metrics", zap.Error(err))
			}
		}
	}

	err = wg.Wait()
	log.Debug("termination", zap.Error(err))
}
