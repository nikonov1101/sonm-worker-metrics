package main

import (
	"context"
	"flag"
	"time"

	"github.com/jinzhu/configor"
	"github.com/sonm-io/core/accounts"
	"github.com/sonm-io/core/cmd"
	"github.com/sonm-io/core/toolz/metrics-collector/collector"
	"github.com/sonm-io/core/toolz/metrics-collector/discovery"
	"github.com/sonm-io/core/toolz/metrics-collector/exporter"
	"github.com/sonm-io/core/insonmnia/logging"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/core/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

type Config struct {
	Exporter exporter.Config    `yaml:"exporter"`
	NPP      npp.Config         `yaml:"npp"`
	Eth      accounts.EthConfig `yaml:"ethereum"`
}

var configPath string

func init() {
	flag.StringVar(&configPath, "config", "exporter.yaml", "path to config file")
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
	creds := util.NewTLS(tlsConfig)

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
		log.Fatal("failef to create collector service", zap.Error(err))
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return cmd.WaitInterrupted(ctx)
	})

	log.Info("starting metrics collector")
	wg.Go(func() error {
		for {
			workers, err := disco.List(ctx)
			if err != nil {
				log.Warn("failed to get workers from discovery", zap.Error(err))
				continue
			}

			log.Info("workers collected", zap.Int("count", len(workers)))

			for _, w := range workers {
				worker := w
				wg.Go(func() error {
					s, err := collectro.Status(ctx, worker)
					if err != nil {
						log.Warn("failed to collect status", zap.Stringer("worker", worker), zap.Error(err))
						return nil
					}

					err = exporto.Write(
						worker,
						map[string]float64{},
						map[string]string{
							"version": s.GetVersion(),
							"geo":     s.GetGeo().GetCountry().GetIsoCode(),
						})

					if err != nil {
						log.Warn("failed to write metrics", zap.Stringer("worker", worker), zap.Error(err))
					}

					return nil
				})
			}

			log.Info("start waiting for 60s")
			time.Sleep(60 * time.Second)
		}
	})

	wg.Wait()
}
