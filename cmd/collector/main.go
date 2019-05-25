package main

import (
	"context"
	"flag"

	"github.com/jinzhu/configor"
	"github.com/opentracing/opentracing-go"
	"github.com/sonm-io/core/accounts"
	"github.com/sonm-io/core/cmd"
	"github.com/sonm-io/core/insonmnia/logging"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/monitoring/aggregator"
	"github.com/sonm-io/monitoring/collector"
	"github.com/sonm-io/monitoring/discovery"
	"github.com/sonm-io/monitoring/influx"
	"github.com/sonm-io/monitoring/types"
	"github.com/sonm-io/core/util"
	"github.com/sonm-io/core/util/debug"
	"github.com/sonm-io/core/util/xgrpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

type Config struct {
	Influx    influx.Config      `yaml:"influx"`
	Collector collector.Config   `yaml:"collector"`
	Eth       accounts.EthConfig `yaml:"ethereum"`
	NPP       npp.Config         `yaml:"npp"`
}

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
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rot, tlsConfig, err := util.NewHitlessCertRotator(ctx, pkey)
	if err != nil {
		log.Fatal("failed to create certificate rotator")
		return
	}

	defer rot.Close()
	creds := xgrpc.NewTransportCredentials(tlsConfig)

	inf, err := influx.NewInfluxClient(&cfg.Influx)
	if err != nil {
		log.Fatal("failed to create exporter instance", zap.Error(err))
		return
	}
	defer inf.Close()

	// aggregator reading data from InfluxDB and aggregating
	// various metrics
	aggr := aggregator.NewAggregator(log, inf)

	// discovery service is obtaining info about online peers in SONM network
	disco, err := discovery.NewRendezvousDiscovery(ctx, log, creds, cfg.NPP)
	if err != nil {
		log.Fatal("failed to create discovery service", zap.Error(err))
	}

	// collector querying peers for various hardware (and software in closest future) metrics
	collectro, err := collector.NewMetricsCollector(log, pkey, creds, cfg.NPP, cfg.Collector, inf, disco)
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
	wg.Go(func() error {
		collectro.Run(ctx)
		return nil
	})

	err = wg.Wait()
	log.Debug("termination", zap.Error(err))
}
