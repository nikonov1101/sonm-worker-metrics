package main

import (
	"context"
	"flag"

	"github.com/jinzhu/configor"
	"github.com/opentracing/opentracing-go"
	"github.com/sonm-io/core/blockchain"
	"github.com/sonm-io/core/cmd"
	"github.com/sonm-io/core/insonmnia/logging"
	sonm "github.com/sonm-io/core/proto"
	"github.com/sonm-io/core/util"
	"github.com/sonm-io/core/util/debug"
	"github.com/sonm-io/core/util/xgrpc"
	"github.com/sonm-io/monitoring/aggregator"
	"github.com/sonm-io/monitoring/collector"
	"github.com/sonm-io/monitoring/config"
	"github.com/sonm-io/monitoring/discovery"
	"github.com/sonm-io/monitoring/influx"
	"github.com/sonm-io/monitoring/plugins/wallet"
	"github.com/sonm-io/monitoring/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
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

	/* load config */
	cfg := config.Config{}
	if err := configor.Load(&cfg, configPath); err != nil {
		log.Fatal("failed to load config file", zap.String("path", configPath), zap.Error(err))
		return
	}

	/* init services dependencies */
	pkey, err := cfg.Services.Ethereum.LoadKey()
	if err != nil {
		log.Fatal("failed to load ethereum key", zap.String("keystore", cfg.Services.Ethereum.Keystore), zap.Error(err))
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

	inf, err := influx.NewInfluxClient(&cfg.Services.Influx)
	if err != nil {
		log.Fatal("failed to create exporter instance", zap.Error(err))
		return
	}
	defer inf.Close()

	bc, err := blockchain.NewAPI(ctx)
	if err != nil {
		log.Fatal("failed to create blockchain API client", zap.Error(err))
	}

	dwhCC, err := xgrpc.NewClient(ctx, cfg.Services.DWH.Endpoint, creds)
	if err != nil {
		log.Fatal("failed to create DWH API client", zap.Error(err))
	}

	/* init plugins */

	// aggregator reading data from InfluxDB and aggregating
	// various metrics
	aggr := aggregator.NewAggregator(log, inf)

	// discovery service is obtaining info about online peers in SONM network
	disco, err := discovery.NewRendezvousDiscovery(ctx, log, creds, cfg.Services.NPP)
	if err != nil {
		log.Fatal("failed to create discovery service", zap.Error(err))
	}

	// collector querying peers for various hardware (and software in closest future) metrics
	collectro, err := collector.NewMetricsCollector(log, pkey, creds, cfg.Services.NPP, cfg.Plugins.Collector, inf, disco)
	if err != nil {
		log.Fatal("failed to create collector service", zap.Error(err))
	}

	wp := wallet.NewWalletPlugin(&cfg.Plugins.Wallet, log, inf, bc.SidechainToken(), sonm.NewDWHClient(dwhCC))

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return cmd.WaitInterrupted(ctx)
	})
	wg.Go(func() error {
		return debug.ServePProf(ctx, debug.Config{Port: 6065}, log)
	})
	wg.Go(func() error {
		wp.Run(ctx)
		return nil
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
