package main

import (
	"context"
	"flag"

	"github.com/sonm-io/core/cmd"
	"github.com/sonm-io/core/insonmnia/logging"
	"github.com/sonm-io/core/toolz/sonm-monitoring/aggregator"
	"github.com/sonm-io/core/toolz/sonm-monitoring/exporter"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

var (
	nameFlag string
	addrFlag string
)

func init() {
	flag.StringVar(&nameFlag, "db", "sonm", "influx database name")
	flag.StringVar(&addrFlag, "addr", "http://localhost:8086", "influx endpoint")
	flag.Parse()
}

func main() {
	log, err := logging.BuildLogger(logging.Config{Output: "stdout", Level: logging.NewLevel(zapcore.DebugLevel)})
	if err != nil {
		panic(err)
	}

	cfg := &exporter.Config{DBAddr: addrFlag, DBName: nameFlag}
	log.Info("loaded configuration", zap.Any("cfg", *cfg))

	aggr, err := aggregator.NewAggregator(log, cfg)
	if err != nil {
		log.Fatal("failed to create aggregator", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return cmd.WaitInterrupted(ctx)
	})

	aggr.Run(ctx)
}
