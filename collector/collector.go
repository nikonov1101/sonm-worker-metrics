package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/blang/semver"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/core/toolz/sonm-monitoring/influx"
	"github.com/sonm-io/core/toolz/sonm-monitoring/types"
	"github.com/sonm-io/core/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	connTimeoutSec       = 50
	metricsPointName     = "worker_metrics"
	protocolsPointName   = "protocols"
	connectionsPointName = "connections"
)

var desiredVersion = semver.Version{Major: 0, Minor: 4, Patch: 19}

type Config struct {
	VerboseDialerLogs       bool `yaml:"verbose_dialer_logs"`
	SaveDialerMetricsToFile bool `yaml:"save_dialer_metrics_to_file"`
}

type workerMetricsCollector struct {
	cfg    *Config
	log    *zap.Logger
	influx *influx.Influx
	connw  *connectWrapper
}

func NewMetricsCollector(log *zap.Logger, cfg Config, infl *influx.Influx, cw *connectWrapper) *workerMetricsCollector {
	m := &workerMetricsCollector{
		cfg:    &cfg,
		log:    log.Named("observer"),
		influx: infl,
		connw:  cw,
	}
	return m
}

func (m *workerMetricsCollector) Run(ctx context.Context) {
	m.log.Info("starting metrics collector")
	defer m.log.Info("stopping metrics collector")

	tk := util.NewImmediateTicker(time.Minute)
	defer tk.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			m.once(ctx)
		}
	}
}

func (m *workerMetricsCollector) once(ctx context.Context) {
	// load peer list to observe
	workers, protocols, err := m.connw.PeerList(ctx)
	if err != nil {
		m.log.Warn("failed to get workers from discovery", zap.Error(err))
		return
	}

	// write protocols info into separated distribution
	m.log.Info("workers collected", zap.Int("count", len(workers)))
	if err := m.influx.WriteRaw(protocolsPointName, nil, protocols); err != nil {
		m.log.Warn("failed to write protocols metrics", zap.Error(err))
	}

	// loop over peers, connect via NPP and collect metrics
	wg, cctx := errgroup.WithContext(ctx)
	for _, worker := range workers {
		w := worker
		wg.Go(func() error {
			metrics, status := m.connw.PeerStats(cctx, w)

			// write scrapped info into influxDB for alerting and future processing
			if err := m.influx.Write(metricsPointName, w, metrics, status); err != nil {
				m.log.Warn("failed to write metrics", zap.Stringer("worker", w), zap.Error(err))
			}

			return nil
		})
	}

	_ = wg.Wait()

	if err := m.influx.WriteRaw(connectionsPointName, nil, m.DialerMetrics()); err != nil {
		m.log.Warn("failed to write dialer metrics", zap.Error(err))
	}
}

// DialerMetrics converts nppDialer metrics into influxDB-friendly format
func (m *workerMetricsCollector) DialerMetrics() map[string]interface{} {
	metrics, err := m.dialer.Metrics()
	if err != nil {
		m.log.Warn("failed to get npp metrics", zap.Error(err))
		return map[string]interface{}{}
	}

	if m.cfg.SaveDialerMetricsToFile {
		m.stashDialerMetrics(metrics)
	}

	return types.DialerMetricsToMap(metrics)
}

func (m *workerMetricsCollector) stashDialerMetrics(data map[string][]*npp.NamedMetric) {
	fname := fmt.Sprintf("/tmp/dialer_stats_%d.json", time.Now().Unix())
	j, err := json.Marshal(data)
	if err != nil {
		m.log.Error("failed to marshal dialer stats", zap.Error(err))
		return
	}

	if err := ioutil.WriteFile(fname, j, 0600); err != nil {
		m.log.Error("failed to write dialer stats", zap.String("file", fname), zap.Error(err))
	}
}
