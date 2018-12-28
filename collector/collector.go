package collector

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"time"

	"github.com/blang/semver"
	"github.com/ethereum/go-ethereum/common"
	"github.com/sonm-io/core/insonmnia/auth"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/core/proto"
	"github.com/sonm-io/core/toolz/sonm-monitoring/discovery"
	"github.com/sonm-io/core/toolz/sonm-monitoring/exporter"
	"github.com/sonm-io/core/toolz/sonm-monitoring/types"
	"github.com/sonm-io/core/util"
	"github.com/sonm-io/core/util/xgrpc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	connTimeoutSec     = 50
	metricsPointName   = "worker_metrics"
	protocolsPointName = "protocols"
)

var desiredVersion = semver.Version{Major: 0, Minor: 4, Patch: 19}

type Config struct {
	VerboseDialerLogs       bool `yaml:"verbose_dialer_logs"`
	SaveDialerMetricsToFile bool `yaml:"save_dialer_metrics_to_file"`
}

type metricsLoader struct {
	cfg *Config
	log *zap.Logger

	dialer      *npp.Dialer
	credentials credentials.TransportCredentials

	influx *exporter.Exporter
	disco  *discovery.RVDiscovery
}

// todo: split connection and
//  metrics gathering parts;

func NewMetricsCollector(log *zap.Logger, key *ecdsa.PrivateKey, creds *xgrpc.TransportCredentials, nppCfg npp.Config, cfg Config,
	infl *exporter.Exporter, dis *discovery.RVDiscovery) (*metricsLoader, error) {
	nppDialerOptions := []npp.Option{
		npp.WithRendezvous(nppCfg.Rendezvous, creds),
		npp.WithRelay(nppCfg.Relay, key),
	}

	if cfg.VerboseDialerLogs {
		nppDialerOptions = append(nppDialerOptions, npp.WithLogger(log))
	}

	nppDialer, err := npp.NewDialer(nppDialerOptions...)
	if err != nil {
		return nil, err
	}

	m := &metricsLoader{
		cfg:         &cfg,
		log:         log.Named("observer"),
		dialer:      nppDialer,
		credentials: creds,
		disco:       dis,
		influx:      infl,
	}

	return m, nil
}

func (m *metricsLoader) Run(ctx context.Context) {
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

func (m *metricsLoader) once(ctx context.Context) {
	// load peer list to monitor
	workers, protocols, err := m.disco.List(ctx)
	if err != nil {
		m.log.Warn("failed to get workers from discovery", zap.Error(err))
		return
	}

	m.log.Info("workers collected", zap.Int("count", len(workers)))
	if err := m.influx.WriteRaw(protocolsPointName, nil, protocols); err != nil {
		m.log.Warn("failed to write protocols metrics", zap.Error(err))
	}

	// loop over peers, connect via NPP and collect metrics
	wg, cctx := errgroup.WithContext(ctx)
	for _, worker := range workers {
		w := worker
		wg.Go(func() error {
			metrics, status := m.collectWorkerData(cctx, w)

			// write scrapped info into influxDB for alerting and future processing
			if err := m.influx.Write(metricsPointName, w, metrics, status); err != nil {
				m.log.Warn("failed to write metrics", zap.Stringer("worker", w), zap.Error(err))
			}

			return nil
		})
	}

	_ = wg.Wait()
	if err := m.influx.WriteRaw("connections", nil, m.DialerMetrics()); err != nil {
		m.log.Warn("failed to write dialer metrics", zap.Error(err))
	}
}

func (m *metricsLoader) getMetrics(ctx context.Context, addr common.Address, version string) (map[string]float64, error) {
	log := m.log.Named("metrics").With(zap.String("worker", addr.Hex()))

	if !m.compareVersions(version) {
		log.Warn("worker does not support metrics collection", zap.String("version", version))
		return map[string]float64{}, nil
	}

	log.Info("start collecting metrics")
	ctx, cancel := context.WithTimeout(ctx, connTimeoutSec*time.Second)
	defer cancel()

	cc, err := m.workerClient(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create client connetion: %v", err)
	}

	defer m.closeConn(cc)
	client := sonm.NewWorkerManagementClient(cc)

	response, err := client.Metrics(ctx, &sonm.WorkerMetricsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to obtain metrics: %v", err)
	}

	if len(response.GetMetrics()) == 0 {
		log.Warn("empty metrics set returned")
		return map[string]float64{}, nil
	}

	log.Info("metrics successfully collect")
	return m.addPercentFields(response.GetMetrics()), nil
}

func (m *metricsLoader) getStatus(ctx context.Context, addr common.Address) (map[string]string, error) {
	log := m.log.Named("status").With(zap.String("worker", addr.Hex()))
	log.Info("start collecting status")

	ctx, cancel := context.WithTimeout(ctx, connTimeoutSec*time.Second)
	defer cancel()

	cc, err := m.workerClient(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create client connetion: %v", err)
	}

	defer m.closeConn(cc)
	client := sonm.NewWorkerManagementClient(cc)

	status, err := client.Status(ctx, &sonm.Empty{})
	if err != nil {
		return nil, err
	}

	log.Info("status successfully collect ", zap.String("ver", status.GetVersion()),
		zap.String("loc", status.GetGeo().GetCountry().GetIsoCode()))

	return map[string]string{
		"version": status.GetVersion(),
		"geo":     status.GetGeo().GetCountry().GetIsoCode(),
	}, nil
}

func (m *metricsLoader) collectWorkerData(ctx context.Context, addr common.Address) (map[string]float64, map[string]string) {
	metrics := map[string]float64{"error": 0}
	noStatus := false

	// the `status` handle returns version and country
	status, err := m.getStatus(ctx, addr)
	if err != nil {
		m.log.Warn("failed to collect status", zap.Stringer("worker", addr), zap.Error(err))
		metrics = map[string]float64{"error": 1}
		status = map[string]string{}
		noStatus = true
	}

	if !noStatus {
		// we can ask for metrics if worker is online and version supports metrics
		metrics, err = m.getMetrics(ctx, addr, status["version"])
		if err != nil {
			m.log.Warn("failed to collect metrics", zap.Stringer("worker", addr), zap.Error(err))
		} else {
			metrics["error"] = 0
		}
	}

	return metrics, status
}

func (m *metricsLoader) workerClient(ctx context.Context, addr common.Address) (*grpc.ClientConn, error) {
	ethAddr := auth.NewETHAddr(addr)
	conn, err := m.dialer.DialContext(ctx, *ethAddr)
	if err != nil {
		return nil, err
	}

	return xgrpc.NewClient(ctx, "-", auth.NewWalletAuthenticator(m.credentials, addr), xgrpc.WithConn(conn))
}

// addPercentFields calculates percent values for absolute values such as total/free memory in bytes,
// then appends it to the whole metrics set.
func (m *metricsLoader) addPercentFields(data map[string]float64) map[string]float64 {
	disk := (1 - (data[sonm.MetricsKeyDiskFree] / data[sonm.MetricsKeyDiskTotal])) * 100
	if math.IsInf(disk, 0) || math.IsNaN(disk) {
		disk = 0
	}

	ram := (1 - (data[sonm.MetricsKeyRAMFree] / data[sonm.MetricsKeyRAMTotal])) * 100
	if math.IsInf(ram, 0) || math.IsNaN(ram) {
		disk = 0
	}

	data[sonm.MetricsKeyDiskFreePercent] = disk
	data[sonm.MetricsKeyRAMFreePercent] = ram
	return data
}

func (m *metricsLoader) closeConn(cc *grpc.ClientConn) {
	if err := cc.Close(); err != nil {
		m.log.Warn("clientConn close failed with error", zap.Error(err))
	}
}

func (m *metricsLoader) compareVersions(version string) bool {
	v, err := semver.ParseTolerant(version)
	if err != nil {
		m.log.Warn("failed to parse worker version", zap.String("raw", version), zap.Error(err))
		return false
	}

	v.Build = nil
	v.Pre = nil

	return v.Compare(desiredVersion) >= 0
}

// DialerMetrics converts nppDialer metrics into influxDB-friendly format
func (m *metricsLoader) DialerMetrics() map[string]interface{} {
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

func (m *metricsLoader) stashDialerMetrics(data map[string][]*npp.NamedMetric) {
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
