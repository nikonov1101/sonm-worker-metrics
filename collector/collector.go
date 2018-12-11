package collector

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math"
	"time"

	"github.com/blang/semver"
	"github.com/ethereum/go-ethereum/common"
	"github.com/sonm-io/core/insonmnia/auth"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/core/proto"
	"github.com/sonm-io/core/util/xgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type metricsLoader struct {
	log         *zap.Logger
	dialer      *npp.Dialer
	credentials credentials.TransportCredentials
}

var desiredVersion = semver.Version{Major: 0, Minor: 4, Patch: 19}

func NewMetricsCollector(log *zap.Logger, key *ecdsa.PrivateKey, creds *xgrpc.TransportCredentials, cfg npp.Config) (*metricsLoader, error) {
	nppDialerOptions := []npp.Option{
		npp.WithRendezvous(cfg.Rendezvous, creds),
		npp.WithRelay(cfg.Relay, key),
		// npp.WithLogger(log),
	}

	nppDialer, err := npp.NewDialer(nppDialerOptions...)
	if err != nil {
		return nil, err
	}

	m := &metricsLoader{
		log:         log,
		dialer:      nppDialer,
		credentials: creds,
	}

	return m, nil
}

func (m *metricsLoader) Metrics(ctx context.Context, addr common.Address, version string) (map[string]float64, error) {
	log := m.log.Named("metrics").With(zap.String("worker", addr.Hex()))

	if !m.compareVersions(version) {
		log.Warn("worker does not support metrics collection", zap.String("version", version))
		return map[string]float64{}, nil
	}

	log.Info("start collecting metrics")
	ctx, cancel := context.WithTimeout(ctx, 150*time.Second)
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

func (m *metricsLoader) TestMetrics(ctx context.Context, addr common.Address) (map[string]float64, error) {
	return map[string]float64{}, nil
}

func (m *metricsLoader) Status(ctx context.Context, addr common.Address) (map[string]string, error) {
	log := m.log.Named("status").With(zap.String("worker", addr.Hex()))
	log.Info("start collecting status")

	ctx, cancel := context.WithTimeout(ctx, 150*time.Second)
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

// DialerMetrics accumulates nppDialer into influxDB-friendly format
func (m *metricsLoader) DialerMetrics() map[string]interface{} {
	metrics, err := m.dialer.Metrics()
	if err != nil {
		m.log.Warn("failed to get npp metrics", zap.Error(err))
		return nil
	}

	x := accumulatedMetrics{}

	for _, rows := range metrics {
		for _, row := range rows {
			if row.Metric.Counter != nil {
				x.add(row.Name, row.Metric.Counter.GetValue())
			}

			if row.Metric.Histogram != nil {
				x.add(row.Name, float64(row.Metric.GetHistogram().GetSampleCount()))
			}
		}
	}

	return x.calculatePercents().intoMapStringInterface()
}
