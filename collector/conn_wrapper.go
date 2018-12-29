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
	"github.com/sonm-io/core/toolz/sonm-monitoring/discovery"
	"github.com/sonm-io/core/toolz/sonm-monitoring/types"
	"github.com/sonm-io/core/util/xgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type connectWrapper struct {
	log         *zap.Logger
	disco       *discovery.RVDiscovery
	dialer      *npp.Dialer
	credentials credentials.TransportCredentials
}

/*
	TODO: interface
*/

func NewConnWrapper(log *zap.Logger, nppCfg npp.Config, key *ecdsa.PrivateKey, creds *xgrpc.TransportCredentials, dis *discovery.RVDiscovery) (*connectWrapper, error) {
	nppDialerOptions := []npp.Option{
		npp.WithRendezvous(nppCfg.Rendezvous, creds),
		npp.WithRelay(nppCfg.Relay, key),
	}

	//if cfg.VerboseDialerLogs {
	//	nppDialerOptions = append(nppDialerOptions, npp.WithLogger(log))
	//}

	nppDialer, err := npp.NewDialer(nppDialerOptions...)
	if err != nil {
		return nil, err
	}

	return &connectWrapper{
		log:         log,
		disco:       dis,
		dialer:      nppDialer,
		credentials: creds,
	}, nil
}

// PeerList returns online peers and related meta information (protocols counters in that impl).
func (m *connectWrapper) PeerList(ctx context.Context) ([]common.Address, map[string]interface{}, error) {
	return m.disco.List(ctx)
}

func (m *connectWrapper) PeerStats(ctx context.Context, addr common.Address) (map[string]float64, map[string]string) {
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

func (m *connectWrapper) x() map[string]interface{} {
	metrics, err := m.dialer.Metrics()
	if err != nil {
		m.log.Warn("failed to get npp metrics", zap.Error(err))
		return map[string]interface{}{}
	}

	// TODO(sshaman1101):
	//if m.cfg.SaveDialerMetricsToFile {
	//	m.stashDialerMetrics(metrics)
	//}

	return types.DialerMetricsToMap(metrics)
}

func (m *connectWrapper) getMetrics(ctx context.Context, addr common.Address, version string) (map[string]float64, error) {
	log := m.log.Named("metrics").With(zap.String("worker", addr.Hex()))

	if !m.compareVersions(version) {
		log.Warn("worker does not support metrics collection", zap.String("version", version))
		return map[string]float64{}, nil
	}

	log.Info("start collecting metrics")
	ctx, cancel := context.WithTimeout(ctx, connTimeoutSec*time.Second)
	defer cancel()

	cc, err := m.getWorkerClientConn(ctx, addr)
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

func (m *connectWrapper) getStatus(ctx context.Context, addr common.Address) (map[string]string, error) {
	log := m.log.Named("status").With(zap.String("worker", addr.Hex()))
	log.Info("start collecting status")

	ctx, cancel := context.WithTimeout(ctx, connTimeoutSec*time.Second)
	defer cancel()

	cc, err := m.getWorkerClientConn(ctx, addr)
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

func (m *connectWrapper) compareVersions(version string) bool {
	v, err := semver.ParseTolerant(version)
	if err != nil {
		m.log.Warn("failed to parse worker version", zap.String("raw", version), zap.Error(err))
		return false
	}

	v.Build = nil
	v.Pre = nil

	return v.Compare(desiredVersion) >= 0
}

func (m *connectWrapper) getWorkerClientConn(ctx context.Context, addr common.Address) (*grpc.ClientConn, error) {
	ethAddr := auth.NewETHAddr(addr)
	conn, err := m.dialer.DialContext(ctx, *ethAddr)
	if err != nil {
		return nil, err
	}

	return xgrpc.NewClient(ctx, "-", auth.NewWalletAuthenticator(m.credentials, addr), xgrpc.WithConn(conn))
}

// addPercentFields calculates percent values for absolute values such as total/free memory in bytes,
// then appends it to the whole metrics set.
func (m *connectWrapper) addPercentFields(data map[string]float64) map[string]float64 {
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

func (m *connectWrapper) closeConn(cc *grpc.ClientConn) {
	if err := cc.Close(); err != nil {
		m.log.Warn("clientConn close failed with error", zap.Error(err))
	}
}
