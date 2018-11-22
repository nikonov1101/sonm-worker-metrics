package collector

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sonm-io/core/insonmnia/auth"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/core/proto"
	"github.com/sonm-io/core/util/xgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
)

type metricsLoader struct {
	log         *zap.Logger
	dialer      *npp.Dialer
	credentials credentials.TransportCredentials
}

func NewMetricsCollector(log *zap.Logger, key *ecdsa.PrivateKey, creds credentials.TransportCredentials, cfg npp.Config) (*metricsLoader, error) {
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

func (m *metricsLoader) Metrics(ctx context.Context, addr common.Address) (map[string]float64, error) {
	m.log.Sugar().Infof("start collecting metrics from %s", addr.Hex())

	ctx, cancel := context.WithTimeout(ctx, 150*time.Second)
	defer cancel()

	client, err := m.workerClient(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker client: %v", err)
	}

	response, err := client.Metrics(ctx, &sonm.WorkerMetricsRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to obtain metrics: %v", err)
	}

	return response.GetMetrics(), nil
}

func (m *metricsLoader) Status(ctx context.Context, addr common.Address) (*sonm.StatusReply, error) {
	m.log.Sugar().Infof("start collecting status from %s", addr.Hex())

	ctx, cancel := context.WithTimeout(ctx, 150*time.Second)
	defer cancel()

	client, err := m.workerClient(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker client: %v", err)
	}

	return client.Status(ctx, &sonm.Empty{})
}

func (m *metricsLoader) workerClient(ctx context.Context, addr common.Address) (sonm.WorkerManagementClient, error) {
	ethAddr := auth.NewETHAddr(addr)
	conn, err := m.dialer.DialContext(ctx, *ethAddr)
	if err != nil {
		return nil, err
	}

	cc, err := xgrpc.NewClient(ctx, "-", auth.NewWalletAuthenticator(m.credentials, addr), xgrpc.WithConn(conn))
	if err != nil {
		return nil, err
	}

	return sonm.NewWorkerManagementClient(cc), nil
}
