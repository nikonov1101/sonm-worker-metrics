package discovery

import (
	"context"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sonm-io/core/insonmnia/auth"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/core/proto"
	"github.com/sonm-io/core/util"
	"github.com/sonm-io/core/util/xgrpc"
	"go.uber.org/zap"
)

// Discoverer provides a list of targets to monitor
type Discoverer interface {
	List(ctx context.Context) ([]common.Address, error)
}

type rvDiscovery struct {
	log        *zap.Logger
	rendezvous sonm.RendezvousClient
}

func NewRendezvousDiscovery(ctx context.Context, log *zap.Logger, creds *xgrpc.TransportCredentials, nppCfg npp.Config) (Discoverer, error) {
	rvEthAddr, err := nppCfg.Rendezvous.Endpoints[0].ETH() // TODO(sshaman1101): wtf?
	if err != nil {
		return nil, err
	}

	rvFullAddr := nppCfg.Rendezvous.Endpoints[0].String() // TODO(sshaman1101): WTF?

	rvCreds := auth.NewWalletAuthenticator(creds, rvEthAddr)
	rvClientConn, err := xgrpc.NewClient(ctx, rvFullAddr, rvCreds)
	if err != nil {
		return nil, err
	}

	d := &rvDiscovery{
		log:        log,
		rendezvous: sonm.NewRendezvousClient(rvClientConn),
	}

	return d, nil
}

func (m *rvDiscovery) List(ctx context.Context) ([]common.Address, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	state, err := m.rendezvous.Info(ctx, &sonm.Empty{})
	if err != nil {
		return nil, err
	}

	peers := map[common.Address]struct{}{}
	for addr := range state.GetState() {
		parts := strings.Split(addr, "//")
		if len(parts) < 2 {
			m.log.Warn("failed to parse peer addr", zap.String("raw", addr))
			continue
		}

		parsed, err := util.HexToAddress(parts[1])
		if err != nil {
			m.log.Warn("failed to parse peer's eth addr", zap.String("raw", parts[1]), zap.Error(err))
			continue
		}

		peers[parsed] = struct{}{}
	}

	var list []common.Address
	for peer := range peers {
		list = append(list, peer)
	}

	return list, nil
}
