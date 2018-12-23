package discovery

import (
	"context"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sonm-io/core/insonmnia/auth"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/core/proto"
	"github.com/sonm-io/core/toolz/sonm-monitoring/types"
	"github.com/sonm-io/core/util"
	"github.com/sonm-io/core/util/xgrpc"
	"go.uber.org/zap"
)

type rvDiscovery struct {
	log        *zap.Logger
	rendezvous sonm.RendezvousClient
}

func NewRendezvousDiscovery(ctx context.Context, log *zap.Logger, creds *xgrpc.TransportCredentials, nppCfg npp.Config) (*rvDiscovery, error) {
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

func (m *rvDiscovery) List(ctx context.Context) ([]common.Address, map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	state, err := m.rendezvous.Info(ctx, &sonm.Empty{})
	if err != nil {
		return nil, nil, err
	}

	peers := map[common.Address]struct{}{}
	protocols := types.AccumulatedMetrics{}

	for addr, info := range state.GetState() {
		// drop servers which are not re-announced yet
		if len(info.Servers) == 0 {
			continue
		}

		parts := strings.Split(addr, "//")
		if len(parts) < 2 {
			m.log.Warn("failed to parse peer addr", zap.String("raw", addr))
			continue
		}

		proto := strings.Replace(parts[0], ":", "", -1)
		protocols.Insert(proto, 1)

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

	return list, protocols.Unwrap(), nil
}
