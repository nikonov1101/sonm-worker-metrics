package config

import (
	"github.com/sonm-io/core/accounts"
	"github.com/sonm-io/core/insonmnia/dwh"
	"github.com/sonm-io/core/insonmnia/npp"
	"github.com/sonm-io/monitoring/collector"
	"github.com/sonm-io/monitoring/influx"
	"github.com/sonm-io/monitoring/plugins/wallet"
)

type Config struct {
	Services struct {
		Ethereum accounts.EthConfig `yaml:"ethereum"`
		Influx   influx.Config      `yaml:"influx"`
		NPP      npp.Config         `yaml:"npp"`
		DWH      dwh.YAMLConfig     `yaml:"dwh"`
	} `yaml:"services"`

	Plugins struct {
		Collector collector.Config `yaml:"collector"`
		Wallet    wallet.Config    `yaml:"wallet"`
	} `yaml:"plugins"`
}
