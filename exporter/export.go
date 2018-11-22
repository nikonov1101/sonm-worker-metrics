package exporter

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/influxdata/influxdb/client/v2"
)

const influxClientDefaultTimeout = 10 * time.Second

type Config struct {
	DBAddr        string `yaml:"db_addr"`
	DBName        string `yaml:"db_name"`
	DataPointName string `yaml:"data_point_name" default:"worker_metrics"`
}

type exporter struct {
	cli       client.Client
	dbName    string
	pointName string
}

func (m *exporter) Close() {
	if m.cli != nil {
		m.cli.Close()
	}
}

func NewExporter(cfg *Config) (*exporter, error) {
	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:    cfg.DBAddr,
		Timeout: influxClientDefaultTimeout,
	})

	if err != nil {
		return nil, err
	}

	e := &exporter{
		cli:       cli,
		dbName:    cfg.DBName,
		pointName: cfg.DataPointName,
	}

	return e, nil
}

func (m *exporter) Write(worker common.Address, metrics map[string]float64, extra map[string]string) error {
	batch, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  m.dbName,
		Precision: "s",
	})
	if err != nil {
		return err
	}

	tags := map[string]string{"worker": worker.Hex()}
	fields := map[string]interface{}{}
	for k, v := range metrics {
		fields[k] = v
	}
	for k, v := range extra {
		fields[k] = v
	}

	point, err := client.NewPoint(m.pointName, tags, fields, time.Now())
	if err != nil {
		return err
	}

	batch.AddPoint(point)
	return m.cli.Write(batch)
}
