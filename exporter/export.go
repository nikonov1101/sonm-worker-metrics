package exporter

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/influxdata/influxdb/client/v2"
)

const influxClientDefaultTimeout = 10 * time.Second

type Config struct {
	DBAddr string `yaml:"db_addr"`
	DBName string `yaml:"db_name"`
}

type Exporter struct {
	cli    client.Client
	dbName string
}

func (m *Exporter) Close() {
	if m.cli != nil {
		m.cli.Close()
	}
}

func NewExporter(cfg *Config) (*Exporter, error) {
	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:    cfg.DBAddr,
		Timeout: influxClientDefaultTimeout,
	})

	if err != nil {
		return nil, err
	}

	e := &Exporter{
		cli:    cli,
		dbName: cfg.DBName,
	}

	return e, nil
}

func (m *Exporter) Write(pointName string, worker common.Address, metrics map[string]float64, extra map[string]string) error {
	tags := map[string]string{"worker": worker.Hex()}
	fields := map[string]interface{}{}

	for k, v := range metrics {
		fields[k] = v
	}
	for k, v := range extra {
		fields[k] = v
	}

	return m.WriteRaw(pointName, tags, fields)
}

func (m *Exporter) WriteRaw(pointName string, tags map[string]string, values map[string]interface{}) error {
	batch, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  m.dbName,
		Precision: "s",
	})

	if err != nil {
		return err
	}

	point, err := client.NewPoint(pointName, tags, values, time.Now())
	if err != nil {
		return err
	}

	batch.AddPoint(point)
	return m.cli.Write(batch)
}

func (m *Exporter) Read(cmd string) ([]client.Result, error) {
	q := client.Query{
		Command:  cmd,
		Database: m.dbName,
	}

	response, err := m.cli.Query(q)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %v", err)
	}

	if response.Error() != nil {
		return nil, fmt.Errorf("query failed with the following error: %v", response.Error())
	}

	return response.Results, nil
}
