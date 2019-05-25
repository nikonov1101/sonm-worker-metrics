package aggregator

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	"github.com/sonm-io/core/util"
	"github.com/sonm-io/monitoring/influx"
	"go.uber.org/zap"
)

type aggregator struct {
	log    *zap.Logger
	influx *influx.Influx
}

func NewAggregator(log *zap.Logger, inf *influx.Influx) *aggregator {
	return &aggregator{log: log.Named("aggrg"), influx: inf}
}

func (m *aggregator) Run(ctx context.Context) {
	tk := util.NewImmediateTicker(3 * time.Minute)
	defer tk.Stop()

	m.log.Info("aggregator started")
	defer m.log.Info("aggregator stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			m.runOnce()
		}
	}
}

func (m *aggregator) runOnce() {
	rows, err := m.influx.Read(`select * from worker_metrics where error = 0 and time > now() - 90s`)
	if err != nil {
		m.log.Warn("failed to run query", zap.Error(err))
		return
	}

	ctr := m.rowsToCounters(rows)
	m.log.Info("counters updated",
		zap.Int("by_addr", len(ctr.byAddr)),
		zap.Int("by_vers", len(ctr.byVersion)),
		zap.Int("by_location", len(ctr.byLocation)))

	if err := m.influx.WriteRaw("versions", nil, ctr.toVersion()); err != nil {
		m.log.Warn("failed to write versions measurement", zap.Error(err), zap.Any("values", ctr.toVersion()))
	}

	if err := m.influx.WriteRaw("locations", nil, ctr.toLocation()); err != nil {
		m.log.Warn("failed to write locations measurement", zap.Error(err), zap.Any("values", ctr.toLocation()))
	}

	if err := m.influx.WriteRaw("workers_count", nil, ctr.toWorkersCount()); err != nil {
		m.log.Warn("failed to write count measurement", zap.Error(err), zap.Any("values", ctr.toWorkersCount()))
	}
}

func (m *aggregator) rowsToCounters(rows []client.Result) *counters {
	ctr := newCounters()
	for _, row := range rows {
		if len(row.Series) == 0 {
			m.log.Warn("empty series found, please check wth is going on with influxdb")
			continue
		}

		workers := m.processRow(row.Series[0])
		for _, worker := range workers {
			ctr.addWorker(worker)
		}
	}

	return ctr
}

func (m *aggregator) processRow(row models.Row) []*workerRow {
	versionIdx := 0
	geoIdx := 0
	addrIdx := 0

	// find array index for values to aggregate
	for i, v := range row.Columns {
		if v == "version" {
			versionIdx = i
		}
		if v == "geo" {
			geoIdx = i
		}
		if v == "worker" {
			addrIdx = i
		}
	}

	// extract values from data rows
	var workers []*workerRow
	for x := range row.Values {
		addr := row.Values[x][addrIdx]
		geo := row.Values[x][geoIdx]
		ver := row.Values[x][versionIdx]
		workers = append(workers, newWorkerRow(addr, ver, geo))
	}

	return workers
}
