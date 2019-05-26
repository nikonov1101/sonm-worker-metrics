package aggregator

import (
	"context"
	"encoding/json"
	"time"

	"github.com/influxdata/influxdb/client/v2"
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
	rows, err := m.influx.Read(`select * from worker_metrics where time > now() - 90s`)
	if err != nil {
		m.log.Warn("failed to run query", zap.Error(err))
		return
	}

	ctr := m.rowsToCounters(rows)
	m.log.Info("counters updated",
		zap.Int("by_addr", len(ctr.byAddr)),
		zap.Int("by_vers", len(ctr.byVersion)),
		zap.Int("by_location", len(ctr.byLocation)),
		zap.Int("by_err", ctr.byErr))

	if err := m.influx.WriteRaw("versions", nil, ctr.toVersion()); err != nil {
		m.log.Warn("failed to write `versions` measurement", zap.Error(err), zap.Any("values", ctr.toVersion()))
	}

	if err := m.influx.WriteRaw("locations", nil, ctr.toLocation()); err != nil {
		m.log.Warn("failed to write `locations` measurement", zap.Error(err), zap.Any("values", ctr.toLocation()))
	}

	if err := m.influx.WriteRaw("workers_count", nil, ctr.toWorkersCount()); err != nil {
		m.log.Warn("failed to write `workers_count` measurement", zap.Error(err), zap.Any("values", ctr.toWorkersCount()))
	}

	if err := m.influx.WriteRaw("err_count", nil, ctr.toErrorCount()); err != nil {
		m.log.Warn("failed to write `err_count` measurement", zap.Error(err), zap.Any("values", ctr.toErrorCount()))
	}
}

func (m *aggregator) rowsToCounters(rows []client.Result) *counters {
	uniqAddrs := map[string]*workerRow{}
	for _, row := range rows {
		if len(row.Series) == 0 {
			m.log.Warn("empty series found, please check wth is going on with influxdb")
			continue
		}

		ser := row.Series[0]
		var versionIdx, geoIdx, addrIdx, errIdx int

		// find array index for values to aggregate
		for i, v := range ser.Columns {
			switch v {
			case "worker":
				addrIdx = i
			case "version":
				versionIdx = i
			case "geo":
				geoIdx = i
			case "error":
				errIdx = i
			}
		}

		// extract values from data rows
		for x := range ser.Values {
			addr, ok := ser.Values[x][addrIdx].(string)
			vers, ok := ser.Values[x][versionIdx].(string)
			if !ok {
				vers = ""
			}
			geo, ok := ser.Values[x][geoIdx].(string)
			if !ok {
				geo = ""
			}

			// Damn it! I hate working with raw influx values.
			var errVal int64
			if errValJson, ok := ser.Values[x][errIdx].(json.Number); !ok {
				errVal = 1
			} else {
				if v, err := errValJson.Int64(); err != nil {
					errVal = 1
				} else {
					errVal = v
				}
			}

			uniqAddrs[addr] = &workerRow{
				addr:     addr,
				version:  vers,
				location: geo,
				err:      errVal > 0,
			}
		}
	}

	ctr := newCounters()
	for _, m := range uniqAddrs {
		ctr.addWorker(m)
	}

	return ctr
}
