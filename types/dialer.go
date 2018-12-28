package types

import "github.com/sonm-io/core/insonmnia/npp"

func DialerMetricsToMap(metrics map[string][]*npp.NamedMetric) map[string]interface{} {
	index := map[string]map[string]float64{}

	// extract values from particular metrics
	for addr, rows := range metrics {
		tmp := map[string]float64{}
		for _, row := range rows {
			if row.Metric.Counter != nil {
				tmp[row.Name] = row.Metric.Counter.GetValue()
			}

			if row.Metric.Histogram != nil {
				tmp[row.Name] = float64(row.Metric.GetHistogram().GetSampleCount())
			}

			if row.Metric.Gauge != nil {
				tmp[row.Name] = row.Metric.Gauge.GetValue()
			}
		}

		index[addr] = tmp
	}

	if len(index) == 0 {
		return map[string]interface{}{}
	}

	// calculate percents for each worker
	// for each metric
	for _, measurements := range index {
		ok := measurements["NumAttemptsRate01"]
		if ok < 1e-6 {
			continue
		}

		measurements["TCPDirectPercent"] = measurements["UsingTCPDirectHistogramRate01"] / ok * 100.
		measurements["NATPercent"] = measurements["UsingNATHistogramRate01"] / ok * 100.
		measurements["QNATPercent"] = measurements["UsingQNATHistogramRate01"] / ok * 100.
		measurements["RelayPercent"] = measurements["UsingRelayHistogramRate01"] / ok * 100.
		measurements["FailedPercent"] = measurements["NumFailedRate01"] / ok * 100.
		measurements["SuccessPercent"] = measurements["NumSuccessRate01"] / ok * 100.
	}

	result := map[string]float64{
		"TCPDirectPercent": 0.,
		"NATPercent":       0.,
		"QNATPercent":      0.,
		"RelayPercent":     0.,
		"FailedPercent":    0.,
		"SuccessPercent":   0,
	}

	// calculate average for each field
	ko := float64(len(index))
	for _, tmp := range index {
		result["TCPDirectPercent"] += tmp["TCPDirectPercent"] / ko
		result["NATPercent"] += tmp["NATPercent"] / ko
		result["QNATPercent"] += tmp["QNATPercent"] / ko
		result["RelayPercent"] += tmp["RelayPercent"] / ko
		result["FailedPercent"] += tmp["FailedPercent"] / ko
		result["SuccessPercent"] += tmp["SuccessPercent"] / ko
	}

	final := map[string]interface{}{
		"FailedPercent2": 100 - result["SuccessPercent"],
	}

	// make map of interfaces from map of floats.
	// Yes, re're still love golang.
	for k, v := range result {
		final[k] = v
	}

	return final

}
