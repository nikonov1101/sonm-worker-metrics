package collector

type accumulatedMetrics map[string]float64

func (m accumulatedMetrics) add(name string, value float64) {
	current := m[name]
	m[name] = current + value
}

func (m accumulatedMetrics) calculatePercents() accumulatedMetrics {
	ok := m["SummaryHistogram"]
	m["TCPDirectPercent"] = m["UsingTCPDirectHistogram"] / ok * 100.
	m["NATPercent"] = m["UsingNATHistogram"] / ok * 100.
	m["QNATPercent"] = m["UsingQNATHistogram"] / ok * 100.
	m["RelayPercent"] = m["UsingRelayHistogram"] / ok * 100.
	m["FailedPercent"] = m["NumFailed"] / ok * 100.

	return m
}

func (m accumulatedMetrics) intoMapStringInterface() map[string]interface{} {
	x := map[string]interface{}{}
	for k, v := range m {
		x[k] = v
	}

	return x
}
