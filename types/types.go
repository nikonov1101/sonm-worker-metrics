package types

type AccumulatedMetrics map[string]float64

func (m AccumulatedMetrics) Insert(name string, value float64) {
	current := m[name]
	m[name] = current + value
}

func (m AccumulatedMetrics) Unwrap() map[string]interface{} {
	x := map[string]interface{}{}
	for k, v := range m {
		x[k] = v
	}

	return x
}
