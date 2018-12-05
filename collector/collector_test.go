package collector

import (
	"testing"

	"github.com/sonm-io/core/proto"
	"github.com/stretchr/testify/assert"
)

func TestCalculatePercent(t *testing.T) {
	ld := &metricsLoader{}

	input := map[string]float64{
		sonm.MetricsKeyDiskTotal: 1000,
		sonm.MetricsKeyDiskFree:  400,
		sonm.MetricsKeyRAMTotal:  500,
		sonm.MetricsKeyRAMFree:   300,
	}

	output := ld.addPercentFields(input)
	assert.Equal(t, 60., output[sonm.MetricsKeyDiskFreePercent])
	assert.Equal(t, 40., output[sonm.MetricsKeyRAMFreePercent])
}
