package aggregator

const (
	emptyValue = "NONE"
)

type workerRow struct {
	addr     string
	version  string
	location string
}

func newWorkerRow(addr, ver, loc interface{}) *workerRow {
	var address, version, location string
	ok := false

	if address, ok = addr.(string); !ok {
		addr = ""
	}
	if version, ok = ver.(string); !ok {
		version = ""
	}
	if location, ok = loc.(string); !ok {
		location = ""
	}

	if len(version) == 0 {
		version = emptyValue
	}

	if len(location) == 0 {
		location = emptyValue
	}

	return &workerRow{
		addr:     address,
		version:  version,
		location: location,
	}
}

type counters struct {
	byAddr     map[string]struct{}
	byLocation map[string]int
	byVersion  map[string]int
}

func newCounters() *counters {
	return &counters{
		byAddr:     make(map[string]struct{}),
		byLocation: make(map[string]int),
		byVersion:  make(map[string]int),
	}
}

func (m *counters) addWorker(w *workerRow) {
	_, ok := m.byAddr[w.addr]
	if ok {
		// if we already have such worker in the cache
		// just skip the record, because we're inserting
		// records in reversed time order.
		// Thus duplicate record is outdated in that case.
		return
	}

	// remember this worker
	m.byAddr[w.addr] = struct{}{}

	// count workers by location
	v, ok := m.byLocation[w.location]
	if ok {
		m.byLocation[w.location] = v + 1
	} else {
		m.byLocation[w.location] = 1
	}

	// count workers by versions
	v, ok = m.byVersion[w.version]
	if ok {
		m.byVersion[w.version] = v + 1
	} else {
		m.byVersion[w.version] = 1
	}
}

func (m *counters) toWorkersCount() map[string]interface{} {
	return map[string]interface{}{"count": len(m.byAddr)}
}

func (m *counters) toVersion() map[string]interface{} {
	return toMapStringInterface(m.byVersion)
}

func (m *counters) toLocation() map[string]interface{} {
	return toMapStringInterface(m.byLocation)
}

func toMapStringInterface(m map[string]int) map[string]interface{} {
	r := make(map[string]interface{})
	for k, v := range m {
		r[k] = v
	}
	return r
}
