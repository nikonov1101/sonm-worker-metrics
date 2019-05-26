package aggregator

const (
	emptyValue = "NONE"
)

type workerRow struct {
	addr     string
	version  string
	location string
	err      bool
}

type counters struct {
	byAddr     map[string]struct{}
	byLocation map[string]int
	byVersion  map[string]int
	byErr      int
}

func newCounters() *counters {
	return &counters{
		byAddr:     make(map[string]struct{}),
		byLocation: make(map[string]int),
		byVersion:  make(map[string]int),
		byErr:      0,
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

	loc := w.location
	if len(loc) == 0 {
		loc = emptyValue
	}

	ver := w.version
	if len(ver) == 0 {
		ver = emptyValue
	}

	// calculate connectivity errors
	if w.err {
		m.byErr++
	}

	// count workers by location
	v, ok := m.byLocation[loc]
	if ok {
		m.byLocation[loc] = v + 1
	} else {
		m.byLocation[loc] = 1
	}

	// count workers by versions
	v, ok = m.byVersion[ver]
	if ok {
		m.byVersion[ver] = v + 1
	} else {
		m.byVersion[ver] = 1
	}
}

func (m *counters) toWorkersCount() map[string]interface{} {
	return map[string]interface{}{"count": len(m.byAddr)}
}

func (m *counters) toErrorCount() map[string]interface{} {
	return map[string]interface{}{"count": m.byErr}
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
