package types

import (
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
)

func NewBasicTracer() opentracing.Tracer {
	opts := basictracer.DefaultOptions()
	opts.Recorder = newNoOpRecorder()
	opts.ShouldSample = func(traceID uint64) bool {
		return true
	}

	return basictracer.NewWithOptions(opts)
}

type noopRecorder struct{}

func newNoOpRecorder() *noopRecorder {
	return &noopRecorder{}
}

func (noopRecorder) RecordSpan(span basictracer.RawSpan) {}
