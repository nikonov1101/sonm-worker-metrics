GO ?= go

HOSTOS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
HOSTARCH := $(shell uname -m)

GOOS ?= ${HOSTOS}
GOARCH ?= ${HOSTARCH}
OS_ARCH := $(GOOS)_$(GOARCH)$(EXE)

build:
	@echo "+ $@"
	${GO} build -tags "nocgo" -o target/collector_$(OS_ARCH) ./cmd/collector/
