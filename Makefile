GO ?= go

HOSTOS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
HOSTARCH := $(shell uname -m)

GOOS ?= ${HOSTOS}
GOARCH ?= ${HOSTARCH}
OS_ARCH := $(GOOS)_$(GOARCH)$(EXE)

run:
	${GO} run main.go

build:
	@echo "+ $@"
	${GO} build -tags "nocgo" -o target/exporter_$(OS_ARCH)
