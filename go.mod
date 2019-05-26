module github.com/sonm-io/monitoring

go 1.12

replace github.com/Sirupsen/logrus v1.1.0 => github.com/sirupsen/logrus v1.1.0

replace github.com/libp2p/go-reuseport v0.1.10 => github.com/libp2p/go-reuseport v0.0.0-20180201025315-5f99154da15f

require (
	github.com/allegro/bigcache v1.2.0 // indirect
	github.com/blang/semver v3.5.1+incompatible
	github.com/ethereum/go-ethereum v0.0.0-20180929205331-b69942befeb9
	github.com/influxdata/influxdb v1.7.6
	github.com/jinzhu/configor v1.0.0
	github.com/opentracing/basictracer-go v1.0.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/sonm-io/core v0.4.28
	go.uber.org/zap v1.10.0
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	google.golang.org/grpc v1.21.0
)
