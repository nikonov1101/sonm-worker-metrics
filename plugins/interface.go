package plugins

import "context"

type Plugin interface {
	Run(ctx context.Context)
}
