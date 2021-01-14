package pipeline

import "context"

type Sink interface {
	Initialize(ctx context.Context)
	StartSink(inStream <-chan interface{}) <-chan interface{}
}
