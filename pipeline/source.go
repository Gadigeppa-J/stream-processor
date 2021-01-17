package pipeline

import "context"

type Source interface {
	Initialize(ctx context.Context)
	StartStream() <-chan interface{}
	ConsumeSinkStream(inStream <-chan interface{})
}
