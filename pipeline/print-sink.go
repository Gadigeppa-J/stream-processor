package pipeline

import (
	"context"
	"fmt"
)

type PrintSink struct {
	ctx context.Context
}

func NewPrintSink() Sink {
	return &PrintSink{}
}

func (p *PrintSink) Initialize(ctx context.Context) {
	p.ctx = ctx
}

func (p *PrintSink) StartSink(inStream <-chan interface{}) <-chan interface{} {

	outStream := make(chan interface{})

	go func() {
		defer close(outStream)

		for {
			select {
			case <-p.ctx.Done():
				return
			case v := <-inStream:
				fmt.Println("Print Value: ", v)
				select {
				case <-p.ctx.Done():
					return
				case outStream <- v:
				}
			}
		}

	}()

	return outStream
}
