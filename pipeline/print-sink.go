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
			case in := <-inStream:

				switch in.(type) {
				case BarrierEvent:
				case Message:
					if in.(Message).err != nil {
						fmt.Println("Print Error: ", in.(Message).err)
					} else {
						fmt.Println("Print Data: ", in.(Message).data)
					}

				default:
					//fmt.Println("Print Value: ", v)
				}

				select {
				case <-p.ctx.Done():
					return
				case outStream <- in:
				}
			}
		}

	}()

	return outStream
}
