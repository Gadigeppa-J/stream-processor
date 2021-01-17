package pipeline

import (
	"context"
	"log"
	"sync"
)

type Pipleline struct {
	source Source
	flow   *Flow
	sink   Sink
}

func NewPipeline() *Pipleline {
	return &Pipleline{}
}

func (p *Pipleline) Source(src Source) *Flow {
	p.source = src
	p.flow = NewFlow()
	return p.flow
}

func (p *Pipleline) Sink(sink Sink) {

	//fmt.Println(p)

	p.sink = sink
}

func (p *Pipleline) Execute() {

	var wg sync.WaitGroup
	ctx, _ := context.WithCancel(context.Background())

	wg.Add(1)
	// check if source is set
	if p.source == nil {
		log.Fatal("Source is not set!")
	}
	// Initialize source
	p.source.Initialize(ctx)
	// Start Source Stream
	srcStream := p.source.StartStream()

	// check if flow is set
	if p.flow == nil {
		log.Fatal("Flow is not set!")
	}
	p.flow.Initialize(ctx, p, srcStream)
	flowStream := p.flow.StartFlow()

	// check if sink is set
	if p.sink == nil {
		log.Fatal("Sink is not set!")
	}

	p.sink.Initialize(ctx)
	sinkStream := p.sink.StartSink(flowStream)

	/*
		for {
			select {
			case <-ctx.Done():
				return
			case <-sinkStream:
			}
		}
	*/

	p.source.ConsumeSinkStream(sinkStream)
	wg.Wait()

}
