package pipeline

import (
	"context"
	"fmt"
)

type FlowStage struct {
	flowType         string
	flowLogic        interface{}
	keybyProcessFlow *KeyByProcessFlow
}

type Flow struct {
	srcStream <-chan interface{}
	ctx       context.Context
	flowOrder []FlowStage
	pipeline  *Pipleline
	sink      Sink
}

func NewFlow() *Flow {
	return &Flow{
		flowOrder: []FlowStage{},
	}

}

func (f *Flow) Initialize(ctx context.Context, pipeline *Pipleline, srcStream <-chan interface{}) {
	f.ctx = ctx
	f.srcStream = srcStream
	f.pipeline = pipeline
	f.pipeline.Sink(f.sink)
}

func (f *Flow) StartFlow() <-chan interface{} {

	currChan := f.srcStream

	//fmt.Println("Flow order length: ", len(f.flowOrder), f)

	// go through flow
	for _, flowStage := range f.flowOrder {

		switch flowStage.flowType {
		case "map":
			currChan = mapFlow(f.ctx, currChan, flowStage.flowLogic.(MapFunc))
		case "filter":
			currChan = filterFlow(f.ctx, currChan, flowStage.flowLogic.(FilterFunc))
		case "keyby":
			outChans := keyByFlow(f.ctx, currChan, flowStage.flowLogic.(KeyByFunc), flowStage.keybyProcessFlow, f)
			currChan = chansConverge(f.ctx, outChans)
		default:
			fmt.Println("Warn: unknown flow type")
		}

	}

	return currChan
}

func mapFlow(ctx context.Context, inStream <-chan interface{}, mapFunc MapFunc) <-chan interface{} {

	outStream := make(chan interface{})

	go func() {
		defer close(outStream)

		for {
			select {
			case <-ctx.Done():
				return
			case in := <-inStream:

				var out interface{}
				switch in.(type) {
				case BarrierEvent:
					out = in
				case Message:
					msg := in.(Message)
					if msg.err == nil {
						mOut, err := mapFunc(msg.data)
						if err != nil {
							msg.err = err
						} else {
							msg.data = mOut
						}
					}
					out = msg
				default:
					//out = mapFunc(in)
				}

				//fmt.Println("Output of MapFunc: ", out)
				select {
				case <-ctx.Done():
					return
				case outStream <- out:
				}
			}
		}

	}()

	return outStream

}

func filterFlow(ctx context.Context, inStream <-chan interface{}, filterFunc FilterFunc) <-chan interface{} {

	outStream := make(chan interface{})

	go func() {
		defer close(outStream)

		for {
			select {
			case <-ctx.Done():
				return
			case in := <-inStream:

				var ok bool
				switch in.(type) {
				case BarrierEvent:
					ok = true
				case Message:
					if in.(Message).err == nil {
						ok = filterFunc(in.(Message).data)
					} else {
						ok = true
					}
				default:
					//ok = filterFunc(in)
				}

				if ok {
					select {
					case <-ctx.Done():
						return
					case outStream <- in:
					}
				}

			}
		}

	}()

	return outStream

}

type MapFunc func(in interface{}) (interface{}, error)

func (f *Flow) Map(mapFunc MapFunc) *Flow {
	//fmt.Println("From Map method. setting floworder", f)
	f.flowOrder = append(f.flowOrder, FlowStage{"map", mapFunc, nil})
	//fmt.Println("Floworder length after setting map: ", len(f.flowOrder))
	return f
}

type FilterFunc func(in interface{}) bool

func (f *Flow) Filter(filterFunc FilterFunc) *Flow {
	f.flowOrder = append(f.flowOrder, FlowStage{"filter", filterFunc, nil})
	return f
}

func (f *Flow) Sink(sink Sink) {
	f.sink = sink
}

type KeyByFunc func(in interface{}) string

func (f *Flow) KeyBy(keybyFunc KeyByFunc) *KeyByProcessFlow {
	keybyProcessFlow := NewKeyByProcessFlow()
	keybyProcessFlow.Initialize(f.ctx, f)
	f.flowOrder = append(f.flowOrder, FlowStage{"keyby", keybyFunc, keybyProcessFlow})
	return keybyProcessFlow
}

func keyByFlow(ctx context.Context, inStream <-chan interface{}, keybyFunc KeyByFunc,
	keybyProcessFlow *KeyByProcessFlow, flow *Flow) <-chan (<-chan interface{}) {

	outChans := make(chan (<-chan interface{}))

	go func() {
		defer close(outChans)
		chanMap := make(map[string]chan interface{})
		passThroughCh := make(chan interface{})

		// plug passThroughCh channel to outChans
		select {
		case <-ctx.Done():
			return
		case outChans <- passThroughCh:
		}

		for {
			select {
			case <-ctx.Done():
				return
			case in := <-inStream:

				switch in.(type) {
				case BarrierEvent:
					eb := in.(BarrierEvent)

					if len(chanMap) > 0 {
						eb.chunkCount = len(chanMap)
						for _, outChn := range chanMap {
							select {
							case <-ctx.Done():
								return
								// inject barrier event into all keyby stream
							case outChn <- eb:
							}
						}
					} else {
						select {
						case <-ctx.Done():
							return
							// inject barrier event into all keyby stream
						case passThroughCh <- eb:
						}
					}

				case Message:
					msg := in.(Message)

					if msg.err != nil {
						fmt.Println("Received error message: ", msg.err)
						select {
						case <-ctx.Done():
							return
						case passThroughCh <- in:
							//fmt.Println("Sending ", in, " to KeyByProcessFlow")
						}
					} else {
						key := keybyFunc(msg.data)
						//fmt.Println("**Keyby: ", key)
						outChn, ok := chanMap[key]

						if !ok {
							//fmt.Println("Channel not found for key: ", key)
							outChn = make(chan interface{}, 1)
							chanMap[key] = outChn
							keybyChan := keybyProcessFlow.StartKeyByProcessFlow(ctx, outChn)

							select {
							case <-ctx.Done():
								return
							case outChans <- keybyChan:
							}
						} else {
							//fmt.Println("Channel found for key: ", key)
						}

						select {
						case <-ctx.Done():
							return
						case outChn <- in:
							//fmt.Println("Sending ", in, " to KeyByProcessFlow")
						}
					}

				default:
					/*
						key := keybyFunc(in)
						//fmt.Println("**Keyby: ", key)
						outChn, ok := chanMap[key]

						if !ok {
							//fmt.Println("Channel not found for key: ", key)
							outChn = make(chan interface{})
							chanMap[key] = outChn
							keybyChan := keybyProcessFlow.StartKeyByProcessFlow(ctx, outChn)

							select {
							case <-ctx.Done():
								return
							case outChans <- keybyChan:
							}
						} else {
							//fmt.Println("Channel found for key: ", key)
						}

						select {
						case <-ctx.Done():
							return
						case outChn <- in:
							//fmt.Println("Sending ", in, " to KeyByProcessFlow")
						}
					*/

				}

			}
		}

	}()

	return outChans

}

func chansConverge(ctx context.Context, inChans <-chan (<-chan interface{})) <-chan interface{} {

	outStream := make(chan interface{})

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case inCh := <-inChans:

				//fmt.Println("chansConverge - starting new goroutine for consuming new channel")
				go func(innerCtx context.Context, inStream <-chan interface{}) {
					for {
						select {
						case <-innerCtx.Done():
							return
						case msg := <-inStream:
							select {
							case <-innerCtx.Done():
								return
							case outStream <- msg:
							}
						}
					}
				}(ctx, inCh)
			}
		}
	}()

	return outStream
}
