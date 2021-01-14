package pipeline

import (
	"context"
)

type KeyByProcessFlow struct {
	//ctx         context.Context
	processFunc KeyedProcessFunc
	flow        *Flow
}

func NewKeyByProcessFlow() *KeyByProcessFlow {
	return &KeyByProcessFlow{}
}

func (k *KeyByProcessFlow) Initialize(ctx context.Context, flow *Flow) {
	k.flow = flow
}

func (k *KeyByProcessFlow) StartKeyByProcessFlow(ctx context.Context, inStream <-chan interface{}) <-chan interface{} {

	outStream := make(chan interface{})

	//fmt.Println("Starting StartKeyByProcessFlow", ctx)

	go func() {
		defer close(outStream)

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-inStream:
				//fmt.Println("StartKeyByProcessFlow before processing: ", msg)
				processedMsg := k.processFunc(msg)
				//fmt.Println("StartKeyByProcessFlow after processing: ", processedMsg)
				select {
				case <-ctx.Done():
					return
				case outStream <- processedMsg:
				}

			}
		}

	}()

	return outStream
}

type KeyedProcessFunc func(in interface{}) interface{}

func (k *KeyByProcessFlow) Process(processFunc KeyedProcessFunc) *Flow {
	k.processFunc = processFunc
	return k.flow
}
