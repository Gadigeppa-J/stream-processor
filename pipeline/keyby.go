package pipeline

import (
	"context"
)

type KeyByProcessFlow struct {
	//ctx         context.Context
	processFnFactory ProcessFuncFactory
	flow             *Flow
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
		var processFn ProcessFn
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-inStream:
				//fmt.Println("StartKeyByProcessFlow before processing: ", msg)
				if processFn == nil {
					processFn = k.processFnFactory.NewProcessFunc()
				}
				processedMsg := processFn.Process(msg)
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

func (k *KeyByProcessFlow) Process(processFnFactory ProcessFuncFactory) *Flow {
	k.processFnFactory = processFnFactory
	return k.flow
}
