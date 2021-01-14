package pipeline

import "fmt"

type ProcessFuncFactory interface {
	NewProcessFunc() ProcessFn
}

type ProcessFn interface {
	Process(in interface{}) interface{}
}

// Implementation
type IdentityProcessFuncFactory struct {
}

func NewIdentityProcessFuncFactory() ProcessFuncFactory {
	return IdentityProcessFuncFactory{}
}

func (pf IdentityProcessFuncFactory) NewProcessFunc() ProcessFn {
	return NewProcessFn()
}

type IdentityProcessFn struct {
	state int
}

func NewProcessFn() ProcessFn {
	return &IdentityProcessFn{}
}

func (p *IdentityProcessFn) Process(in interface{}) interface{} {
	msg := in.(map[string]string)
	msg["status"] = fmt.Sprint("processed-", p.state)
	p.state++
	return msg
}

/*


type ProcessFn struct {
	state int
}

func NewProcessFn() *ProcessFn {
	return &ProcessFn{}
}

func (p *ProcessFn) processFunction(in interface{}) interface{} {
	msg := in.(map[string]string)
	msg["status"] = fmt.Sprint("processed-", p.state)
	p.state++
	return msg
}
*/
