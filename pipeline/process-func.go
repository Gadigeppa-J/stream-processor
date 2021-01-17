package pipeline

import (
	"fmt"
	"math/rand"
	"time"
)

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

	r := rand.Intn(120)

	orgID, ok := msg["orgID"]

	if ok {
		fmt.Println("Waiting for :", r, " orgID: ", orgID)
	} else {
		fmt.Println("Waiting for :", r)
	}

	time.Sleep(time.Second * time.Duration(r))

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
