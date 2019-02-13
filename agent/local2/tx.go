package local

import (
	"log"

	"github.com/hashicorp/consul/agent/structs"
)

type ReadTx struct {
	state *State

	commit func()
}

func (t *ReadTx) Done() {
	t.commit()
}

func (t *ReadTx) Service(id string) *structs.NodeService {
	state, ok := t.state.services[id]
	if !ok {
		return nil
	}
	return state.Service
}

type WriteTx struct {
	ReadTx
	done   bool
	logger *log.Logger

	commit   func(notifies []interface{})
	discard  func()
	notifies []interface{}
	onCommit []func()

	newTx func() *WriteTx
}

func (t *WriteTx) Done() {
	t.commit(t.notifies)
	for _, f := range
	t.done = true
}

func (t *WriteTx) Discard() {
	if t.done {
		return
	}
	t.discard()
	t.done = true
}

func (t *WriteTx) SetService(srv *structs.NodeService) {
	t.state.services[srv.ID] = &ServiceState{
		Service: srv,
	}

	t.notifies = append(t.notifies, watchKeyService(srv.ID))
}
