package local

import "fmt"

type ReadTx struct {
	state *StateFrame

	commit func()
}

func (t *ReadTx) Done() {
	t.commit()
}

type WriteTx struct {
	ReadTx
	done bool

	manager *State

	notifies []interface{}
	onCommit []func()
}

func (t *WriteTx) Done() error {
	if t.done {
		return fmt.Errorf("use of discarded or done transaction")
	}

	for _, f := range t.onCommit {
		f()
	}

	t.manager.current = t.state
	t.done = true
	t.manager.lock.Unlock()

	for _, k := range t.notifies {
		t.manager.watches.Trigger(k)
		t.manager.needsSync[k] = struct{}{}
	}

	t.manager.watches.Trigger(watchKeyAll{})

	return nil
}

func (t *WriteTx) Discard() {
	if t.done {
		return
	}

	t.done = true
	t.manager.lock.Unlock()
}
