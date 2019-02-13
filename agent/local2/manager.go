package local

import "sync"

type Manager struct {
	lock      sync.RWMutex
	current   *State
	watches   *watches
	needsSync map[interface{}]struct{}
}

func (m *Manager) ReadTx() *ReadTx {
	m.lock.RLock()

	return &ReadTx{
		state: m.current,
		commit: func() {
			m.lock.RUnlock()
		},
	}
}

func (m *Manager) WriteTx() *WriteTx {
	m.lock.Lock()

	new := m.current.copy()
	return &WriteTx{
		ReadTx: ReadTx{
			state: new,
		},
		discard: func() {
			m.lock.Unlock()
		},
		commit: func(notifies []interface{}) {
			m.current = new

			for _, k := range notifies {
				m.watches.Notify(k)
				m.needsSync[k] = struct{}{}
			}

			m.lock.Unlock()
		},
	}
}
