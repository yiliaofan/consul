package local

import (
	"sync"
)

type watchKeyAll struct{}
type watchKeyService string
type watchKeyCheck string
type watchKeyNode struct{}
type watchKeyAnyProxy struct{}

type watches struct {
	lock  sync.Mutex
	chans map[interface{}][]chan struct{}
}

func (w *watches) Trigger(k interface{}) {
	w.lock.Lock()
	defer w.lock.Unlock()

	for _, c := range w.chans[k] {
		close(c)
	}

	w.chans[k] = nil
}

func (w *watches) Notify(k interface{}, c chan struct{}) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.chans[k] = append(w.chans[k], c)
}

func (w *watches) StopNotify(k interface{}, c chan struct{}) {
	w.lock.Lock()
	defer w.lock.Unlock()

	watches := w.chans[k]

	for i, ec := range watches {
		if ec != c {
			continue
		}

		watches[i] = watches[len(watches)-1]
		watches = watches[:len(watches)-1]
	}

	w.chans[k] = watches
}
