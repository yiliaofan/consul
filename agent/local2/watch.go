package local

import (
	"sync"
)

type watchKeyService string
type watchKeyCheck string
type watchKeyNode struct{}

type watches struct {
	lock  sync.Mutex
	chans map[interface{}][]chan struct{}
}

func (w *watches) Notify(k interface{}) {
	w.lock.Lock()
	defer w.lock.Unlock()

	for _, c := range w.chans[k] {
		close(c)
	}

	w.chans[k] = nil
}

func (w *watches) Watch(k interface{}, c chan struct{}) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.chans[k] = append(w.chans[k], c)
}
