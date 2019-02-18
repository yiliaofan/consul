package local

import (
	"log"
	"sync"

	"github.com/hashicorp/consul/agent/checks"
	"github.com/hashicorp/consul/agent/config"
	"github.com/hashicorp/consul/agent/token"
)

type State struct {
	config             *config.RuntimeConfig
	delegate           rpc
	persistanceManager *persistanceManager
	dockerClient       *checks.DockerClient
	logger             *log.Logger
	// tokens contains the ACL tokens
	tokens *token.Store

	lock      sync.RWMutex
	current   *StateFrame
	watches   *watches
	needsSync map[interface{}]struct{}
}

func New(config *config.RuntimeConfig, delegate rpc, tokens *token.Store, logger *log.Logger) *State {
	return &State{
		config:             config,
		delegate:           delegate,
		tokens:             tokens,
		persistanceManager: &persistanceManager{},
		logger:             logger,
		current:            newFrame(),
		needsSync:          make(map[interface{}]struct{}),
		watches: &watches{
			chans: make(map[interface{}][]chan struct{}),
		},
	}
}

func (m *State) ReadTx() *ReadTx {
	m.lock.RLock()

	return &ReadTx{
		state: m.current,
		commit: func() {
			m.lock.RUnlock()
		},
	}
}

func (m *State) WriteTx() *WriteTx {
	m.lock.Lock()

	new := m.current.copy()
	return &WriteTx{
		manager: m,
		ReadTx: ReadTx{
			state: new,
		},
	}
}

func (m *State) SyncChanges() error {
	// TODO
	return nil
}

func (m *State) SyncFull() error {
	// TODO
	return nil
}

func (m *State) NotifyAll(c chan struct{}) {
	m.watches.Notify(watchKeyAll{}, c)
}

func (m *State) StopNotifyAll(c chan struct{}) {
	m.watches.StopNotify(watchKeyAll{}, c)
}

func (m *State) NotifyAnyProxy(c chan struct{}) {
	m.watches.Notify(watchKeyAnyProxy{}, c)
}

func (m *State) StopNotifyAnyProxy(c chan struct{}) {
	m.watches.StopNotify(watchKeyAnyProxy{}, c)
}
