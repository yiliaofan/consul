package local

import (
	"time"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/types"
)

type ServiceState struct {
	// Service is the local copy of the service record.
	Service *structs.NodeService

	// Token is the ACL to update or delete the service record on the
	// server.
	Token string

	// InSync contains whether the local state of the service record
	// is in sync with the remote state on the server.
	InSync bool

	// Deleted is true when the service record has been marked as deleted
	// but has not been removed on the server yet.
	Deleted bool

	// WatchCh is closed when the service state changes suitable for use in a
	// memdb.WatchSet when watching agent local changes with hash-based blocking.
	WatchCh chan struct{}
}

type CheckState struct {
	// Check is the local copy of the health check record.
	Check *structs.HealthCheck

	// Token is the ACL record to update or delete the health check
	// record on the server.
	Token string

	ReapAfter time.Duration

	// CriticalTime is the last time the health check status went
	// from non-critical to critical. When the health check is not
	// in critical state the value is the zero value.
	CriticalTime time.Time

	// DeferCheck is used to delay the sync of a health check when
	// only the output has changed. This rate limits changes which
	// do not affect the state of the node and/or service.
	DeferCheck *time.Timer

	// InSync contains whether the local state of the health check
	// record is in sync with the remote state on the server.
	InSync bool

	// Deleted is true when the health check record has been marked as
	// deleted but has not been removed on the server yet.
	Deleted bool
}

// Critical returns true when the health check is in critical state.
func (c *CheckState) Critical() bool {
	return !c.CriticalTime.IsZero()
}

// ManagedProxy represents the local state for a registered proxy instance.
type ManagedProxy struct {
	Proxy *structs.ConnectManagedProxy

	// ProxyToken is a special local-only security token that grants the bearer
	// access to the proxy's config as well as allowing it to request certificates
	// on behalf of the target service. Certain connect endpoints will validate
	// against this token and if it matches will then use the target service's
	// registration token to actually authenticate the upstream RPC on behalf of
	// the service. This token is passed securely to the proxy process via ENV
	// vars and should never be exposed any other way. Unmanaged proxies will
	// never see this and need to use service-scoped ACL tokens distributed
	// externally. It is persisted in the local state to allow authenticating
	// running proxies after the agent restarts.
	//
	// TODO(banks): In theory we only need to persist this at all to _validate_
	// which means we could keep only a hash in memory and on disk and only pass
	// the actual token to the process on startup. That would require a bit of
	// refactoring though to have the required interaction with the proxy manager.
	ProxyToken string

	// WatchCh is a close-only chan that is closed when the proxy is removed or
	// updated.
	WatchCh chan struct{}
}

type checkTask interface {
	Start()
	Stop()
}

type rpc interface {
	RPC(method string, args interface{}, reply interface{}) error
}

type StateFrame struct {
	// Services tracks the local services
	services map[string]*ServiceState

	// Checks tracks the local checks. checkAliases are aliased checks.
	checks       map[types.CheckID]*CheckState
	checkTasks   map[types.CheckID]checkTask
	checkAliases map[string]map[types.CheckID]chan<- struct{}
	// managedProxies is a map of all managed connect proxies registered locally on
	// this agent. This is NOT kept in sync with servers since it's agent-local
	// config only. Proxy instances have separate service registrations in the
	// services map above which are kept in sync via anti-entropy. Un-managed
	// proxies (that registered themselves separately from the service
	// registration) do not appear here as the agent doesn't need to manage their
	// process nor config. The _do_ still exist in services above though as
	// services with Kind == connect-proxy.
	//
	// managedProxyHandlers is a map of registered channel listeners that
	// are sent a message each time a proxy changes via Add or RemoveProxy.
	managedProxies map[string]*ManagedProxy

	// metadata tracks the node metadata fields
	metadata map[string]string
}

func newFrame() *StateFrame {
	return &StateFrame{
		services:       make(map[string]*ServiceState),
		checks:         make(map[types.CheckID]*CheckState),
		checkTasks:     make(map[types.CheckID]checkTask),
		checkAliases:   make(map[string]map[types.CheckID]chan<- struct{}),
		managedProxies: make(map[string]*ManagedProxy),
		metadata:       make(map[string]string),
	}
}

func (s *StateFrame) copy() *StateFrame {
	new := newFrame()

	for k, v := range s.services {
		new.services[k] = v
	}

	for k, v := range s.checks {
		new.checks[k] = v
	}

	for k, v := range s.checkTasks {
		new.checkTasks[k] = v
	}

	for k, v := range s.checkAliases {
		new.checkAliases[k] = v
	}

	for k, v := range s.managedProxies {
		new.managedProxies[k] = v
	}

	for k, v := range s.metadata {
		new.metadata[k] = v
	}

	return new
}
