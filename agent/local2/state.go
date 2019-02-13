package local

import (
	"time"

	"github.com/hashicorp/consul/agent/checks"
	"github.com/hashicorp/consul/agent/config"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/agent/token"
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

type checkTask interface {
	Start()
	Stop()
}

type rpc interface {
	RPC(method string, args interface{}, reply interface{}) error
}

type State struct {
	config *config.RuntimeConfig
	// Services tracks the local services
	services map[string]*ServiceState

	// Checks tracks the local checks. checkAliases are aliased checks.
	checks       map[types.CheckID]*CheckState
	checkTasks   map[types.CheckID]checkTask
	checkAliases map[string]map[types.CheckID]chan<- struct{}

	// checkReapAfter maps the check ID to a timeout after which we should
	// reap its associated service
	checkReapAfter map[types.CheckID]time.Duration

	// metadata tracks the node metadata fields
	metadata map[string]string

	dockerClient *checks.DockerClient

	// tokens contains the ACL tokens
	tokens *token.Store

	delegate rpc

	persistanceManager *persistanceManager
}

func (s *State) copy() *State {
	new := &State{
		services:     make(map[string]*ServiceState),
		checks:       make(map[types.CheckID]*CheckState),
		checkAliases: make(map[string]map[types.CheckID]chan<- struct{}),
		metadata:     make(map[string]string),
		dockerClient: s.dockerClient,
		delegate:     s.delegate,
	}

	for k, v := range s.services {
		new.services[k] = v
	}

	for k, v := range s.checks {
		new.checks[k] = v
	}

	for k, v := range s.checkAliases {
		new.checkAliases[k] = v
	}

	for k, v := range s.metadata {
		new.metadata[k] = v
	}

	return new
}
