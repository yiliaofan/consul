package local

import (
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/armon/go-metrics"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/agent/token"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/consul/types"
)

// Config is the configuration for the State.
type Config struct {
	AdvertiseAddr       string
	CheckUpdateInterval time.Duration
	Datacenter          string
	DiscardCheckOutput  bool
	NodeID              types.NodeID
	NodeName            string
	TaggedAddresses     map[string]string
	ProxyBindMinPort    int
	ProxyBindMaxPort    int
}

type rpc interface {
	RPC(method string, args interface{}, reply interface{}) error
}

// State is used to represent the node's services,
// and checks. We use it to perform anti-entropy with the
// catalog representation
type State struct {
	sync.RWMutex

	// Delegate the RPC interface to the consul server or agent.
	//
	// It is set after both the state and the consul server/agent have
	// been created.
	Delegate rpc

	// TriggerSyncChanges is used to notify the state syncer that a
	// partial sync should be performed.
	//
	// It is set after both the state and the state syncer have been
	// created.
	TriggerSyncChanges func()

	logger *log.Logger

	// Config is the agent config
	config Config

	data *stateData

	// nodeInfoInSync tracks whether the server has our correct top-level
	// node information in sync
	nodeInfoInSync bool

	// discardCheckOutput stores whether the output of health checks
	// is stored in the raft log.
	discardCheckOutput atomic.Value // bool

	// tokens contains the ACL tokens
	tokens *token.Store

	// notifyHandlers is a map of registered channel listeners that are sent
	// messages whenever state changes occur. For now these events only include
	// service registration and deregistration since that is all that is needed
	// but the same mechanism could be used for other state changes.
	//
	// Note that we haven't refactored managedProxyHandlers into this mechanism
	// yet because that is soon to be deprecated and removed so it's easier to
	// just leave them separate until managed proxies are removed entirely. Any
	// future notifications should re-use this mechanism though.
	notifyHandlers map[chan<- struct{}]struct{}

	// managedProxyHandlers is a map of registered channel listeners that
	// are sent a message each time a proxy changes via Add or RemoveProxy.
	managedProxyHandlers map[chan<- struct{}]struct{}
}

// NewState creates a new local state for the agent.
func NewState(c Config, lg *log.Logger, tokens *token.Store) *State {
	l := &State{
		config:               c,
		logger:               lg,
		tokens:               tokens,
		notifyHandlers:       make(map[chan<- struct{}]struct{}),
		managedProxyHandlers: make(map[chan<- struct{}]struct{}),
	}
	l.SetDiscardCheckOutput(c.DiscardCheckOutput)
	return l
}

// SetDiscardCheckOutput configures whether the check output
// is discarded. This can be changed at runtime.
func (l *State) SetDiscardCheckOutput(b bool) {
	l.discardCheckOutput.Store(b)
}

// ServiceToken returns the configured ACL token for the given
// service ID. If none is present, the agent's token is returned.
func (l *State) ServiceToken(id string) string {
	l.RLock()
	defer l.RUnlock()
	return l.serviceToken(id)
}

// serviceToken returns an ACL token associated with a service.
// This method is not synchronized and the lock must already be held.
func (l *State) serviceToken(id string) string {
	var token string
	if s := l.data.services[id]; s.Service != nil {
		token = s.Token
	}
	if token == "" {
		token = l.tokens.UserToken()
	}
	return token
}

// Service returns the locally registered service that the
// agent is aware of and are being kept in sync with the server
func (l *State) Service(id string) *structs.NodeService {
	l.RLock()
	defer l.RUnlock()

	s := l.data.services[id]
	if s.Service == nil || s.Deleted {
		return nil
	}
	return s.Service
}

// Services returns the locally registered services that the
// agent is aware of and are being kept in sync with the server
func (l *State) Services() map[string]*structs.NodeService {
	l.RLock()
	defer l.RUnlock()

	m := make(map[string]*structs.NodeService)
	for id, s := range l.data.services {
		if s.Deleted {
			continue
		}
		m[id] = s.Service
	}
	return m
}

// UpdateCheck is used to update the status of a check
func (l *State) UpdateCheck(id types.CheckID, status, output string) {
	l.Lock()
	defer l.Unlock()

	c := l.checks[id]
	if c == nil || c.Deleted {
		return
	}

	if l.discardCheckOutput.Load().(bool) {
		output = ""
	}

	// Update the critical time tracking (this doesn't cause a server updates
	// so we can always keep this up to date).
	if status == api.HealthCritical {
		if !c.Critical() {
			c.CriticalTime = time.Now()
		}
	} else {
		c.CriticalTime = time.Time{}
	}

	// Do nothing if update is idempotent
	if c.Check.Status == status && c.Check.Output == output {
		return
	}

	// Defer a sync if the output has changed. This is an optimization around
	// frequent updates of output. Instead, we update the output internally,
	// and periodically do a write-back to the servers. If there is a status
	// change we do the write immediately.
	if l.config.CheckUpdateInterval > 0 && c.Check.Status == status {
		c.Check.Output = output
		if c.DeferCheck == nil {
			d := l.config.CheckUpdateInterval
			intv := time.Duration(uint64(d)/2) + lib.RandomStagger(d)
			c.DeferCheck = time.AfterFunc(intv, func() {
				l.Lock()
				defer l.Unlock()

				c := l.checks[id]
				if c == nil {
					return
				}
				c.DeferCheck = nil
				if c.Deleted {
					return
				}
				c.InSync = false
				l.TriggerSyncChanges()
			})
		}
		return
	}

	// If this is a check for an aliased service, then notify the waiters.
	if aliases, ok := l.checkAliases[c.Check.ServiceID]; ok && len(aliases) > 0 {
		for _, notifyCh := range aliases {
			// Do not block. All notify channels should be buffered to at
			// least 1 in which case not-blocking does not result in loss
			// of data because a failed send means a notification is
			// already queued. This must be called with the lock held.
			select {
			case notifyCh <- struct{}{}:
			default:
			}
		}
	}

	// Update status and mark out of sync
	c.Check.Status = status
	c.Check.Output = output
	c.InSync = false
	l.TriggerSyncChanges()
}

// Check returns the locally registered check that the
// agent is aware of and are being kept in sync with the server
func (l *State) Check(id types.CheckID) *structs.HealthCheck {
	l.RLock()
	defer l.RUnlock()

	c := l.data.checks[id]
	if c.Deleted {
		return nil
	}
	return c.Check
}

// Checks returns the locally registered checks that the
// agent is aware of and are being kept in sync with the server
func (l *State) Checks() map[types.CheckID]*structs.HealthCheck {
	m := make(map[types.CheckID]*structs.HealthCheck)
	for id, c := range l.CheckStates() {
		m[id] = c.Check
	}
	return m
}

// CheckState returns a shallow copy of the current health check state
// record. The health check record and the deferred check still point to
// the original values and must not be modified.
func (l *State) CheckState(id types.CheckID) *CheckState {
	l.RLock()
	defer l.RUnlock()

	c := l.data.checks[id]
	if c.Deleted {
		return nil
	}
	return c.Clone()
}

// SetCheckState is used to overwrite a raw check state with the given
// state. This method is safe to be called concurrently but should only be used
// during testing. You should most likely call AddCheck instead.
func (l *State) SetCheckState(c CheckState) {
	l.Lock()
	defer l.Unlock()

	l.data.checks[c.Check.CheckID] = c
	l.TriggerSyncChanges()
}

// CheckStates returns a shallow copy of all health check state records.
// The health check records and the deferred checks still point to
// the original values and must not be modified.
func (l *State) CheckStates() map[types.CheckID]*CheckState {
	l.RLock()
	defer l.RUnlock()

	m := make(map[types.CheckID]*CheckState)
	for id, c := range l.data.checks {
		if c.Deleted {
			continue
		}
		m[id] = c.Clone()
	}
	return m
}

// CriticalCheckStates returns the locally registered checks that the
// agent is aware of and are being kept in sync with the server.
// The map contains a shallow copy of the current check states but
// references to the actual check definition which must not be
// modified.
func (l *State) CriticalCheckStates() map[types.CheckID]*CheckState {
	l.RLock()
	defer l.RUnlock()

	m := make(map[types.CheckID]*CheckState)
	for id, c := range l.data.checks {
		if c.Deleted || !c.Critical() {
			continue
		}
		m[id] = c.Clone()
	}
	return m
}

// Proxy returns the local proxy state.
func (l *State) Proxy(id string) *ManagedProxy {
	l.RLock()
	defer l.RUnlock()
	return l.data.managedProxies[id]
}

// Proxies returns the locally registered proxies.
func (l *State) Proxies() map[string]*ManagedProxy {
	l.RLock()
	defer l.RUnlock()

	m := make(map[string]*ManagedProxy)
	for id, p := range l.data.managedProxies {
		m[id] = p
	}
	return m
}

// broadcastUpdateLocked assumes l is locked and delivers an update to all
// registered watchers.
func (l *State) broadcastUpdateLocked() {
	for ch := range l.notifyHandlers {
		// Do not block
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Notify will register a channel to receive messages when the local state
// changes. Only service add/remove are supported for now. See notes on
// l.notifyHandlers for more details.
//
// This will not block on channel send so ensure the channel has a buffer. Note
// that any buffer size is generally fine since actual data is not sent over the
// channel, so a dropped send due to a full buffer does not result in any loss
// of data. The fact that a buffer already contains a notification means that
// the receiver will still be notified that changes occurred.
func (l *State) Notify(ch chan<- struct{}) {
	l.Lock()
	defer l.Unlock()
	l.notifyHandlers[ch] = struct{}{}
}

// StopNotify will deregister a channel receiving state change notifications.
// Pair this with all calls to Notify to clean up state.
func (l *State) StopNotify(ch chan<- struct{}) {
	l.Lock()
	defer l.Unlock()
	delete(l.notifyHandlers, ch)
}

// NotifyProxy will register a channel to receive messages when the
// configuration or set of proxies changes. This will not block on
// channel send so ensure the channel has a buffer. Note that any buffer
// size is generally fine since actual data is not sent over the channel,
// so a dropped send due to a full buffer does not result in any loss of
// data. The fact that a buffer already contains a notification means that
// the receiver will still be notified that changes occurred.
//
// NOTE(mitchellh): This could be more generalized but for my use case I
// only needed proxy events. In the future if it were to be generalized I
// would add a new Notify method and remove the proxy-specific ones.
func (l *State) NotifyProxy(ch chan<- struct{}) {
	l.Lock()
	defer l.Unlock()
	l.managedProxyHandlers[ch] = struct{}{}
}

// StopNotifyProxy will deregister a channel receiving proxy notifications.
// Pair this with all calls to NotifyProxy to clean up state.
func (l *State) StopNotifyProxy(ch chan<- struct{}) {
	l.Lock()
	defer l.Unlock()
	delete(l.managedProxyHandlers, ch)
}

// Metadata returns the local node metadata fields that the
// agent is aware of and are being kept in sync with the server
func (l *State) Metadata() map[string]string {
	l.RLock()
	defer l.RUnlock()

	m := make(map[string]string)
	for k, v := range l.data.metadata {
		m[k] = v
	}
	return m
}

// Stats is used to get various debugging state from the sub-systems
func (l *State) Stats() map[string]string {
	l.RLock()
	defer l.RUnlock()

	services := 0
	for _, s := range l.data.services {
		if s.Deleted {
			continue
		}
		services++
	}

	checks := 0
	for _, c := range l.data.checks {
		if c.Deleted {
			continue
		}
		checks++
	}

	return map[string]string{
		"services": strconv.Itoa(services),
		"checks":   strconv.Itoa(checks),
	}
}

// updateSyncState does a read of the server state, and updates
// the local sync status as appropriate
func (l *State) updateSyncState() error {
	// Get all checks and services from the master
	req := structs.NodeSpecificRequest{
		Datacenter:   l.config.Datacenter,
		Node:         l.config.NodeName,
		QueryOptions: structs.QueryOptions{Token: l.tokens.AgentToken()},
	}

	var out1 structs.IndexedNodeServices
	if err := l.Delegate.RPC("Catalog.NodeServices", &req, &out1); err != nil {
		return err
	}

	var out2 structs.IndexedHealthChecks
	if err := l.Delegate.RPC("Health.NodeChecks", &req, &out2); err != nil {
		return err
	}

	// Create useful data structures for traversal
	remoteServices := make(map[string]*structs.NodeService)
	if out1.NodeServices != nil {
		remoteServices = out1.NodeServices.Services
	}

	remoteChecks := make(map[types.CheckID]*structs.HealthCheck, len(out2.HealthChecks))
	for _, rc := range out2.HealthChecks {
		remoteChecks[rc.CheckID] = rc
	}

	// Traverse all checks, services and the node info to determine
	// which entries need to be updated on or removed from the server

	l.Lock()
	defer l.Unlock()

	// Check if node info needs syncing
	if out1.NodeServices == nil || out1.NodeServices.Node == nil ||
		out1.NodeServices.Node.ID != l.config.NodeID ||
		!reflect.DeepEqual(out1.NodeServices.Node.TaggedAddresses, l.config.TaggedAddresses) ||
		!reflect.DeepEqual(out1.NodeServices.Node.Meta, l.data.metadata) {
		l.nodeInfoInSync = false
	}

	// Check which services need syncing

	// Look for local services that do not exist remotely and mark them for
	// syncing so that they will be pushed to the server later
	for id, s := range l.data.services {
		if remoteServices[id] == nil {
			s.InSync = false
		}
	}

	// Traverse the list of services from the server.
	// Remote services which do not exist locally have been deregistered.
	// Otherwise, check whether the two definitions are still in sync.
	for id, rs := range remoteServices {
		ls, ok := l.data.services[id]
		if !ok {
			// The consul service is managed automatically and does
			// not need to be deregistered
			if id == structs.ConsulServiceID {
				continue
			}

			// Mark a remote service that does not exist locally as deleted so
			// that it will be removed on the server later.
			l.data.services[id] = ServiceState{Deleted: true}
			continue
		}

		// If the service is already scheduled for removal skip it
		if ls.Deleted {
			continue
		}

		// If our definition is different, we need to update it. Make a
		// copy so that we don't retain a pointer to any actual state
		// store info for in-memory RPCs.
		if ls.Service.EnableTagOverride {
			ls.Service.Tags = make([]string, len(rs.Tags))
			copy(ls.Service.Tags, rs.Tags)
		}
		ls.InSync = ls.Service.IsSame(rs)
	}

	// Check which checks need syncing

	// Look for local checks that do not exist remotely and mark them for
	// syncing so that they will be pushed to the server later
	for id, c := range l.data.checks {
		if remoteChecks[id] == nil {
			c.InSync = false
		}
	}

	// Traverse the list of checks from the server.
	// Remote checks which do not exist locally have been deregistered.
	// Otherwise, check whether the two definitions are still in sync.
	for id, rc := range remoteChecks {
		lc, ok := l.data.checks[id]

		if !ok {
			// The Serf check is created automatically and does not
			// need to be deregistered.
			if id == structs.SerfCheckID {
				l.logger.Printf("[DEBUG] agent: Skipping remote check %q since it is managed automatically", id)
				continue
			}

			// Mark a remote check that does not exist locally as deleted so
			// that it will be removed on the server later.
			l.data.checks[id] = CheckState{Deleted: true}
			continue
		}

		// If the check is already scheduled for removal skip it.
		if lc.Deleted {
			continue
		}

		// If our definition is different, we need to update it
		if l.config.CheckUpdateInterval == 0 {
			lc.InSync = lc.Check.IsSame(rc)
			continue
		}

		// Copy the existing check before potentially modifying
		// it before the compare operation.
		lcCopy := lc.Check.Clone()

		// Copy the server's check before modifying, otherwise
		// in-memory RPCs will have side effects.
		rcCopy := rc.Clone()

		// If there's a defer timer active then we've got a
		// potentially spammy check so we don't sync the output
		// during this sweep since the timer will mark the check
		// out of sync for us. Otherwise, it is safe to sync the
		// output now. This is especially important for checks
		// that don't change state after they are created, in
		// which case we'd never see their output synced back ever.
		if lc.DeferCheck != nil {
			lcCopy.Output = ""
			rcCopy.Output = ""
		}
		lc.InSync = lcCopy.IsSame(rcCopy)
	}
	return nil
}

// SyncFull determines the delta between the local and remote state
// and synchronizes the changes.
func (l *State) SyncFull() error {
	// note that we do not acquire the lock here since the methods
	// we are calling will do that themselves.
	//
	// Also note that we don't hold the lock for the entire operation
	// but release it between the two calls. This is not an issue since
	// the algorithm is best-effort to achieve eventual consistency.
	// SyncChanges will sync whatever updateSyncState() has determined
	// needs updating.

	if err := l.updateSyncState(); err != nil {
		return err
	}
	return l.SyncChanges()
}

// SyncChanges pushes checks, services and node info data which has been
// marked out of sync or deleted to the server.
func (l *State) SyncChanges() error {
	l.Lock()
	defer l.Unlock()

	// We will do node-level info syncing at the end, since it will get
	// updated by a service or check sync anyway, given how the register
	// API works.

	// Sync the services
	// (logging happens in the helper methods)
	for id, s := range l.data.services {
		var err error
		switch {
		case s.Deleted:
			err = l.deleteService(id)
		case !s.InSync:
			err = l.syncService(id)
		default:
			l.logger.Printf("[DEBUG] agent: Service %q in sync", id)
		}
		if err != nil {
			return err
		}
	}

	// Sync the checks
	// (logging happens in the helper methods)
	for id, c := range l.data.checks {
		var err error
		switch {
		case c.Deleted:
			err = l.deleteCheck(id)
		case !c.InSync:
			if c.DeferCheck != nil {
				c.DeferCheck.Stop()
				c.DeferCheck = nil
			}
			err = l.syncCheck(id)
		default:
			l.logger.Printf("[DEBUG] agent: Check %q in sync", id)
		}
		if err != nil {
			return err
		}
	}

	// Now sync the node level info if we need to, and didn't do any of
	// the other sync operations.
	if l.nodeInfoInSync {
		l.logger.Printf("[DEBUG] agent: Node info in sync")
		return nil
	}
	return l.syncNodeInfo()
}

// deleteService is used to delete a service from the server
// TODO: tnx
func (l *State) deleteService(id string) error {
	if id == "" {
		return fmt.Errorf("ServiceID missing")
	}

	req := structs.DeregisterRequest{
		Datacenter:   l.config.Datacenter,
		Node:         l.config.NodeName,
		ServiceID:    id,
		WriteRequest: structs.WriteRequest{Token: l.serviceToken(id)},
	}
	var out struct{}
	err := l.Delegate.RPC("Catalog.Deregister", &req, &out)
	switch {
	case err == nil || strings.Contains(err.Error(), "Unknown service"):
		delete(l.data.services, id)
		l.logger.Printf("[INFO] agent: Deregistered service %q", id)
		return nil

	case acl.IsErrPermissionDenied(err):
		// todo(fs): mark the service to be in sync to prevent excessive retrying before next full sync
		// todo(fs): some backoff strategy might be a better solution
		service := l.data.services[id]
		service.InSync = true
		l.data.services[id] = service
		l.logger.Printf("[WARN] agent: Service %q deregistration blocked by ACLs", id)
		metrics.IncrCounter([]string{"acl", "blocked", "service", "deregistration"}, 1)
		return nil

	default:
		l.logger.Printf("[WARN] agent: Deregistering service %q failed. %s", id, err)
		return err
	}
}

// deleteCheck is used to delete a check from the server
func (l *State) deleteCheck(id types.CheckID) error {
	if id == "" {
		return fmt.Errorf("CheckID missing")
	}

	req := structs.DeregisterRequest{
		Datacenter:   l.config.Datacenter,
		Node:         l.config.NodeName,
		CheckID:      id,
		WriteRequest: structs.WriteRequest{Token: l.data.checks[id].Token},
	}
	var out struct{}
	err := l.Delegate.RPC("Catalog.Deregister", &req, &out)
	switch {
	case err == nil || strings.Contains(err.Error(), "Unknown check"):
		c := l.data.checks[id]
		if c.DeferCheck != nil {
			c.DeferCheck.Stop()
		}
		delete(l.data.checks, id)
		l.logger.Printf("[INFO] agent: Deregistered check %q", id)
		return nil

	case acl.IsErrPermissionDenied(err):
		// todo(fs): mark the check to be in sync to prevent excessive retrying before next full sync
		// todo(fs): some backoff strategy might be a better solution
		check := l.data.checks[id]
		check.InSync = true
		l.data.checks[id] = check
		l.logger.Printf("[WARN] agent: Check %q deregistration blocked by ACLs", id)
		metrics.IncrCounter([]string{"acl", "blocked", "check", "deregistration"}, 1)
		return nil

	default:
		l.logger.Printf("[WARN] agent: Deregistering check %q failed. %s", id, err)
		return err
	}
}

// syncService is used to sync a service to the server
func (l *State) syncService(id string) error {
	// If the service has associated checks that are out of sync,
	// piggyback them on the service sync so they are part of the
	// same transaction and are registered atomically. We only let
	// checks ride on service registrations with the same token,
	// otherwise we need to register them separately so they don't
	// pick up privileges from the service token.
	var checks structs.HealthChecks
	for checkID, c := range l.data.checks {
		if c.Deleted || c.InSync {
			continue
		}
		if c.Check.ServiceID != id {
			continue
		}
		if l.serviceToken(id) != l.data.checks[checkID].Token {
			continue
		}
		checks = append(checks, c.Check)
	}

	req := structs.RegisterRequest{
		Datacenter:      l.config.Datacenter,
		ID:              l.config.NodeID,
		Node:            l.config.NodeName,
		Address:         l.config.AdvertiseAddr,
		TaggedAddresses: l.config.TaggedAddresses,
		NodeMeta:        l.data.metadata,
		Service:         l.data.services[id].Service,
		WriteRequest:    structs.WriteRequest{Token: l.serviceToken(id)},
	}

	// Backwards-compatibility for Consul < 0.5
	if len(checks) == 1 {
		req.Check = checks[0]
	} else {
		req.Checks = checks
	}

	var out struct{}
	err := l.Delegate.RPC("Catalog.Register", &req, &out)
	switch {
	case err == nil:
		service := l.data.services[id]
		service.InSync = true
		l.data.services[id] = service
		// Given how the register API works, this info is also updated
		// every time we sync a service.
		l.nodeInfoInSync = true
		for _, check := range checks {
			c := l.data.checks[check.CheckID]
			c.InSync = true
			l.data.checks[check.CheckID] = c
		}
		l.logger.Printf("[INFO] agent: Synced service %q", id)
		return nil

	case acl.IsErrPermissionDenied(err):
		// todo(fs): mark the service and the checks to be in sync to prevent excessive retrying before next full sync
		// todo(fs): some backoff strategy might be a better solution
		service := l.data.services[id]
		service.InSync = true
		l.data.services[id] = service
		for _, check := range checks {
			c := l.data.checks[check.CheckID]
			c.InSync = true
			l.data.checks[check.CheckID] = c
		}
		l.logger.Printf("[WARN] agent: Service %q registration blocked by ACLs", id)
		metrics.IncrCounter([]string{"acl", "blocked", "service", "registration"}, 1)
		return nil

	default:
		l.logger.Printf("[WARN] agent: Syncing service %q failed. %s", id, err)
		return err
	}
}

// syncCheck is used to sync a check to the server
func (l *State) syncCheck(id types.CheckID) error {
	c := l.data.checks[id]

	req := structs.RegisterRequest{
		Datacenter:      l.config.Datacenter,
		ID:              l.config.NodeID,
		Node:            l.config.NodeName,
		Address:         l.config.AdvertiseAddr,
		TaggedAddresses: l.config.TaggedAddresses,
		NodeMeta:        l.data.metadata,
		Check:           c.Check,
		WriteRequest:    structs.WriteRequest{Token: l.data.checks[id].Token},
	}

	// Pull in the associated service if any
	s := l.data.services[c.Check.ServiceID]
	if !s.Deleted {
		req.Service = s.Service
	}

	var out struct{}
	err := l.Delegate.RPC("Catalog.Register", &req, &out)
	switch {
	case err == nil:
		c := l.data.checks[id]
		c.InSync = true
		l.data.checks[id] = c
		// Given how the register API works, this info is also updated
		// every time we sync a check.
		l.nodeInfoInSync = true
		l.logger.Printf("[INFO] agent: Synced check %q", id)
		return nil

	case acl.IsErrPermissionDenied(err):
		// todo(fs): mark the check to be in sync to prevent excessive retrying before next full sync
		// todo(fs): some backoff strategy might be a better solution
		c := l.data.checks[id]
		c.InSync = true
		l.data.checks[id] = c
		l.logger.Printf("[WARN] agent: Check %q registration blocked by ACLs", id)
		metrics.IncrCounter([]string{"acl", "blocked", "check", "registration"}, 1)
		return nil

	default:
		l.logger.Printf("[WARN] agent: Syncing check %q failed. %s", id, err)
		return err
	}
}

func (l *State) syncNodeInfo() error {
	req := structs.RegisterRequest{
		Datacenter:      l.config.Datacenter,
		ID:              l.config.NodeID,
		Node:            l.config.NodeName,
		Address:         l.config.AdvertiseAddr,
		TaggedAddresses: l.config.TaggedAddresses,
		NodeMeta:        l.data.metadata,
		WriteRequest:    structs.WriteRequest{Token: l.tokens.AgentToken()},
	}
	var out struct{}
	err := l.Delegate.RPC("Catalog.Register", &req, &out)
	switch {
	case err == nil:
		l.nodeInfoInSync = true
		l.logger.Printf("[INFO] agent: Synced node info")
		return nil

	case acl.IsErrPermissionDenied(err):
		// todo(fs): mark the node info to be in sync to prevent excessive retrying before next full sync
		// todo(fs): some backoff strategy might be a better solution
		l.nodeInfoInSync = true
		l.logger.Printf("[WARN] agent: Node info update blocked by ACLs")
		metrics.IncrCounter([]string{"acl", "blocked", "node", "registration"}, 1)
		return nil

	default:
		l.logger.Printf("[WARN] agent: Syncing node info failed. %s", err)
		return err
	}
}
