package local

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/consul/types"
	uuid "github.com/hashicorp/go-uuid"
)

type op interface {
	apply(s *State, d *stateData) (bool, error)
}

type setNodeMetaOp struct {
	data map[string]string
}

func (o setNodeMetaOp) apply(s *State, d *stateData) (bool, error) {
	d.metadata = o.data

	return true, nil
}

type updateCheckOp struct {
	id     types.CheckID
	status string
	output string
}

func (o updateCheckOp) apply(s *State, d *stateData) (bool, error) {
	c := d.checks[o.id]
	if c.Deleted {
		return false, nil
	}

	if s.discardCheckOutput.Load().(bool) {
		o.output = ""
	}

	// Update the critical time tracking (this doesn't cause a server updates
	// so we can always keep this up to date).
	if o.status == api.HealthCritical {
		if !c.Critical() {
			c.CriticalTime = time.Now()
		}
	} else {
		c.CriticalTime = time.Time{}
	}

	// Do nothing if update is idempotent
	if c.Check.Status == o.status && c.Check.Output == o.output {
		return false, nil
	}

	// Defer a sync if the output has changed. This is an optimization around
	// frequent updates of output. Instead, we update the output internally,
	// and periodically do a write-back to the servers. If there is a status
	// change we do the write immediately.
	if s.config.CheckUpdateInterval > 0 && c.Check.Status == o.status {
		c.Check.Output = o.output
		if c.DeferCheck == nil {
			d := s.config.CheckUpdateInterval
			intv := time.Duration(uint64(d)/2) + lib.RandomStagger(d)
			c.DeferCheck = time.AfterFunc(intv, func() {
				// TODO unmess
				s.Lock()
				defer s.Unlock()

				c := s.data.checks[o.id]

				c.DeferCheck = nil
				if c.Deleted {
					return
				}
				c.InSync = false
				s.TriggerSyncChanges()
			})
		}
		return false, nil
	}

	// If this is a check for an aliased service, then notify the waiters.
	if aliases, ok := d.checkAliases[c.Check.ServiceID]; ok && len(aliases) > 0 {
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
	c.Check.Status = o.status
	c.Check.Output = o.output
	c.InSync = false

	return true, nil
}

type addServiceOp struct {
	service *structs.NodeService
	token   string
}

func (o addServiceOp) apply(s *State, d *stateData) (bool, error) {
	if o.service == nil {
		return false, fmt.Errorf("no service")
	}

	// use the service name as id if the id was omitted
	if o.service.ID == "" {
		o.service.ID = o.service.Service
	}

	d.services[o.service.ID] = ServiceState{
		Service: o.service,
		Token:   o.token,
	}

	return true, nil
}

type addCheckOp struct {
	check     *structs.HealthCheck
	checkTask checkTask
	token     string
}

func (o addCheckOp) apply(s *State, d *stateData) (bool, error) {
	check := *o.check.Clone()

	if check.CheckID == "" {
		return false, fmt.Errorf("CheckID missing")
	}

	if s.discardCheckOutput.Load().(bool) {
		check.Output = ""
	}

	// if there is a serviceID associated with the check, make sure it exists before adding it
	// NOTE - This logic may be moved to be handled within the Agent's Addcheck method after a refactor
	if _, ok := d.services[check.ServiceID]; check.ServiceID != "" && !ok {
		return false, fmt.Errorf("Check %q refers to non-existent service %q", check.CheckID, check.ServiceID)
	}

	// hard-set the node name
	check.Node = s.config.NodeName

	if check.ServiceID != "" {
		check.ServiceName = d.services[check.ServiceID].Service.Service
		check.ServiceTags = d.services[check.ServiceID].Service.Tags
	}

	if existing, ok := d.checkTasks[check.CheckID]; ok {
		existing.Stop()
		delete(d.checkTasks, check.CheckID)
	}

	d.checkTasks[check.CheckID] = o.checkTask

	d.checks[check.CheckID] = CheckState{
		Check: check,
		Token: o.token,
	}

	return true, nil
}

type addAliasCheckOp struct {
	checkID      types.CheckID
	srcServiceID string
	notifyCh     chan<- struct{}
}

func (o addAliasCheckOp) apply(s *State, d *stateData) (bool, error) {
	m, ok := d.checkAliases[o.srcServiceID]
	if !ok {
		m = make(map[types.CheckID]chan<- struct{})
		d.checkAliases[o.srcServiceID] = m
	}
	m[o.checkID] = o.notifyCh

	return true, nil
}

type removeServiceOp struct {
	serviceID string
}

func (o removeServiceOp) apply(s *State, d *stateData) (bool, error) {
	service, ok := d.services[o.serviceID]
	if !ok {
		return false, fmt.Errorf("Service %s does not exist", o.serviceID)
	}

	service.Deleted = true
	service.InSync = false
	d.services[o.serviceID] = service

	return true, nil
}

type removeCheckOp struct {
	checkID types.CheckID
}

func (o removeCheckOp) apply(s *State, d *stateData) (bool, error) {
	check, ok := d.checks[o.checkID]
	if !ok {
		return false, fmt.Errorf("Check %s does not exist", o.checkID)
	}

	check.Deleted = true
	d.checks[o.checkID] = check

	return true, nil
}

type removeCheckAliasOp struct {
	checkID      types.CheckID
	srcServiceID string
}

func (o removeCheckAliasOp) apply(s *State, d *stateData) (bool, error) {
	if m, ok := d.checkAliases[o.srcServiceID]; ok {
		delete(m, o.checkID)
		if len(m) == 0 {
			delete(d.checkAliases, o.srcServiceID)
		}
	}

	return true, nil
}

type addProxyOp struct {
	proxy              *structs.ConnectManagedProxy
	token              string
	restoredProxyToken string
}

func (o addProxyOp) apply(s *State, d *stateData) (bool, error) {
	if o.proxy == nil {
		return false, fmt.Errorf("no proxy")
	}

	// Lookup the local service
	target, ok := d.services[o.proxy.TargetServiceID]
	if !ok {
		return false, fmt.Errorf("target service ID %s not registered",
			o.proxy.TargetServiceID)
	}

	// Get bind info from config
	cfg, err := o.proxy.ParseConfig()
	if err != nil {
		return false, err
	}

	// Construct almost all of the NodeService that needs to be registered by the
	// caller outside of the lock.
	svc := &structs.NodeService{
		Kind:    structs.ServiceKindConnectProxy,
		ID:      target.Service.ID + "-proxy",
		Service: target.Service.Service + "-proxy",
		Proxy: structs.ConnectProxyConfig{
			DestinationServiceName: target.Service.Service,
			LocalServiceAddress:    cfg.LocalServiceAddress,
			LocalServicePort:       cfg.LocalServicePort,
		},
		Address: cfg.BindAddress,
		Port:    cfg.BindPort,
	}

	// Set default port now while the target is known
	if svc.Proxy.LocalServicePort < 1 {
		svc.Proxy.LocalServicePort = target.Service.Port
	}

	pToken := o.restoredProxyToken

	// Does this proxy instance allready exist?
	if existing, ok := d.managedProxies[svc.ID]; ok {
		// Keep the existing proxy token so we don't have to restart proxy to
		// re-inject token.
		pToken = existing.ProxyToken
		// If the user didn't explicitly change the port, use the old one instead of
		// assigning new.
		if svc.Port < 1 {
			svc.Port = existing.Proxy.ProxyService.Port
		}
	} else if proxyService, ok := d.services[svc.ID]; ok {
		// The proxy-service already exists so keep the port that got assigned. This
		// happens on reload from disk since service definitions are reloaded first.
		svc.Port = proxyService.Service.Port
	}

	// If this is a new instance, generate a token
	if pToken == "" {
		pToken, err = uuid.GenerateUUID()
		if err != nil {
			return false, err
		}
	}

	// Allocate port if needed (min and max inclusive).
	rangeLen := s.config.ProxyBindMaxPort - s.config.ProxyBindMinPort + 1
	if svc.Port < 1 && s.config.ProxyBindMinPort > 0 && rangeLen > 0 {
		// This should be a really short list so don't bother optimising lookup yet.
	OUTER:
		for _, offset := range rand.Perm(rangeLen) {
			p := s.config.ProxyBindMinPort + offset
			// See if this port was already allocated to another proxy
			for _, other := range d.managedProxies {
				if other.Proxy.ProxyService.Port == p {
					// allready taken, skip to next random pick in the range
					continue OUTER
				}
			}
			// We made it through all existing proxies without a match so claim this one
			svc.Port = p
			break
		}
	}
	// If no ports left (or auto ports disabled) fail
	if svc.Port < 1 {
		return false, fmt.Errorf("no port provided for proxy bind_port and none "+
			" left in the allocated range [%d, %d]", s.config.ProxyBindMinPort,
			s.config.ProxyBindMaxPort)
	}

	o.proxy.ProxyService = svc

	// All set, add the proxy and return the service
	if old, ok := d.managedProxies[svc.ID]; ok {
		// Notify watchers of the existing proxy config that it's changing. Note
		// this is safe here even before the map is updated since we still hold the
		// state lock and the watcher can't re-read the new config until we return
		// anyway.
		close(old.WatchCh)
	}
	d.managedProxies[svc.ID] = ManagedProxy{
		Proxy:      o.proxy,
		ProxyToken: pToken,
		WatchCh:    make(chan struct{}),
	}

	//TODO managed proxy event

	// No need to trigger sync as proxy state is local only.
	return false, nil
}

type Tnx struct {
	s         *State
	data      *stateData
	discarded bool

	ops []op
}

func (t *Tnx) Discard() {
	if t.discarded {
		return
	}
	t.discarded = true
	t.s.Unlock()
}

func (t *Tnx) Commit() error {
	if t.discarded {
		return fmt.Errorf("local state transaction discarded")
	}

	defer t.Discard()

	changed := false

	for _, op := range t.ops {
		c, err := op.apply(t.s, t.data)
		if err != nil {
			return err
		}
		if c {
			changed = true
		}
	}

	// all task applied successfully, sync checkTasks
	for id, check := range t.data.checks {
		task, ok := t.data.checkTasks[id]
		if !ok {
			continue
		}

		if check.Deleted {
			task.Stop()
			delete(t.data.checkTasks, id)
		} else if !task.Running() {
			task.Start()
		}
	}

	if changed {
		t.s.data = t.data
		t.s.TriggerSyncChanges()
		t.s.broadcastUpdateLocked()
	}

	return nil
}

// AddService is used to add a service entry to the local state.
// This entry is persistent and the agent will make a best effort to
// ensure it is registered
func (t *Tnx) AddService(service *structs.NodeService, token string) {
	t.ops = append(t.ops, addServiceOp{
		service: service,
		token:   token,
	})
}

// AddCheck is used to add a health check to the local state.
// This entry is persistent and the agent will make a best effort to
// ensure it is registered
func (t *Tnx) AddCheck(check *structs.HealthCheck, checkTask checkTask, token string) {
	t.ops = append(t.ops, addCheckOp{
		check:     check,
		checkTask: checkTask,
		token:     token,
	})
}

// UpdateCheck is used to update the status of a check
func (t *Tnx) UpdateCheck(id types.CheckID, status, output string) {
	t.ops = append(t.ops, updateCheckOp{
		id:     id,
		status: status,
		output: output,
	})
}

// AddAliasCheck creates an alias check. When any check for the srcServiceID is
// changed, checkID will reflect that using the same semantics as
// checks.CheckAlias.
//
// This is a local optimization so that the Alias check doesn't need to use
// blocking queries against the remote server for check updates for local
// services.
func (t *Tnx) AddAliasCheck(checkID types.CheckID, srcServiceID string, notifyCh chan<- struct{}) {
	t.ops = append(t.ops, addAliasCheckOp{
		checkID:      checkID,
		srcServiceID: srcServiceID,
		notifyCh:     notifyCh,
	})
}

// RemoveService is used to remove a service entry from the local state.
// The agent will make a best effort to ensure it is deregistered.
func (t *Tnx) RemoveService(serviceID string) {
	t.ops = append(t.ops, removeServiceOp{
		serviceID: serviceID,
	})
}

// RemoveCheck is used to remove a health check from the local state.
// The agent will make a best effort to ensure it is deregistered
// todo(fs): RemoveService returns an error for a non-existant service. RemoveCheck should as well.
// todo(fs): Check code that calls this to handle the error.
func (t *Tnx) RemoveCheck(checkID types.CheckID) {
	t.ops = append(t.ops, removeCheckOp{
		checkID: checkID,
	})
}

// RemoveAliasCheck removes the mapping for the alias check.
func (t *Tnx) RemoveAliasCheck(checkID types.CheckID, serviceID string) {
	t.ops = append(t.ops, removeCheckAliasOp{
		checkID:      checkID,
		srcServiceID: serviceID,
	})
}

// AddProxy is used to add a connect proxy entry to the local state. This
// assumes the proxy's NodeService is already registered via Agent.AddService
// (since that has to do other book keeping). The token passed here is the ACL
// token the service used to register itself so must have write on service
// record. AddProxy returns the newly added proxy and an error.
//
// The restoredProxyToken argument should only be used when restoring proxy
// definitions from disk; new proxies must leave it blank to get a new token
// assigned. We need to restore from disk to enable to continue authenticating
// running proxies that already had that credential injected.
func (t *Tnx) AddProxy(proxy *structs.ConnectManagedProxy, token string, restoredProxyToken string) {
	t.ops = append(t.ops, addProxyOp{})
}

// SetNodeMeta sets the node metadata
func (t *Tnx) SetNodeMeta(meta map[string]string) {
	t.ops = append(t.ops, setNodeMetaOp{
		data: meta,
	})
}
