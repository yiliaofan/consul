package local

import (
	"fmt"
	"math/rand"

	"github.com/hashicorp/consul/agent/structs"
	uuid "github.com/hashicorp/go-uuid"
)

// Proxy returns the local proxy state.
func (t *ReadTx) Proxy(id string) *ManagedProxy {
	return t.state.managedProxies[id]
}

// Proxies returns the locally registered proxies.
func (t *ReadTx) Proxies() map[string]*ManagedProxy {

	m := make(map[string]*ManagedProxy)
	for id, p := range t.state.managedProxies {
		m[id] = p
	}
	return m
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
func (t *WriteTx) AddProxy(proxy *structs.ConnectManagedProxy, token, restoredProxyToken string) (p *ManagedProxy, err error) {
	defer func() {
		if err != nil {
			t.Discard()
			return
		}

		t.notifies = append(t.notifies, watchKeyAnyProxy{})
	}()

	if proxy == nil {
		err = fmt.Errorf("no proxy")
		return
	}

	// Lookup the local service
	target := t.Service(proxy.TargetServiceID)
	if target == nil {
		err = fmt.Errorf("target service ID %s not registered",
			proxy.TargetServiceID)
		return
	}

	// Get bind info from config
	var cfg *structs.ConnectManagedProxyConfig
	cfg, err = proxy.ParseConfig()
	if err != nil {
		return
	}

	// Construct almost all of the NodeService that needs to be registered by the
	// caller outside of the lock.
	svc := &structs.NodeService{
		Kind:    structs.ServiceKindConnectProxy,
		ID:      target.ID + "-proxy",
		Service: target.Service + "-proxy",
		Proxy: structs.ConnectProxyConfig{
			DestinationServiceName: target.Service,
			LocalServiceAddress:    cfg.LocalServiceAddress,
			LocalServicePort:       cfg.LocalServicePort,
		},
		Address: cfg.BindAddress,
		Port:    cfg.BindPort,
	}

	// Set default port now while the target is known
	if svc.Proxy.LocalServicePort < 1 {
		svc.Proxy.LocalServicePort = target.Port
	}

	pToken := restoredProxyToken

	// Does this proxy instance allready exist?
	if existing, ok := t.state.managedProxies[svc.ID]; ok {
		// Keep the existing proxy token so we don't have to restart proxy to
		// re-inject token.
		pToken = existing.ProxyToken
		// If the user didn't explicitly change the port, use the old one instead of
		// assigning new.
		if svc.Port < 1 {
			svc.Port = existing.Proxy.ProxyService.Port
		}
	} else if proxyService, ok := t.state.services[svc.ID]; ok {
		// The proxy-service already exists so keep the port that got assigned. This
		// happens on reload from disk since service definitions are reloaded first.
		svc.Port = proxyService.Service.Port
	}

	// If this is a new instance, generate a token
	if pToken == "" {
		pToken, err = uuid.GenerateUUID()
		if err != nil {
			return
		}
	}

	// Allocate port if needed (min and max inclusive).
	rangeLen := t.manager.config.ConnectProxyBindMaxPort - t.manager.config.ConnectProxyBindMinPort + 1
	if svc.Port < 1 && t.manager.config.ConnectProxyBindMinPort > 0 && rangeLen > 0 {
		// This should be a really short list so don't bother optimising lookup yet.
	OUTER:
		for _, offset := range rand.Perm(rangeLen) {
			p := t.manager.config.ConnectProxyBindMinPort + offset
			// See if this port was already allocated to another proxy
			for _, other := range t.state.managedProxies {
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
		return nil, fmt.Errorf("no port provided for proxy bind_port and none "+
			" left in the allocated range [%d, %d]", t.manager.config.ConnectProxyBindMinPort,
			t.manager.config.ConnectProxyBindMaxPort)
	}

	proxy.ProxyService = svc

	// All set, add the proxy and return the service
	if old, ok := t.state.managedProxies[svc.ID]; ok {
		// Notify watchers of the existing proxy config that it's changing. Note
		// this is safe here even before the map is updated since we still hold the
		// state lock and the watcher can't re-read the new config until we return
		// anyway.
		close(old.WatchCh)
	}
	t.state.managedProxies[svc.ID] = &ManagedProxy{
		Proxy:      proxy,
		ProxyToken: pToken,
		WatchCh:    make(chan struct{}),
	}

	// No need to trigger sync as proxy state is local only.
	return t.state.managedProxies[svc.ID], nil
}

// RemoveProxy is used to remove a proxy entry from the local state.
// This returns the proxy that was removed.
func (t *WriteTx) RemoveProxy(id string) (p *ManagedProxy, err error) {
	defer func() {
		if err != nil {
			t.Discard()
			return
		}

		t.notifies = append(t.notifies, watchKeyAnyProxy{})
	}()

	p = t.state.managedProxies[id]
	if p == nil {
		err = fmt.Errorf("Proxy %s does not exist", id)
		return
	}
	delete(t.state.managedProxies, id)

	return
}
