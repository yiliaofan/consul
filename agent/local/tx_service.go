package local

import (
	"fmt"

	"github.com/hashicorp/consul/agent/structs"
)

// Service returns the locally registered service that the
// agent is aware of and are being kept in sync with the server
func (t *ReadTx) Service(id string) *structs.NodeService {
	s := t.state.services[id]
	if s == nil || s.Deleted {
		return nil
	}
	return s.Service
}

// Service returns the locally registered service that the
// agent is aware of and are being kept in sync with the server
func (t *ReadTx) ServiceToken(id string) string {
	s := t.state.services[id]
	if s == nil || s.Deleted {
		return ""
	}
	return s.Token
}

// Services returns the locally registered services that the
// agent is aware of and are being kept in sync with the server
func (t *ReadTx) Services() map[string]*structs.NodeService {
	m := make(map[string]*structs.NodeService)
	for id, s := range t.state.services {
		if s.Deleted {
			continue
		}
		m[id] = s.Service
	}
	return m
}

// AddService is used to add a service entry to the local state.
// This entry is persistent and the agent will make a best effort to
// ensure it is registered
func (t *WriteTx) AddService(service *structs.NodeService, token string) (err error) {
	defer func() {
		if err != nil {
			t.Discard()
			return
		}

		t.notifies = append(t.notifies, watchKeyService(service.ID))
	}()

	if service == nil {
		err = fmt.Errorf("no service")
		return
	}

	// use the service name as id if the id was omitted
	if service.ID == "" {
		service.ID = service.Service
	}

	t.state.services[service.ID] = &ServiceState{
		Service: service,
		Token:   token,
	}

	return nil
}

// RemoveService is used to remove a service entry from the local state.
// The agent will make a best effort to ensure it is deregistered.
func (t *WriteTx) RemoveService(id string) (err error) {
	defer func() {
		if err != nil {
			t.Discard()
			return
		}

		t.notifies = append(t.notifies, watchKeyService(id))
	}()

	s := t.state.services[id]
	if s == nil || s.Deleted {
		err = fmt.Errorf("Service %q does not exist", id)
		return
	}

	// To remove the service on the server we need the token.
	// Therefore, we mark the service as deleted and keep the
	// entry around until it is actually removed.
	s.Deleted = true
	return nil
}
