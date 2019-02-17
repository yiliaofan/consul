package local

import (
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/hashicorp/consul/agent/checks"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/lib"
	"github.com/hashicorp/consul/types"
)

type checkNotifier struct {
	m *State
}

func (n checkNotifier) UpdateCheck(checkID types.CheckID, status, output string) {
	tx := n.m.WriteTx()
	defer tx.Done()
	tx.UpdateCheck(checkID, status, output)
}

func (n checkNotifier) AddAliasCheck(checkID types.CheckID, srcServiceID string, notifyCh chan<- struct{}) error {
	tx := n.m.WriteTx()
	defer tx.Done()
	return tx.AddAliasCheck(checkID, srcServiceID, notifyCh)
}

func (n checkNotifier) RemoveAliasCheck(checkID types.CheckID, srcServiceID string) {
	tx := n.m.WriteTx()
	defer tx.Done()
	tx.RemoveAliasCheck(checkID, srcServiceID)
}

func (n checkNotifier) Checks() map[types.CheckID]*structs.HealthCheck {
	tx := n.m.ReadTx()
	defer tx.Done()
	return tx.Checks()
}

func (t *ReadTx) Check(id types.CheckID) *structs.HealthCheck {
	state, ok := t.state.checks[id]
	if !ok {
		return nil
	}

	return state.Check
}

func (t *ReadTx) Checks() map[types.CheckID]*structs.HealthCheck {
	m := make(map[types.CheckID]*structs.HealthCheck)
	for id, c := range t.state.checks {
		m[id] = c.Check
	}
	return m
}

func (t *WriteTx) UpdateCheck(id types.CheckID, status, output string) {
	c := t.state.checks[id]
	if c == nil || c.Deleted {
		return
	}

	if t.manager.config.DiscardCheckOutput {
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
	if t.manager.config.CheckUpdateInterval > 0 && c.Check.Status == status {
		c.Check.Output = output
		if c.DeferCheck == nil {
			d := t.manager.config.CheckUpdateInterval
			intv := time.Duration(uint64(d)/2) + lib.RandomStagger(d)
			c.DeferCheck = time.AfterFunc(intv, func() {
				tx := t.manager.ReadTx()
				defer tx.Done()

				c := tx.state.checks[id]
				if c == nil {
					return
				}
				c.DeferCheck = nil
				if c.Deleted {
					return
				}
				c.InSync = false
			})
		}
		return
	}

	// If this is a check for an aliased service, then notify the waiters.
	if aliases, ok := t.state.checkAliases[c.Check.ServiceID]; ok && len(aliases) > 0 {
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
}

func (t *WriteTx) AddCheck(check *structs.HealthCheck, chkType *structs.CheckType, token string) (err error) {
	defer func() {
		if err != nil {
			t.Discard()
			return
		}

		t.notifies = append(t.notifies, watchKeyCheck(check.CheckID))
	}()

	if check.CheckID == "" {
		err = fmt.Errorf("CheckID missing")
		return
	}

	if chkType != nil {
		if err = chkType.Validate(); err != nil {
			err = fmt.Errorf("Check is not valid: %v", err)
			return
		}
	}

	if check.ServiceID != "" {
		s := t.Service(check.ServiceID)
		if s == nil {
			return fmt.Errorf("ServiceID %q does not exist", check.ServiceID)
		}
		check.ServiceName = s.Service
		check.ServiceTags = s.Tags
	}

	if t.manager.config.DiscardCheckOutput {
		check.Output = ""
	}

	// snapshot the current state of the health check to avoid potential flapping
	existing := t.Check(check.CheckID)
	defer func() {
		if existing != nil {
			t.UpdateCheck(check.CheckID, existing.Status, existing.Output)
		}
	}()

	// Check if already registered
	if chkType != nil {
		if existing, ok := t.state.checkTasks[check.CheckID]; ok {
			t.onCommit = append(t.onCommit, func() {
				existing.Stop()
			})
		}

		var checkTask checkTask

		switch {
		case chkType.IsTTL():
			ttl := &checks.CheckTTL{
				Notify:  checkNotifier{t.manager},
				CheckID: check.CheckID,
				TTL:     chkType.TTL,
				Logger:  t.manager.logger,
			}

			// Restore persisted state, if any
			status, output, err := t.manager.persistanceManager.loadCheckState(check.CheckID)
			if err != nil {
				t.manager.logger.Printf("[WARN] agent: failed restoring state for check %q: %s",
					check.CheckID, err)
			}
			check.Status = status
			check.Output = output

			checkTask = ttl

		case chkType.IsHTTP():
			if chkType.Interval < checks.MinInterval {
				t.manager.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}

			var tlsClientConfig *tls.Config
			tlsClientConfig, err = t.setupTLSClientConfig(chkType.TLSSkipVerify)
			if err != nil {
				err = fmt.Errorf("Failed to set up TLS: %v", err)
				return
			}

			http := &checks.CheckHTTP{
				Notify:          checkNotifier{t.manager},
				CheckID:         check.CheckID,
				HTTP:            chkType.HTTP,
				Header:          chkType.Header,
				Method:          chkType.Method,
				Interval:        chkType.Interval,
				Timeout:         chkType.Timeout,
				Logger:          t.manager.logger,
				TLSClientConfig: tlsClientConfig,
			}
			checkTask = http

		case chkType.IsTCP():
			if chkType.Interval < checks.MinInterval {
				t.manager.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}

			tcp := &checks.CheckTCP{
				Notify:   checkNotifier{t.manager},
				CheckID:  check.CheckID,
				TCP:      chkType.TCP,
				Interval: chkType.Interval,
				Timeout:  chkType.Timeout,
				Logger:   t.manager.logger,
			}
			checkTask = tcp

		case chkType.IsGRPC():
			if chkType.Interval < checks.MinInterval {
				t.manager.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}

			var tlsClientConfig *tls.Config
			if chkType.GRPCUseTLS {
				var err error
				tlsClientConfig, err = t.setupTLSClientConfig(chkType.TLSSkipVerify)
				if err != nil {
					return fmt.Errorf("Failed to set up TLS: %v", err)
				}
			}

			grpc := &checks.CheckGRPC{
				Notify:          checkNotifier{t.manager},
				CheckID:         check.CheckID,
				GRPC:            chkType.GRPC,
				Interval:        chkType.Interval,
				Timeout:         chkType.Timeout,
				Logger:          t.manager.logger,
				TLSClientConfig: tlsClientConfig,
			}
			checkTask = grpc

		case chkType.IsDocker():
			if chkType.Interval < checks.MinInterval {
				t.manager.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}

			if t.manager.dockerClient == nil {
				dc, err := checks.NewDockerClient(os.Getenv("DOCKER_HOST"), checks.BufSize)
				if err != nil {
					t.manager.logger.Printf("[ERR] agent: error creating docker client: %s", err)
					return err
				}
				t.manager.logger.Printf("[DEBUG] agent: created docker client for %s", dc.Host())
				t.manager.dockerClient = dc
			}

			dockerCheck := &checks.CheckDocker{
				Notify:            checkNotifier{t.manager},
				CheckID:           check.CheckID,
				DockerContainerID: chkType.DockerContainerID,
				Shell:             chkType.Shell,
				ScriptArgs:        chkType.ScriptArgs,
				Interval:          chkType.Interval,
				Logger:            t.manager.logger,
				Client:            t.manager.dockerClient,
			}
			checkTask = dockerCheck

		case chkType.IsMonitor():
			if chkType.Interval < checks.MinInterval {
				t.manager.logger.Printf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval)
				chkType.Interval = checks.MinInterval
			}

			monitor := &checks.CheckMonitor{
				Notify:     checkNotifier{t.manager},
				CheckID:    check.CheckID,
				ScriptArgs: chkType.ScriptArgs,
				Interval:   chkType.Interval,
				Timeout:    chkType.Timeout,
				Logger:     t.manager.logger,
			}
			checkTask = monitor

		case chkType.IsAlias():
			var rpcReq structs.NodeSpecificRequest
			rpcReq.Datacenter = t.manager.config.Datacenter

			// The token to set is really important. The behavior below follows
			// the same behavior as anti-entropy: we use the user-specified token
			// if set (either on the service or check definition), otherwise
			// we use the "UserToken" on the agent. This is tested.
			rpcReq.Token = t.manager.tokens.UserToken()
			if token != "" {
				rpcReq.Token = token
			}

			chkImpl := &checks.CheckAlias{
				Notify:    checkNotifier{t.manager},
				RPC:       t.manager.delegate,
				RPCReq:    rpcReq,
				CheckID:   check.CheckID,
				Node:      chkType.AliasNode,
				ServiceID: chkType.AliasService,
			}
			checkTask = chkImpl

		default:
			err = fmt.Errorf("Check type is not valid")
			return
		}

		if chkType.DeregisterCriticalServiceAfter > 0 {
			timeout := chkType.DeregisterCriticalServiceAfter
			if timeout < t.manager.config.CheckDeregisterIntervalMin {
				timeout = t.manager.config.CheckDeregisterIntervalMin
				t.manager.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has deregister interval below minimum of %v",
					check.CheckID, t.manager.config.CheckDeregisterIntervalMin))
			}
			t.state.checkReapAfter[check.CheckID] = timeout
		} else {
			delete(t.state.checkReapAfter, check.CheckID)
		}

		t.state.checkTasks[check.CheckID] = checkTask
		t.onCommit = append(t.onCommit, checkTask.Start)
	}

	t.state.checks[check.CheckID] = &CheckState{
		Check: check,
	}

	return nil
}

func (t *WriteTx) AddAliasCheck(checkID types.CheckID, srcServiceID string, notifyCh chan<- struct{}) error {
	m, ok := t.state.checkAliases[srcServiceID]
	if !ok {
		m = make(map[types.CheckID]chan<- struct{})
		t.state.checkAliases[srcServiceID] = m
	}
	m[checkID] = notifyCh

	return nil
}

// RemoveAliasCheck removes the mapping for the alias check.
func (t *WriteTx) RemoveAliasCheck(checkID types.CheckID, srcServiceID string) {
	if m, ok := t.state.checkAliases[srcServiceID]; ok {
		delete(m, checkID)
		if len(m) == 0 {
			delete(t.state.checkAliases, srcServiceID)
		}
	}
}

func (t *WriteTx) setupTLSClientConfig(skipVerify bool) (tlsClientConfig *tls.Config, err error) {
	// We re-use the API client's TLS structure since it
	// closely aligns with Consul's internal configuration.
	tlsConfig := &api.TLSConfig{
		InsecureSkipVerify: skipVerify,
	}
	if t.manager.config.EnableAgentTLSForChecks {
		tlsConfig.Address = t.manager.config.ServerName
		tlsConfig.KeyFile = t.manager.config.KeyFile
		tlsConfig.CertFile = t.manager.config.CertFile
		tlsConfig.CAFile = t.manager.config.CAFile
		tlsConfig.CAPath = t.manager.config.CAPath
	}
	tlsClientConfig, err = api.SetupTLSConfig(tlsConfig)
	return
}
