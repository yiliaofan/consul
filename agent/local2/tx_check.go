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
	l "google.golang.org/genproto"
)

type checkNotifier struct {
	tx *WriteTx
}

func (n checkNotifier) UpdateCheck(checkID types.CheckID, status, output string) {
	tx := n.tx.newTx()
	defer tx.Done()
	tx.UpdateCheck(checkID, status, output)
}

func (t *ReadTx) Check(id types.CheckID) *structs.HealthCheck {
	state, ok := t.state.checks[id]
	if !ok {
		return nil
	}

	return state.Check
}

func (t *WriteTx) UpdateCheck(id types.CheckID, status, output string) {
	c := t.state.checks[id]
	if c == nil || c.Deleted {
		return
	}

	if t.state.config.DiscardCheckOutput {
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
	if t.state.config.CheckUpdateInterval > 0 && c.Check.Status == status {
		c.Check.Output = output
		if c.DeferCheck == nil {
			d := t.state.config.CheckUpdateInterval
			intv := time.Duration(uint64(d)/2) + lib.RandomStagger(d)
			c.DeferCheck = time.AfterFunc(intv, func() {
				tx := t.newTx()
				defer tx.Discard()

				c := tx.state.checks[id]
				if c == nil {
					return
				}
				c.DeferCheck = nil
				if c.Deleted {
					return
				}
				c.InSync = false

				tx.Done()
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

func (t *WriteTx) AddCheck(check *structs.HealthCheck, chkType *structs.CheckType, token string) error {
	if check.CheckID == "" {
		return fmt.Errorf("CheckID missing")
	}

	if chkType != nil {
		if err := chkType.Validate(); err != nil {
			return fmt.Errorf("Check is not valid: %v", err)
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

	if t.state.config.DiscardCheckOutput {
		check.Output = ""
	}

	// snapshot the current state of the health check to avoid potential flapping
	existing := t.Check(check.CheckID)
	defer func() {
		if existing != nil {
			t.UpdateCheck(check.CheckID, existing.Status, existing.Output)
		}
	}()

	if existing, ok := t.state.checkTasks[check.CheckID]; ok {
		t.onCommit = append(t.onCommit, func() {
			existing.Stop()
		})
	}

	// Check if already registered
	if chkType != nil {
		switch {
		case chkType.IsTTL():
			ttl := &checks.CheckTTL{
				Notify:  checkNotifier{t},
				CheckID: check.CheckID,
				TTL:     chkType.TTL,
				Logger:  t.logger,
			}

			// Restore persisted state, if any
			status, output, err := t.state.persistanceManager.loadCheckState(check.CheckID)
			if err != nil {
				t.logger.Printf("[WARN] agent: failed restoring state for check %q: %s",
					check.CheckID, err)
			}
			check.Status = status
			check.Output = output

			ttl.Start()
			t.state.checkTasks[check.CheckID] = ttl

		case chkType.IsHTTP():
			if existing, ok := t.state.checkTasks[check.CheckID]; ok {
				existing.Stop()
				delete(t.state.checkTasks, check.CheckID)
			}
			if chkType.Interval < checks.MinInterval {
				t.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}

			tlsClientConfig, err := t.setupTLSClientConfig(chkType.TLSSkipVerify)
			if err != nil {
				return fmt.Errorf("Failed to set up TLS: %v", err)
			}

			http := &checks.CheckHTTP{
				Notify:          checkNotifier{t},
				CheckID:         check.CheckID,
				HTTP:            chkType.HTTP,
				Header:          chkType.Header,
				Method:          chkType.Method,
				Interval:        chkType.Interval,
				Timeout:         chkType.Timeout,
				Logger:          t.logger,
				TLSClientConfig: tlsClientConfig,
			}
			http.Start()
			t.state.checkTasks[check.CheckID] = http

		case chkType.IsTCP():
			if existing, ok := t.state.checkTasks[check.CheckID]; ok {
				existing.Stop()
				delete(t.state.checkTasks, check.CheckID)
			}
			if chkType.Interval < checks.MinInterval {
				t.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}

			tcp := &checks.CheckTCP{
				Notify:   checkNotifier{t},
				CheckID:  check.CheckID,
				TCP:      chkType.TCP,
				Interval: chkType.Interval,
				Timeout:  chkType.Timeout,
				Logger:   t.logger,
			}
			tcp.Start()
			t.state.checkTasks[check.CheckID] = tcp

		case chkType.IsGRPC():
			if existing, ok := t.state.checkTasks[check.CheckID]; ok {
				existing.Stop()
				delete(t.state.checkTasks, check.CheckID)
			}
			if chkType.Interval < checks.MinInterval {
				t.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
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
				Notify:          checkNotifier{t},
				CheckID:         check.CheckID,
				GRPC:            chkType.GRPC,
				Interval:        chkType.Interval,
				Timeout:         chkType.Timeout,
				Logger:          t.logger,
				TLSClientConfig: tlsClientConfig,
			}
			grpc.Start()
			t.state.checkTasks[check.CheckID] = grpc

		case chkType.IsDocker():
			if existing, ok := t.state.checkTasks[check.CheckID]; ok {
				existing.Stop()
				delete(t.state.checkTasks, check.CheckID)
			}
			if chkType.Interval < checks.MinInterval {
				t.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval))
				chkType.Interval = checks.MinInterval
			}

			if t.state.dockerClient == nil {
				dc, err := checks.NewDockerClient(os.Getenv("DOCKER_HOST"), checks.BufSize)
				if err != nil {
					t.logger.Printf("[ERR] agent: error creating docker client: %s", err)
					return err
				}
				t.logger.Printf("[DEBUG] agent: created docker client for %s", dc.Host())
				t.state.dockerClient = dc
			}

			dockerCheck := &checks.CheckDocker{
				Notify:            checkNotifier{t},
				CheckID:           check.CheckID,
				DockerContainerID: chkType.DockerContainerID,
				Shell:             chkType.Shell,
				ScriptArgs:        chkType.ScriptArgs,
				Interval:          chkType.Interval,
				Logger:            t.logger,
				Client:            t.state.dockerClient,
			}
			if prev := t.state.checkTasks[check.CheckID]; prev != nil {
				prev.Stop()
			}
			dockerCheck.Start()
			t.state.checkTasks[check.CheckID] = dockerCheck

		case chkType.IsMonitor():
			if existing, ok := t.state.checkTasks[check.CheckID]; ok {
				existing.Stop()
				delete(t.state.checkTasks, check.CheckID)
			}
			if chkType.Interval < checks.MinInterval {
				t.logger.Printf("[WARN] agent: check '%s' has interval below minimum of %v",
					check.CheckID, checks.MinInterval)
				chkType.Interval = checks.MinInterval
			}

			monitor := &checks.CheckMonitor{
				Notify:     checkNotifier{t},
				CheckID:    check.CheckID,
				ScriptArgs: chkType.ScriptArgs,
				Interval:   chkType.Interval,
				Timeout:    chkType.Timeout,
				Logger:     t.logger,
			}
			monitor.Start()
			t.state.checkTasks[check.CheckID] = monitor

		case chkType.IsAlias():
			if existing, ok := t.state.checkTasks[check.CheckID]; ok {
				existing.Stop()
				delete(t.state.checkTasks, check.CheckID)
			}

			var rpcReq structs.NodeSpecificRequest
			rpcReq.Datacenter = t.state.config.Datacenter

			// The token to set is really important. The behavior below follows
			// the same behavior as anti-entropy: we use the user-specified token
			// if set (either on the service or check definition), otherwise
			// we use the "UserToken" on the agent. This is tested.
			rpcReq.Token = t.state.tokens.UserToken()
			if token != "" {
				rpcReq.Token = token
			}

			chkImpl := &checks.CheckAlias{
				//Notify:    checkNotifier{t}, TODO
				RPC:       t.state.delegate,
				RPCReq:    rpcReq,
				CheckID:   check.CheckID,
				Node:      chkType.AliasNode,
				ServiceID: chkType.AliasService,
			}
			chkImpl.Start()
			t.state.checkTasks[check.CheckID] = chkImpl

		default:
			return fmt.Errorf("Check type is not valid")
		}

		if chkType.DeregisterCriticalServiceAfter > 0 {
			timeout := chkType.DeregisterCriticalServiceAfter
			if timeout < t.state.config.CheckDeregisterIntervalMin {
				timeout = t.state.config.CheckDeregisterIntervalMin
				t.logger.Println(fmt.Sprintf("[WARN] agent: check '%s' has deregister interval below minimum of %v",
					check.CheckID, t.state.config.CheckDeregisterIntervalMin))
			}
			t.state.checkReapAfter[check.CheckID] = timeout
		} else {
			delete(t.state.checkReapAfter, check.CheckID)
		}
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
		l.checkAliases[srcServiceID] = m
	}
	m[checkID] = notifyCh

	return nil
}

func (t *WriteTx) setupTLSClientConfig(skipVerify bool) (tlsClientConfig *tls.Config, err error) {
	// We re-use the API client's TLS structure since it
	// closely aligns with Consul's internal configuration.
	tlsConfig := &api.TLSConfig{
		InsecureSkipVerify: skipVerify,
	}
	if t.state.config.EnableAgentTLSForChecks {
		tlsConfig.Address = t.state.config.ServerName
		tlsConfig.KeyFile = t.state.config.KeyFile
		tlsConfig.CertFile = t.state.config.CertFile
		tlsConfig.CAFile = t.state.config.CAFile
		tlsConfig.CAPath = t.state.config.CAPath
	}
	tlsClientConfig, err = api.SetupTLSConfig(tlsConfig)
	return
}
