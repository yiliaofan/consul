
// persistedService is used to wrap a service definition and bundle it
// with an ACL token so we can restore both at a later agent start.
type persistedService struct {
	Token   string
	Service *structs.NodeService
}

// persistService saves a service definition to a JSON file in the data dir
func (a *Agent) persistService(service *structs.NodeService) error {
	svcPath := filepath.Join(a.config.DataDir, servicesDir, stringHash(service.ID))

	wrapped := persistedService{
		Token:   a.State.ServiceToken(service.ID),
		Service: service,
	}
	encoded, err := json.Marshal(wrapped)
	if err != nil {
		return err
	}

	return file.WriteAtomic(svcPath, encoded)
}

// purgeService removes a persisted service definition file from the data dir
func (a *Agent) purgeService(serviceID string) error {
	svcPath := filepath.Join(a.config.DataDir, servicesDir, stringHash(serviceID))
	if _, err := os.Stat(svcPath); err == nil {
		return os.Remove(svcPath)
	}
	return nil
}

// persistedProxy is used to wrap a proxy definition and bundle it with an Proxy
// token so we can continue to authenticate the running proxy after a restart.
type persistedProxy struct {
	ProxyToken string
	Proxy      *structs.ConnectManagedProxy

	// Set to true when the proxy information originated from the agents configuration
	// as opposed to API registration.
	FromFile bool
}

// persistProxy saves a proxy definition to a JSON file in the data dir
func (a *Agent) persistProxy(proxy *local.ManagedProxy, FromFile bool) error {
	proxyPath := filepath.Join(a.config.DataDir, proxyDir,
		stringHash(proxy.Proxy.ProxyService.ID))

	wrapped := persistedProxy{
		ProxyToken: proxy.ProxyToken,
		Proxy:      proxy.Proxy,
		FromFile:   FromFile,
	}
	encoded, err := json.Marshal(wrapped)
	if err != nil {
		return err
	}

	return file.WriteAtomic(proxyPath, encoded)
}

// purgeProxy removes a persisted proxy definition file from the data dir
func (a *Agent) purgeProxy(proxyID string) error {
	proxyPath := filepath.Join(a.config.DataDir, proxyDir, stringHash(proxyID))
	if _, err := os.Stat(proxyPath); err == nil {
		return os.Remove(proxyPath)
	}
	return nil
}

// persistCheck saves a check definition to the local agent's state directory
func (a *Agent) persistCheck(check *structs.HealthCheck, chkType *structs.CheckType) error {
	checkPath := filepath.Join(a.config.DataDir, checksDir, checkIDHash(check.CheckID))

	// Create the persisted check
	wrapped := persistedCheck{
		Check:   check,
		ChkType: chkType,
		Token:   a.State.CheckToken(check.CheckID),
	}

	encoded, err := json.Marshal(wrapped)
	if err != nil {
		return err
	}

	return file.WriteAtomic(checkPath, encoded)
}

// purgeCheck removes a persisted check definition file from the data dir
func (a *Agent) purgeCheck(checkID types.CheckID) error {
	checkPath := filepath.Join(a.config.DataDir, checksDir, checkIDHash(checkID))
	if _, err := os.Stat(checkPath); err == nil {
		return os.Remove(checkPath)
	}
	return nil
}