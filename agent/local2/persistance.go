package local

import "github.com/hashicorp/consul/types"

type persistanceManager struct {
}

func (p *persistanceManager) loadCheckState(id types.CheckID) (string, string, error) {
	return "", "", nil
}

func (p *persistanceManager) purgeCheckState() error {
	return nil
}
