package workers

import "BBgrid/common/persist"

// registerRelayProviderGlobal 注册 relay provider 到全局注册表
func registerRelayProviderGlobal(state *StateWorker) {
	persist.Register(&relayProvider{state: state})
}

// registerProxyProviderGlobal 注册 proxy provider 到全局注册表
func registerProxyProviderGlobal(state *StateWorker) {
	persist.Register(&proxyProvider{state: state})
}
