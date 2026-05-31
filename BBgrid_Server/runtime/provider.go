package runtime

import (
	"BBgrid/common/proto"
	"fmt"
)

// EventBusReconcileProvider 通过 EventBus 创建资源的 ReconcileProvider
//
// 当 ReconcileEngine 发现 desired state 存在但 actual state 不存在时，
// 通过 EventBus 发布 ADDED 事件来创建资源。
type EventBusReconcileProvider struct {
	core *Core
}

// NewEventBusReconcileProvider 创建 EventBus ReconcileProvider
func NewEventBusReconcileProvider(core *Core) *EventBusReconcileProvider {
	return &EventBusReconcileProvider{core: core}
}

// CreateProxy 创建代理
func (p *EventBusReconcileProvider) CreateProxy(clientID string, proxy ProxyState) error {
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeProxy,
			Namespace: proto.NamespaceDefault,
			Name:      fmt.Sprintf("%s:%d", clientID, proxy.RemotePort),
		},
		proto.EventAdded,
		proxy,
	)
	p.core.Publish(event)
	return nil
}

// DeleteProxy 删除代理
func (p *EventBusReconcileProvider) DeleteProxy(clientID string, port int) error {
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeProxy,
			Namespace: proto.NamespaceDefault,
			Name:      fmt.Sprintf("%s:%d", clientID, port),
		},
		proto.EventDeleted,
		nil,
	)
	p.core.Publish(event)
	return nil
}

// CreateRelay 创建中继
func (p *EventBusReconcileProvider) CreateRelay(session RelaySession) error {
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeRelay,
			Namespace: proto.NamespaceDefault,
			Name:      session.ID,
		},
		proto.EventAdded,
		session,
	)
	p.core.Publish(event)
	return nil
}

// DeleteRelay 删除中继
func (p *EventBusReconcileProvider) DeleteRelay(sessionID string) error {
	event := proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeRelay,
			Namespace: proto.NamespaceDefault,
			Name:      sessionID,
		},
		proto.EventDeleted,
		nil,
	)
	p.core.Publish(event)
	return nil
}
