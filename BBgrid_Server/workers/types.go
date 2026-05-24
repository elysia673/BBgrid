package workers

import "BBgrid/common/proto"

// Dispatcher 通用事件分发器接口
//
// 基于 ResourceKey 的事件分发，完全不理解业务逻辑。
// 只做一件事：根据资源类型路由事件到对应的 handler。
type Dispatcher interface {
	// SubscribeByType 按资源类型订阅事件
	SubscribeByType(resourceType string, handler func(proto.GenericEvent))

	// Dispatch 分发事件 (按资源类型路由)
	Dispatch(event proto.GenericEvent)
}
