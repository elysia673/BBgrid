package runtime

import (
	"BBgrid/common/runtime/proto/pb"
	"context"
	"sync"

	alog "BBgrid/common/log"
)

// TopologyServer 实现 gRPC TopologyService
//
// 职责：
// - 管理 topology 意图状态（namespace 应该形成怎样的连接关系）
// - 向连接的 Runtime 推送 topology 更新
//
// 边界：
//   - Topology ≠ Namespace。AuthWorker 管理 namespace（客户端分组、认证），
//     TopologyServer 管理 topology（连接意图）。
//   - 两者共享 "namespace" 这个名字作为关联键，但各自独立管理。
//   - Server 不负责实现意图（IP 分配、密钥生成、peer 构建），
//     这些全部由 Runtime 自行决定。
type TopologyServer struct {
	pb.UnimplementedTopologyServiceServer

	mu         sync.RWMutex
	topologies map[string]*pb.Topology // namespace -> topology

	// 活跃的 subscribers
	subMu       sync.RWMutex
	subscribers map[string]chan *pb.TopologyUpdate // runtimeID -> update channel
}

// NewTopologyServer 创建 TopologyServer
func NewTopologyServer() *TopologyServer {
	return &TopologyServer{
		topologies:  make(map[string]*pb.Topology),
		subscribers: make(map[string]chan *pb.TopologyUpdate),
	}
}

// Subscribe 实现 gRPC streaming，Runtime 连接后持续接收更新
func (s *TopologyServer) Subscribe(req *pb.SubscribeRequest, stream pb.TopologyService_SubscribeServer) error {
	runtimeID := req.RuntimeId
	if runtimeID == "" {
		runtimeID = "anonymous"
	}

	alog.Info(alog.CatSystem, "Runtime 已订阅", "runtimeID", runtimeID)

	// 注册 subscriber（如果同 ID 已存在，关闭旧的）
	updateCh := make(chan *pb.TopologyUpdate, 16)
	s.subMu.Lock()
	if oldCh, exists := s.subscribers[runtimeID]; exists {
		close(oldCh)
		alog.Warn(alog.CatSystem, "Runtime 重复连接，关闭旧连接", "runtimeID", runtimeID)
	}
	s.subscribers[runtimeID] = updateCh
	s.subMu.Unlock()

	defer func() {
		s.subMu.Lock()
		if s.subscribers[runtimeID] == updateCh {
			delete(s.subscribers, runtimeID)
		}
		s.subMu.Unlock()
		close(updateCh)
		alog.Info(alog.CatSystem, "Runtime 已断开", "runtimeID", runtimeID)
	}()

	// 首次连接，发送全量快照
	snapshot := s.buildFullUpdate()
	if err := stream.Send(snapshot); err != nil {
		return err
	}

	// 持续推送增量更新
	for update := range updateCh {
		if err := stream.Send(update); err != nil {
			return err
		}
	}

	return nil
}

// ==================== topology 管理 API ====================

// ApplyTopology 创建或更新一个 topology 意图
func (s *TopologyServer) ApplyTopology(topo *pb.Topology) {
	if topo.Intent == nil {
		topo.Intent = &pb.Intent{
			Connectivity: pb.Connectivity_MESH,
			Exposure:     pb.Exposure_INTERNAL,
			RelayPolicy:  pb.RelayPolicy_AUTO,
		}
	}

	s.mu.Lock()
	s.topologies[topo.Namespace] = topo
	s.mu.Unlock()

	alog.Info(alog.CatSystem, "Topology 已应用",
		"namespace", topo.Namespace,
		"connectivity", topo.Intent.Connectivity,
		"members", len(topo.Members))
	s.notifySubscribers(&pb.TopologyUpdate{
		Type:       pb.UpdateType_PATCH,
		Topologies: []*pb.Topology{topo},
	})
}

// DeleteTopology 删除一个 topology
func (s *TopologyServer) DeleteTopology(namespace string) {
	s.mu.Lock()
	delete(s.topologies, namespace)
	s.mu.Unlock()

	alog.Info(alog.CatSystem, "Topology 已删除", "namespace", namespace)
	s.notifySubscribers(&pb.TopologyUpdate{
		Type:              pb.UpdateType_PATCH,
		DeletedNamespaces: []string{namespace},
	})
}

// GetTopology 获取指定 namespace 的 topology
func (s *TopologyServer) GetTopology(namespace string) (*pb.Topology, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	topo, ok := s.topologies[namespace]
	return topo, ok
}

// GetAllTopologies 列出所有 topology
func (s *TopologyServer) GetAllTopologies() []*pb.Topology {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list := make([]*pb.Topology, 0, len(s.topologies))
	for _, topo := range s.topologies {
		list = append(list, topo)
	}
	return list
}

// ListTopologies 实现 gRPC ListTopologies 方法
func (s *TopologyServer) ListTopologies(ctx context.Context, req *pb.ListTopologiesRequest) (*pb.ListTopologiesResponse, error) {
	topologies := s.GetAllTopologies()
	return &pb.ListTopologiesResponse{
		Topologies: topologies,
	}, nil
}

// ==================== 内部方法 ====================

// buildFullUpdate 构建全量快照
func (s *TopologyServer) buildFullUpdate() *pb.TopologyUpdate {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topologies := make([]*pb.Topology, 0, len(s.topologies))
	for _, topo := range s.topologies {
		topologies = append(topologies, topo)
	}
	return &pb.TopologyUpdate{
		Type:       pb.UpdateType_FULL,
		Topologies: topologies,
	}
}

// notifySubscribers 向所有 subscriber 推送更新
func (s *TopologyServer) notifySubscribers(update *pb.TopologyUpdate) {
	s.subMu.RLock()
	defer s.subMu.RUnlock()

	for runtimeID, ch := range s.subscribers {
		select {
		case ch <- update:
		default:
			alog.Warn(alog.CatSystem, "Runtime 更新队列已满，丢弃", "runtimeID", runtimeID)
		}
	}
}
