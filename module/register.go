package module

import "sync"

// Registrar 代表组件注册器的接口。
type Registrar interface {
	// Register 用于注册组件实例。
	Register(module Module) (bool, error)
	// Unregister 用于注销组件实例。
	Unregister(mid MID) (bool, error)
	// Get 用于获取一个指定类型的组件的实例。
	// 本函数应该基于负载均衡策略返回实例。
	Get(moduleType Type) (Module, error)
	// GetAllByType 用于获取指定类型的所有组件实例。
	GetAllByType(moduleType Type) (map[MID]Module, error)
	// GetAll 用于获取所有组件实例。
	GetAll() map[MID]Module
	// Clear 会清除所有的组件注册记录。
	Clear()
}

// myRegistrar 代表组件注册器的实现类型。
type myRegistrar struct {
	// moduleTypeMap 代表组件类型与对应组件实例的映射。
	moduleTypeMap map[Type]map[MID]Module
	// rwlock 代表组件注册专用读写锁。
	rwlock sync.RWMutex
}