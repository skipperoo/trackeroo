package service

import (
	"sync"
)

type SimpleCache struct {
	items map[string]any
	m     sync.RWMutex
}

func NewSimpleCache() *SimpleCache {
	return &SimpleCache{
		items: make(map[string]any),
		m:     sync.RWMutex{},
	}
}

func (kc *SimpleCache) Get(key string) any {
	kc.m.RLock()
	defer kc.m.RUnlock()
	item, ok := kc.items[key]
	if !ok {
		return nil
	}
	return item
}

func (kc *SimpleCache) Set(key string, value any) {
	kc.m.Lock()
	defer kc.m.Unlock()
	kc.items[key] = value
}

func (kc *SimpleCache) Invalidate(key string) {
	kc.m.Lock()
	defer kc.m.Unlock()
	delete(kc.items, key)
}

func (kc *SimpleCache) Clear() {
	kc.m.Lock()
	defer kc.m.Unlock()
	kc.items = make(map[string]any)
}
