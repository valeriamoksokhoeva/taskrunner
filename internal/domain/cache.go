package domain

import "sync"


func NewStore() *Store {
	return &Store{items: make(map[string]TaskResult)}
}

func (s *Store) Set(key string, value TaskResult){
	s.rwm.Lock()
	defer s.rwm.Unlock()
	s.items[key] = value
	
}

func (s *Store) Get(key string) (TaskResult, bool) {
	s.rwm.RLock()
	defer s.rwm.RUnlock()
	res, ok := s.items[key]
	return res, ok
}

func (s *Store) Delete(key string) {
	s.rwm.Lock()
	defer s.rwm.Unlock()
	delete(s.items, key)
	
}

func NewStoreSync() *StoreSync {
	return &StoreSync{m: sync.Map{}}
}

func (s *StoreSync) Set(key string, value TaskResult) {
	s.m.Store(key, value)
}

func (s *StoreSync) Get(key string) (TaskResult, bool) {
	res, ok := s.m.Load(key)
	if !ok {
		return TaskResult{}, ok
	}
	return res.(TaskResult), ok
}

func (s *StoreSync) Delete(key string) {
	s.m.Delete(key)
}