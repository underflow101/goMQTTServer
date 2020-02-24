package sessions

import (
	"fmt"
	"sync"
)

var _ SessionsProvider = (*memProvider)(nil)

type memProvider struct {
	st map[string]*Session
	mu sync.RWMutex
}

func init() {
	Register("mem", NewMemProvider())
}

func NewMemProvider() *memProvider {
	return &memProvider{
		st: make(map[string]*Session),
	}
}

func (self *memProvider) New(id string) (*Session, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.st[id] = &Session{id: id}
	return self.st[id], nil
}

func (self *memProvider) Get(id string) (*Session, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	sess, ok := self.st[id]
	if !ok {
		return nil, fmt.Errorf("store/Get: No session found for key: %s", id)
	}

	return sess, nil
}

func (self *memProvider) Del(id string) {
	self.mu.Lock()
	defer self.mu.Unlock()

	delete(self.st, id)
}

func (self *memProvider) Save(id string) error {
	return nil
}

func (self *memProvider) Count() int {
	return len(self.st)
}

func (self *memProvider) Close() error {
	self.st = make(map[string]*Session)
	return nil
}
