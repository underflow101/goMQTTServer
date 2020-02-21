package sessions

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

var (
	ErrSessionsProviderNotFound = errors.New("Session: Session provider not found")
	ErrKeyNotAvailable          = errors.New("Session: not item found for key.")

	providers = make(map[string]SessionsProvider)
)

type SessionsProvider interface {
	New(id string) (*Session, error)
	Get(id string) (*Session, error)
	Del(id string)
	Save(id string) error
	Count() int
	Close() error
}

type Manager struct {
	p SessionsProvider
}

func Register(name string, provider SessionsProvider) {
	if provider == nil {
		panic("Session: Register provide is NULL")
	}
	if _, dup := providers[name]; dup {
		panic("Session: Register called twice for provider: " + name)
	}
	providers[name] = provider
}

func Unregister(name string) {
	delete(providers, name)
}

func NewManager(providerName string) (*Manager, error) {
	p, ok := providers[providerName]
	if !ok {
		return nil, fmt.Errorf("Session: unknown provider: %q", providerName)
	}
	return &Manager{p: p}, nil
}

func (self *Manager) New(id string) (*Session, error) {
	if id == "" {
		id = self.sessionId()
	}
	return self.p.New(id)
}

func (self *Manager) Get(id string) (*Session, error) {
	return self.p.Get(id)
}

func (self *Manager) Del(id string) {
	self.p.Del(id)
}

func (self *Manager) Save(id string) error {
	return self.p.Save(id)
}

func (self *Manager) Count() int {
	return self.p.Count()
}

func (self *Manager) Close() error {
	return self.p.Close()
}

func (manager *Manager) sessionId() string {
	b := make([]byte, 15)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return base64.URLEncoding.EncodeToString(b)
}
