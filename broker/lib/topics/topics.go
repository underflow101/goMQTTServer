package topics

import (
	"fmt"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	/*******************************
	 * MWC: Multi-Level Wildcard
	 * SWC: Single-Level Wildcard
	 * SEP: Serparator
	 * SYS: System-Level Topics
	 * _WC: All Wildcards
	 ******************************/
	MWC = "#"
	SWC = "+"
	SEP = "/"
	SYS = "$"
	_WC = "#+"
)

type TopicsProvider interface {
	Subscribe(topic []byte, qos byte, subscriber interface{}) (byte, error)
	Unsubscribe(topic []byte, subscriber interface{}) error
	Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error
	Retain(msg *packets.PublishPacket) error
	Retained(topic []byte, msgs *[]*packets.PublishPacket) error
	Close() error
}

type Manager struct {
	p TopicsProvider
}

var (
	providers = make(map[string]TopicsProvider)
)

func Register(name string, provider TopicsProvider) {
	if provider == nil {
		panic("topics: Register provide is NULL")
	}
	if _, dup := providers[name]; dup {
		panic("topics: Register called twice for provider " + name)
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

func (self *Manager) Subscribe(topic []byte, qos byte, subscriber interface{}) (byte, error) {
	return self.p.Subscribe(topic, qos, subscriber)
}

func (self *Manager) Unsubscribe(topic []byte, subscriber interface{}) error {
	return self.p.Unsubscribe(topic, subscriber)
}

func (self *Manager) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	return self.p.Subscribers(topic, qos, subs, qoss)
}

func (self *Manager) Retain(msg *packets.PublishPacket) error {
	return self.p.Retain(msg)
}

func (self *Manager) Retained(topic []byte, msgs *[]*packets.PublishPacket) error {
	return self.p.Retained(topic, msgs)
}

func (self *Manager) Close() error {
	return self.p.Close()
}
