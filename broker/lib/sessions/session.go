package sessions

import (
	"fmt"
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	defaultQueueSize = 16
)

type Session struct {
	didInit  bool
	id       string
	topics   map[string]byte
	cmsg     *packets.ConnectPacket
	Will     *packets.PublishPacket
	Retained *packets.PublishPacket
	mu       sync.Mutex
}

func (self *Session) Initialization(msg *packets.ConnectPacket) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	if self.didInit {
		return fmt.Errorf("Session already initialized")
	}

	self.cmsg = msg

	if self.cmsg.WillFlag {
		self.Will = packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		self.Will.Qos = self.cmsg.Qos
		self.Will.TopicName = self.cmsg.WillTopic
		self.Will.Payload = self.cmsg.WillMessage
		self.Will.Retain = self.cmsg.WillRetain
	}

	self.topics = make(map[string]byte, 1)
	self.id = string(msg.ClientIdentifier)
	self.didInit = true

	return nil
}

func (self *Session) Update(msg *packets.ConnectPacket) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.cmsg = msg
	return nil
}

func (self *Session) RetainMessage(msg *packets.PublishPacket) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.Retained = msg

	return nil
}

func (self *Session) AddTopic(topic string, qos byte) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	if !self.didInit {
		return fmt.Errorf("Session not yet initialized")
	}

	self.topics[topic] = qos

	return nil
}

func (self *Session) RemoveTopic(topic string) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	if !self.didInit {
		return fmt.Errorf("Session not yet initialized")
	}

	delete(self.topics, topic)

	return nil
}

func (self *Session) Topics() ([]string, []byte, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	if !self.didInit {
		return nil, nil, fmt.Errorf("Session not yet initialized")
	}

	var (
		topics []string
		qoss   []byte
	)

	for k, v := range self.topics {
		topics = append(topics, k)
		qoss = append(qoss, v)
	}

	return topics, qoss, nil
}

func (self *Session) ID() string {
	return self.cmsg.ClientIdentifier
}

func (self *Session) WillFlag() bool {
	self.mu.Lock()
	defer self.mu.Unlock()

	return self.cmsg.WillFlag
}

func (self *Session) SetWillFlag(v bool) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.cmsg.WillFlag = v
}

func (self *Session) CleanSession() bool {
	self.mu.Lock()
	defer self.mu.Unlock()

	return self.cmsg.CleanSession
}
