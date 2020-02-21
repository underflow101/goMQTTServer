package topics

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	QosAtMostOnce byte = iota
	QosAtLeastOnce
	QosExactlyOnce
	QosFailure = 0x80
)

const (
	/*******************************
	 * Description is in topics.go
	 *******************************/
	stateCHR byte = iota
	stateMWC
	stateSWC
	stateSEP
	stateSYS
)

type memTopics struct {
	/****************************************
	 * smu: Sub/Unsub mutex
	 * sroot: Subscription tree
	 * rmu: Retained message mutex
	 * rroot: Retained messages topic tree
	 ****************************************/
	smu   sync.RWMutex
	sroot *snode
	rmu   sync.RWMutex
	rroot *rnode
}

type snode struct {
	subs   []interface{}
	qos    []byte
	snodes map[string]*snode
}

type rnode struct {
	msg    *packets.PublishPacket
	rnodes map[string]*rnode
}

var _ TopicsProvider = (*memTopics)(nil)

func init() {
	Register("mem", NewMemProvider())
}

func NewMemProvider() *memTopics {
	return &memTopics{
		sroot: newSNode(),
		rroot: newRNode(),
	}
}

func ValidQos(qos byte) bool {
	return qos == QosAtMostOnce || qos == QosAtLeastOnce || qos == QosExactlyOnce
}

func (self *memTopics) Subscribe(topic []byte, qos byte, sub interface{}) (byte, error) {
	if !ValidQos(qos) {
		return QosFailure, fmt.Errorf("Invalid QoS %d", qos)
	}
	if sub == nil {
		return QosFailure, fmt.Errorf("Subsciber cannot be NULL")
	}

	self.smu.Lock()
	defer self.smu.Unlock()

	if qos > QosExactlyOnce {
		qos = QosExactlyOnce
	}

	if err := self.sroot.sinsert(topic, qos, sub); err != nil {
		return QosFailure, err
	}

	return qos, nil
}

func (self *memTopics) Unsubscribe(topic []byte, sub interface{}) error {
	self.smu.Lock()
	defer self.smu.Unlock()

	return self.sroot.sremove(topic, sub)
}

func (self *memTopics) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	if !ValidQos(qos) {
		return fmt.Errorf("Invalid QoS: %d", qos)
	}

	self.smu.RLock()
	defer self.smu.RUnlock()

	*subs = (*subs)[0:0]
	*qoss = (*qoss)[0:0]

	return self.sroot.smatch(topic, qos, subs, qoss)
}

func (self *memTopics) Retain(msg *packets.PublishPacket) error {
	self.rmu.Lock()
	defer self.rmu.Unlock()

	if len(msg.Payload) == 0 {
		return self.rroot.rremove([]byte(msg.TopicName))
	}
	return self.rroot.rinsertOrUpdate([]byte(msg.TopicName), msg)
}

func (self *memTopics) Retained(topic []byte, msgs *[]*packets.PublishPacket) error {
	self.rmu.RLock()
	defer self.rmu.RUnlock()

	return self.rroot.rmatch(topic, msgs)
}

func (self *memTopics) Close() error {
	self.sroot = nil
	self.rroot = nil
	return nil
}

func newSNode() *snode {
	return &snode{
		snodes: make(map[string]*snode),
	}
}

func (self *snode) sinsert(topic []byte, qos byte, sub interface{}) error {
	if len(topic) == 0 {
		for i := range self.subs {
			if equal(self.subs[i], sub) {
				self.qos[i] = qos
				return nil
			}
		}
		self.subs = append(self.subs, sub)
		self.qos = append(self.qos, qos)

		return nil
	}

	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}
	level := string(ntl)
	n, ok := self.snodes[level]
	if !ok {
		n = newSNode()
		self.snodes[level] = n
	}

	return n.sinsert(rem, qos, sub)
}

func (self *snode) sremove(topic []byte, sub interface{}) error {
	if len(topic) == 0 {
		if sub == nil {
			self.subs = self.subs[0:0]
			self.qos = self.qos[0:0]
			return nil
		}

		for i := range self.subs {
			if equal(self.subs[i], sub) {
				self.subs = append(self.subs[:i], self.subs[i+1:]...)
				self.qos = append(self.qos[:i], self.qos[i+1:]...)
				return nil
			}
		}
		return fmt.Errorf("No topic found for subscriber")
	}
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)
	n, ok := self.snodes[level]
	if !ok {
		return fmt.Errorf("No topic found")
	}

	if err := n.sremove(rem, sub); err != nil {
		return err
	}

	if len(n.subs) == 0 && len(n.snodes) == 0 {
		delete(self.snodes, level)
	}
	return nil
}

func (self *snode) smatch(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	if len(topic) == 0 {
		self.matchQos(qos, subs, qoss)
		if mwcn, _ := self.snodes[MWC]; mwcn != nil {
			mwcn.matchQos(qos, subs, qoss)
		}
		return nil
	}

	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	for k, n := range self.snodes {
		if k == MWC {
			n.matchQos(qos, subs, qoss)
		} else if k == SWC || k == level {
			if err := n.smatch(rem, qos, subs, qoss); err != nil {
				return err
			}
		}
	}

	return nil
}

func newRNode() *rnode {
	return &rnode{
		rnodes: make(map[string]*rnode),
	}
}

func (self *rnode) rinsertOrUpdate(topic []byte, msg *packets.PublishPacket) error {
	if len(topic) == 0 {
		self.msg = msg
		return nil
	}

	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)
	n, ok := self.rnodes[level]
	if !ok {
		n = newRNode()
		self.rnodes[level] = n
	}

	return n.rinsertOrUpdate(rem, msg)
}

func (self *rnode) rremove(topic []byte) error {
	if len(topic) == 0 {
		self.msg = nil
		return nil
	}

	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	n, ok := self.rnodes[level]
	if !ok {
		return fmt.Errorf("No topic found")
	}

	if err := n.rremove(rem); err != nil {
		return err
	}

	if len(n.rnodes) == 0 {
		delete(self.rnodes, level)
	}

	return nil
}

func (self *rnode) rmatch(topic []byte, msgs *[]*packets.PublishPacket) error {
	if len(topic) == 0 {
		if self.msg != nil {
			*msgs = append(*msgs, self.msg)
		}
		return nil
	}

	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	if level == MWC {
		self.allRetained(msgs)
	} else if level == SWC {
		for _, n := range self.rnodes {
			if err := n.rmatch(rem, msgs); err != nil {
				return err
			}
		}
	} else {
		if n, ok := self.rnodes[level]; ok {
			if err := n.rmatch(rem, msgs); err != nil {
				return err
			}
		}
	}

	return nil
}

func (self *rnode) allRetained(msgs *[]*packets.PublishPacket) {
	if self.msg != nil {
		*msgs = append(*msgs, self.msg)
	}

	for _, n := range self.rnodes {
		n.allRetained(msgs)
	}
}

func nextTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := stateCHR

	for i, c := range topic {
		switch c {
		case '/':
			if s == stateMWC {
				return nil, nil, fmt.Errorf("Multi-level wildcard found in topic and it's not at the last level")
			}
			if i == 0 {
				return []byte(SWC), topic[i+1:], nil
			}
			return topic[:i], topic[i+1:], nil

		case '#':
			if i != 0 {
				return nil, nil, fmt.Errorf("Wildcard character '#' must occupy entire topic level")
			}
			s = stateMWC

		case '+':
			if i != 0 {
				return nil, nil, fmt.Errorf("Wildcard character '+' must occupy entire topic level")
			}
			s = stateSWC

		case '$':
			if i == 0 {
				return nil, nil, fmt.Errorf("Cannot publish to $ topics")
			}
			s = stateSYS

		default:
			if s == stateMWC || s == stateSWC {
				return nil, nil, fmt.Errorf("Wildcard characters '#' and '+' must occupy entire topic level")
			}
			s = stateCHR
		}
	}

	return topic, nil, nil
}

func (self *snode) matchQos(qos byte, subs *[]interface{}, qoss *[]byte) {
	for _, sub := range self.subs {
		*subs = append(*subs, sub)
		*qoss = append(*qoss, qos)
	}
}

func equal(k1, k2 interface{}) bool {
	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
		return false
	}
	if reflect.ValueOf(k1).Kind() == reflect.Func {
		return &k1 == &k2
	}
	if k1 == k2 {
		return true
	}

	switch k1 := k1.(type) {
	case string:
		return k1 == k2.(string)

	case int64:
		return k1 == k2.(int64)

	case int32:
		return k1 == k2.(int32)

	case int16:
		return k1 == k2.(int16)

	case int8:
		return k1 == k2.(int8)

	case int:
		return k1 == k2.(int)

	case float32:
		return k1 == k2.(float32)

	case float64:
		return k1 == k2.(float64)

	case uint:
		return k1 == k2.(uint)

	case uint8:
		return k1 == k2.(uint8)

	case uint16:
		return k1 == k2.(uint16)

	case uint32:
		return k1 == k2.(uint32)

	case uint64:
		return k1 == k2.(uint64)

	case uintptr:
		return k1 == k2.(uintptr)
	}

	return false
}
