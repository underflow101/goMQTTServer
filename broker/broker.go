package broker

import (
	"crypto/tls"
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	MessagePoolNum        = 1024
	MessagePoolMessageNum = 1024
)

type Message struct {
	client *client
	packet packets.ControlPacket
}

type Broker struct {
	id        string
	mu        sync.Mutex
	config    *Config
	tlsConfig *tls.Config
	wpool     *pool.WorkerPool
}
