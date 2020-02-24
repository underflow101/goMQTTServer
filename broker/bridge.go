package broker

import (
	"github.com/underflow101/goMQTTServer/plugins/bridge"
	"go.uber.org/zap"
)

func (b *Broker) Publish(e *bridge.Elements) {
	if b.bridgeMQ != nil {
		err := b.bridgeMQ.Publish(e)
		if err != nil {
			log.Error("send message to mq error.", zap.Error(err))
		}
	}
}
