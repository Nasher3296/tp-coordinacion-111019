package messagehandler

import (
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var nextClientID atomic.Int32

type MessageHandler struct {
	clientID  int
	dataCount uint64
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		clientID: int(nextClientID.Add(1)),
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	messageHandler.dataCount++
	return inner.SerializePipeline(inner.PipelineMsg{
		ClientID: messageHandler.clientID,
		Records:  []fruititem.FruitItem{fruitRecord},
	})
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializePipeline(inner.PipelineMsg{
		ClientID:      messageHandler.clientID,
		ExpectedCount: messageHandler.dataCount,
	})
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	m, err := inner.DeserializePipeline(message)
	if err != nil {
		return nil, err
	}
	if m.ClientID != messageHandler.clientID {
		return nil, nil
	}
	return m.Records, nil
}
