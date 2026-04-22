package join

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	aggregationAmount int
	topSize           int
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	clientsFruits     map[int][]fruititem.FruitItem
	clientsEOF        map[int]int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		clientsFruits:     map[int][]fruititem.FruitItem{},
		clientsEOF:        map[int]int{},
	}, nil
}

func (j *Join) Run() {
	go j.handleSignals()
	defer j.close()

	if err := j.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		j.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (j *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	j.inputQueue.StopConsuming()
}

func (j *Join) close() {
	j.inputQueue.Close()
	j.outputQueue.Close()
}

func (j *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	m, err := inner.DeserializePipeline(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if !m.IsEOF() {
		j.clientsFruits[m.ClientID] = append(j.clientsFruits[m.ClientID], m.Records...)
	}
	j.clientsEOF[m.ClientID]++
	if j.clientsEOF[m.ClientID] < j.aggregationAmount {
		return
	}

	if err := j.emitFinalTop(m.ClientID); err != nil {
		slog.Error("While emitting final top", "err", err, "cid", m.ClientID)
	}
}

func (j *Join) emitFinalTop(clientID int) error {
	slog.Info("All Aggregations reported, emitting final top", "cid", clientID)

	finalTop := j.buildFinalTop(clientID)
	topMsg, err := inner.SerializePipeline(inner.PipelineMsg{
		ClientID: clientID,
		Records:  finalTop,
	})
	if err != nil {
		return fmt.Errorf("serializing final top for client %d: %w", clientID, err)
	}
	if err := j.outputQueue.Send(*topMsg); err != nil {
		return fmt.Errorf("sending final top for client %d: %w", clientID, err)
	}

	delete(j.clientsFruits, clientID)
	delete(j.clientsEOF, clientID)
	return nil
}

func (j *Join) buildFinalTop(clientID int) []fruititem.FruitItem {
	items := j.clientsFruits[clientID]
	sort.SliceStable(items, func(i, k int) bool {
		return items[k].Less(items[i])
	})
	finalTopSize := min(j.topSize, len(items))
	return items[:finalTopSize]
}
