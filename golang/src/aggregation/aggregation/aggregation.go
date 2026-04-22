package aggregation

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

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	sumAmount     int
	topSize       int
	outputQueue   middleware.Middleware
	inputQueue    middleware.Middleware
	clientsFruits map[int]map[string]fruititem.FruitItem
	clientsEOF    map[int]int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputQueue, err := middleware.CreateQueueMiddleware(fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id), connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		sumAmount:     config.SumAmount,
		topSize:       config.TopSize,
		outputQueue:   outputQueue,
		inputQueue:    inputQueue,
		clientsFruits: map[int]map[string]fruititem.FruitItem{},
		clientsEOF:    map[int]int{},
	}, nil
}

func (a *Aggregation) Run() {
	go a.handleSignals()
	defer a.close()

	if err := a.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		a.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming from input exchange", "err", err)
	}
}

func (a *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	a.inputQueue.StopConsuming()
}

func (a *Aggregation) close() {
	a.inputQueue.Close()
	a.outputQueue.Close()
}

func (a *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()
	slog.Info("Received message", "msg", msg.Body)

	m, err := inner.DeserializePipeline(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if m.IsEOF() {
		if err := a.handleEOF(m.ClientID); err != nil {
			slog.Error("While handling EOF", "err", err, "cid", m.ClientID)
		}
		return
	}

	a.handleDataMessage(m.ClientID, m.Records)
}

func (a *Aggregation) handleEOF(clientID int) error {
	a.clientsEOF[clientID]++
	if a.clientsEOF[clientID] < a.sumAmount {
		return nil
	}

	slog.Info("All Sums reported EOF, emitting top", "cid", clientID)

	topMsg, err := inner.SerializePipeline(inner.PipelineMsg{
		ClientID: clientID,
		Records:  a.buildFruitTop(clientID),
	})
	if err != nil {
		return fmt.Errorf("serializing top for client %d: %w", clientID, err)
	}
	if err := a.outputQueue.Send(*topMsg); err != nil {
		return fmt.Errorf("sending top for client %d: %w", clientID, err)
	}

	delete(a.clientsFruits, clientID)
	delete(a.clientsEOF, clientID)
	return nil
}

func (a *Aggregation) handleDataMessage(clientID int, fruitRecords []fruititem.FruitItem) {
	fruits, ok := a.clientsFruits[clientID]
	if !ok {
		fruits = map[string]fruititem.FruitItem{}
		a.clientsFruits[clientID] = fruits
	}
	for _, record := range fruitRecords {
		if fruit, ok := fruits[record.Fruit]; ok {
			fruits[record.Fruit] = fruit.Sum(record)
		} else {
			fruits[record.Fruit] = record
		}
	}
}

func (a *Aggregation) buildFruitTop(clientID int) []fruititem.FruitItem {
	fruits := a.clientsFruits[clientID]
	fruitItems := make([]fruititem.FruitItem, 0, len(fruits))
	for _, item := range fruits {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(a.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
