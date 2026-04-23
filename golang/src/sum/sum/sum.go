package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type clientState struct {
	data             map[string]fruititem.FruitItem
	msgCount         uint64
	expectedMsgCount uint64
	flushed          bool
	isCoord          bool
	round            int
	sumsMsgCount     map[int]uint64
}

type Sum struct {
	id                int
	sumAmount         int
	sumPrefix         string
	aggregationAmount int
	aggregationPrefix string
	mu                sync.Mutex
	inputQueue        middleware.Middleware
	outputQueues      []middleware.Middleware
	coordInQueue      middleware.Middleware
	coordOutQueues    []middleware.Middleware
	clientsState      map[int]*clientState
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueues := make([]middleware.Middleware, 0, config.AggregationAmount)
	closeOutputs := func() {
		for _, q := range outputQueues {
			q.Close()
		}
	}
	for i := range config.AggregationAmount {
		q, err := middleware.CreateQueueMiddleware(fmt.Sprintf("%s_%d", config.AggregationPrefix, i), connSettings)
		if err != nil {
			inputQueue.Close()
			closeOutputs()
			return nil, err
		}
		outputQueues = append(outputQueues, q)
	}

	coordInQueue, err := middleware.CreateQueueMiddleware(fmt.Sprintf("%s_coord_%d", config.SumPrefix, config.Id), connSettings)
	if err != nil {
		inputQueue.Close()
		closeOutputs()
		return nil, err
	}

	coordOutQueues := make([]middleware.Middleware, 0, config.SumAmount)
	closeCoordOuts := func() {
		for _, q := range coordOutQueues {
			q.Close()
		}
	}
	for i := range config.SumAmount {
		q, err := middleware.CreateQueueMiddleware(fmt.Sprintf("%s_coord_%d", config.SumPrefix, i), connSettings)
		if err != nil {
			inputQueue.Close()
			closeOutputs()
			coordInQueue.Close()
			closeCoordOuts()
			return nil, err
		}
		coordOutQueues = append(coordOutQueues, q)
	}

	return &Sum{
		id:                config.Id,
		sumAmount:         config.SumAmount,
		sumPrefix:         config.SumPrefix,
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
		inputQueue:        inputQueue,
		outputQueues:      outputQueues,
		coordInQueue:      coordInQueue,
		coordOutQueues:    coordOutQueues,
		clientsState:      map[int]*clientState{},
	}, nil
}

func (s *Sum) Run() {
	go s.handleSignals()
	defer s.close()

	go func() {
		if err := s.coordInQueue.StartConsuming(func(msg middleware.Message, ack, _ func()) {
			s.handleCoord(msg, ack)
		}); err != nil {
			slog.Error("While consuming from coord input queue", "err", err)
		}
	}()

	if err := s.inputQueue.StartConsuming(func(msg middleware.Message, ack, _ func()) {
		s.handleInput(msg, ack)
	}); err != nil {
		slog.Error("While consuming from input queue", "err", err)
	}
}

func (s *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received, stopping consumer")
	s.inputQueue.StopConsuming()
	s.coordInQueue.StopConsuming()
}

func (s *Sum) close() {
	s.inputQueue.Close()
	for _, queue := range s.outputQueues {
		queue.Close()
	}
	s.coordInQueue.Close()
	for _, queue := range s.coordOutQueues {
		queue.Close()
	}
}

func (s *Sum) stateFor(clientID int) *clientState {
	st, ok := s.clientsState[clientID]
	if !ok {
		st = &clientState{
			data:         map[string]fruititem.FruitItem{},
			sumsMsgCount: map[int]uint64{},
		}
		s.clientsState[clientID] = st
	}
	return st
}

func (s *Sum) handleInput(msg middleware.Message, ack func()) {
	defer ack()

	m, err := inner.DeserializePipeline(&msg)
	if err != nil {
		slog.Error("While deserializing pipeline message", "err", err)
		return
	}

	if m.IsEOF() {
		s.handleEOF(m.ClientID, m.ExpectedCount)
		return
	}
	s.handleRecord(m.ClientID, m.Records)
}

func (s *Sum) handleRecord(clientID int, records []fruititem.FruitItem) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.stateFor(clientID)
	if state.flushed {
		return
	}
	for _, r := range records {
		if existing, ok := state.data[r.Fruit]; ok {
			state.data[r.Fruit] = existing.Sum(r)
		} else {
			state.data[r.Fruit] = r
		}
	}
	state.msgCount++
}

func (s *Sum) handleEOF(clientID int, expected uint64) {
	s.mu.Lock()
	state := s.stateFor(clientID)
	if state.isCoord {
		s.mu.Unlock()
		return
	}
	state.expectedMsgCount = expected
	state.isCoord = true
	state.round = 1
	state.sumsMsgCount = map[int]uint64{}
	round := state.round
	s.mu.Unlock()

	slog.Info("Received EOF, sending CountQuery to peers",
		"cid", clientID, "expected", expected, "round", round)

	if s.sumAmount == 1 {
		s.checkCoordProgress(clientID)
		return
	}
	s.sendCountQuery(clientID, round)
}

func (s *Sum) handleCoord(msg middleware.Message, ack func()) {
	defer ack()

	m, err := inner.DeserializeCoord(&msg)
	if err != nil {
		slog.Error("While deserializing coord message", "err", err)
		return
	}

	switch m.Kind {
	case inner.CountQuery:
		s.handleCountQuery(m)
	case inner.CountReply:
		s.handleCountReply(m)
	case inner.Confirm:
		s.handleConfirm(m)
	default:
		slog.Warn("Unexpected kind on coord queue", "kind", m.Kind)
	}
}

func (s *Sum) handleCountQuery(m inner.CoordMsg) {
	if m.CoordID == s.id {
		return
	}

	s.mu.Lock()
	state, ok := s.clientsState[m.ClientID]
	var count uint64
	flushed := false
	if ok {
		count = state.msgCount
		flushed = state.flushed
	}
	s.mu.Unlock()

	if flushed {
		return
	}

	s.sendCountReply(m.CoordID, m.ClientID, m.Round, count)
}

func (s *Sum) handleCountReply(m inner.CoordMsg) {
	s.mu.Lock()
	state, ok := s.clientsState[m.ClientID]
	if !ok || !state.isCoord || m.Round != state.round {
		s.mu.Unlock()
		return
	}
	state.sumsMsgCount[m.SenderID] = m.Count
	s.mu.Unlock()

	s.checkCoordProgress(m.ClientID)
}

func (s *Sum) checkCoordProgress(clientID int) {
	s.mu.Lock()
	state, ok := s.clientsState[clientID]
	if !ok || !state.isCoord || state.flushed {
		return
	}
	if len(state.sumsMsgCount) < s.sumAmount-1 {
		s.mu.Unlock()
		return
	}

	total := state.msgCount
	for _, c := range state.sumsMsgCount {
		total += c
	}
	expected := state.expectedMsgCount
	round := state.round

	if total == expected {
		data := state.data
		state.data = nil
		state.flushed = true
		s.mu.Unlock()

		slog.Info("Counts matched, broadcasting Confirm",
			"cid", clientID, "round", round, "total", total)
		s.sendConfirm(clientID)
		if err := s.flushClient(clientID, data); err != nil {
			slog.Error("While flushing client", "err", err, "cid", clientID)
		}

		s.mu.Lock()
		delete(s.clientsState, clientID)
		s.mu.Unlock()
		return
	}

	state.round++
	state.sumsMsgCount = map[int]uint64{}
	nextRound := state.round
	s.mu.Unlock()

	slog.Info("Counts did not match, sending next CountQuery",
		"cid", clientID, "round", nextRound,
		"total", total, "expected", expected)
	s.sendCountQuery(clientID, nextRound)
}

func (s *Sum) handleConfirm(m inner.CoordMsg) {
	s.mu.Lock()
	state, ok := s.clientsState[m.ClientID]
	if !ok {
		s.mu.Unlock()
		return
	}
	if state.flushed {
		s.mu.Unlock()
		return
	}
	data := state.data
	state.data = nil
	state.flushed = true
	wasCoord := state.isCoord
	s.mu.Unlock()

	if wasCoord {
		return
	}

	slog.Info("Received Confirm, flushing partial", "cid", m.ClientID, "fruits", len(data))
	if err := s.flushClient(m.ClientID, data); err != nil {
		slog.Error("While flushing client", "err", err, "cid", m.ClientID)
	}

	s.mu.Lock()
	delete(s.clientsState, m.ClientID)
	s.mu.Unlock()
}

func (s *Sum) sendCountQuery(clientID, round int) {
	query, err := inner.SerializeCoord(inner.CoordMsg{
		ClientID: clientID,
		Kind:     inner.CountQuery,
		CoordID:  s.id,
		SenderID: s.id,
		Round:    round,
	})
	if err != nil {
		slog.Error("Serializing count query", "err", err, "cid", clientID, "round", round)
		return
	}
	for i, queue := range s.coordOutQueues {
		if i == s.id {
			continue
		}
		if err := queue.Send(*query); err != nil {
			slog.Error("Sending count query", "err", err, "cid", clientID, "round", round, "to", i)
		}
	}
}

func (s *Sum) sendCountReply(coordID, clientID, round int, count uint64) {
	reply, err := inner.SerializeCoord(inner.CoordMsg{
		ClientID: clientID,
		Kind:     inner.CountReply,
		CoordID:  coordID,
		SenderID: s.id,
		Round:    round,
		Count:    count,
	})
	if err != nil {
		slog.Error("Serializing count reply", "err", err, "cid", clientID)
		return
	}
	if err := s.coordOutQueues[coordID].Send(*reply); err != nil {
		slog.Error("Sending count reply", "err", err, "cid", clientID, "to", coordID)
	}
}

func (s *Sum) sendConfirm(clientID int) {
	confirm, err := inner.SerializeCoord(inner.CoordMsg{
		ClientID: clientID,
		Kind:     inner.Confirm,
		CoordID:  s.id,
	})
	if err != nil {
		slog.Error("Serializing confirm", "err", err, "cid", clientID)
		return
	}
	for i, queue := range s.coordOutQueues {
		if i == s.id {
			continue
		}
		if err := queue.Send(*confirm); err != nil {
			slog.Error("Sending confirm", "err", err, "cid", clientID, "to", i)
		}
	}
}

func (s *Sum) flushClient(clientID int, data map[string]fruititem.FruitItem) error {
	for fruit, item := range data {
		msg, err := inner.SerializePipeline(inner.PipelineMsg{
			ClientID: clientID,
			Records:  []fruititem.FruitItem{item},
		})
		if err != nil {
			return fmt.Errorf("serializing data for client %d fruit %q: %w", clientID, fruit, err)
		}
		idx := s.shardFor(clientID, fruit)
		if err := s.outputQueues[idx].Send(*msg); err != nil {
			return fmt.Errorf("publishing data for client %d fruit %q to aggregator %d: %w", clientID, fruit, idx, err)
		}
	}

	eofMsg, err := inner.SerializePipeline(inner.PipelineMsg{ClientID: clientID})
	if err != nil {
		return fmt.Errorf("serializing EOF for client %d: %w", clientID, err)
	}
	for i, queue := range s.outputQueues {
		if err := queue.Send(*eofMsg); err != nil {
			return fmt.Errorf("publishing EOF for client %d to aggregator %d: %w", clientID, i, err)
		}
	}

	slog.Info("Flushed client to aggregators", "cid", clientID, "fruits", len(data))
	return nil
}

func (s *Sum) shardFor(clientID int, fruit string) int {
	h := fnv.New32a()
	fmt.Fprintf(h, "%d\x00%s", clientID, fruit)
	return int(h.Sum32() % uint32(s.aggregationAmount))
}
