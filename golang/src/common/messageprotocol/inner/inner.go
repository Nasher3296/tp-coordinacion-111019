package inner

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type PipelineMsg struct {
	ClientID      int
	Records       []fruititem.FruitItem
	ExpectedCount uint64
}

func (m PipelineMsg) IsEOF() bool { return len(m.Records) == 0 }

type pipelineWire struct {
	ClientID      int     `json:"cid"`
	Records       [][]any `json:"records"`
	ExpectedCount uint64  `json:"expected,omitempty"`
}

func SerializePipeline(m PipelineMsg) (*middleware.Message, error) {
	records := make([][]any, len(m.Records))
	for i, r := range m.Records {
		records[i] = []any{r.Fruit, r.Amount}
	}
	body, err := json.Marshal(pipelineWire{
		ClientID:      m.ClientID,
		Records:       records,
		ExpectedCount: m.ExpectedCount,
	})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializePipeline(msg *middleware.Message) (PipelineMsg, error) {
	var w pipelineWire
	if err := json.Unmarshal([]byte(msg.Body), &w); err != nil {
		return PipelineMsg{}, err
	}
	records := make([]fruititem.FruitItem, len(w.Records))
	for i, r := range w.Records {
		if len(r) != 2 {
			return PipelineMsg{}, fmt.Errorf("record %d: expected [fruit, amount], got %d elements", i, len(r))
		}
		fruit, ok := r[0].(string)
		if !ok {
			return PipelineMsg{}, errors.New("record fruit is not a string")
		}
		amount, ok := r[1].(float64)
		if !ok {
			return PipelineMsg{}, errors.New("record amount is not a number")
		}
		records[i] = fruititem.FruitItem{Fruit: fruit, Amount: uint32(amount)}
	}
	return PipelineMsg{
		ClientID:      w.ClientID,
		Records:       records,
		ExpectedCount: w.ExpectedCount,
	}, nil
}

type CoordKind string

const (
	CountQuery CoordKind = "count_query"
	CountReply CoordKind = "count_reply"
	Confirm    CoordKind = "confirm"
)

type CoordMsg struct {
	ClientID int       `json:"cid"`
	Kind     CoordKind `json:"kind"`
	CoordID  int       `json:"coord,omitempty"`
	SenderID int       `json:"sender,omitempty"`
	Round    int       `json:"round,omitempty"`
	Count    uint64    `json:"count,omitempty"`
}

func SerializeCoord(m CoordMsg) (*middleware.Message, error) {
	body, err := json.Marshal(CoordMsg{
		ClientID: m.ClientID,
		Kind:     m.Kind,
		CoordID:  m.CoordID,
		SenderID: m.SenderID,
		Round:    m.Round,
		Count:    m.Count,
	})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeCoord(msg *middleware.Message) (CoordMsg, error) {
	var w CoordMsg
	if err := json.Unmarshal([]byte(msg.Body), &w); err != nil {
		return CoordMsg{}, err
	}
	return w, nil
}
