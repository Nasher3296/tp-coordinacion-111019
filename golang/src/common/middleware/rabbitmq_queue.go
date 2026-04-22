package middleware

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
	stop      chan any
	returns   chan amqp.Return
}

func NewQueueMiddleware(queueName string, cs ConnSettings) (Middleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", cs.Hostname, cs.Port))
	if err != nil {
		return nil, ErrMessageMiddlewareMessage
	}
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		return nil, ErrMessageMiddlewareMessage
	}
	defer func() {
		if err != nil {
			ch.Close()
		}
	}()

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, ErrMessageMiddlewareMessage
	}

	returns := ch.NotifyReturn(make(chan amqp.Return))

	qm := &queueMiddleware{
		conn:      conn,
		channel:   ch,
		queueName: queueName,
		stop:      make(chan any),
		returns:   returns,
	}
	go qm.listenReturns()

	return qm, nil
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) (err error) {
	msgs, err := q.channel.Consume(
		q.queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for {
		select {
		case <-q.stop:
			return nil
		case msg, ok := <-msgs:
			if !ok {
				if q.conn.IsClosed() {
					return ErrMessageMiddlewareDisconnected
				}
				return nil
			}
			callbackFunc(Message{Body: string(msg.Body)}, func() { msg.Ack(false) }, func() { msg.Nack(false, true) })
		}
	}
}

func (q *queueMiddleware) StopConsuming() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrMessageMiddlewareClose
		}
	}()
	close(q.stop)
	return nil
}

func (q *queueMiddleware) Send(msg Message) error {
	err := q.channel.Publish(
		"",
		q.queueName,
		true,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		if q.conn.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (q *queueMiddleware) listenReturns() {
	for ret := range q.returns {
		log.Printf("rabbit: message returned queue=%s routing_key=%q reason=%q", q.queueName, ret.RoutingKey, ret.ReplyText)
	}
}

func (q *queueMiddleware) Close() error {
	errChan := q.channel.Close()
	errConn := q.conn.Close()
	if errChan != nil || errConn != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}
