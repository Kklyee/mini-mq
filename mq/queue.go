package mq

import (
	"context"
	"errors"
	"sync"
)

type Queue interface {
	Publish(ctx context.Context, msg Message) error
	Consume(ctx context.Context) (Message, error)
	Ack(msgID string) error
	Close() error
}

type MemoryQueue struct {
	messages chan Message
	inflight map[string]Message
	ids      map[string]struct{}
	retrying map[string]struct{}
	mu       sync.Mutex
}

var (
	ErrQueueClosed = errors.New("queue closed")
)

func NewMemoryQueue(capacity int) (*MemoryQueue, error) {
	if capacity <= 0 {
		return nil, errors.New("capacity must be positive")
	}
	return &MemoryQueue{
		messages: make(chan Message, capacity),
		inflight: make(map[string]Message),
		retrying: make(map[string]struct{}),
		ids:      make(map[string]struct{}),
	}, nil
}

func (q *MemoryQueue) Publish(
	ctx context.Context,
	msg Message,
) error {
	if len(msg.Payload) == 0 {
		return errors.New("payload is empty")
	}
	if msg.ID == "" {
		return errors.New("message id is empty")
	}
	q.mu.Lock()
	if _, ok := q.ids[msg.ID]; ok {
		q.mu.Unlock()
		return errors.New("duplicated message")
	}

	q.ids[msg.ID] = struct{}{}
	q.mu.Unlock()

	select {
	case q.messages <- msg:
		return nil
	case <-ctx.Done():
		q.mu.Lock()
		delete(q.ids, msg.ID)
		q.mu.Unlock()
		return ctx.Err()
	}
}

func (q *MemoryQueue) Consume(
	ctx context.Context,
) (Message, error) {

	select {
	case msg, ok := <-q.messages:
		if !ok {
			return Message{}, ErrQueueClosed
		}
		q.mu.Lock()
		q.inflight[msg.ID] = msg
		q.mu.Unlock()
		return msg, nil
	case <-ctx.Done():
		return Message{}, ctx.Err()
	}
}

func (q *MemoryQueue) Ack(
	msgID string,
) error {
	if msgID == "" {
		return errors.New("message id is empty")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.inflight[msgID]; !ok {
		return errors.New("message does not exist")
	}

	delete(q.ids, msgID)
	delete(q.inflight, msgID)
	return nil
}

func (q *MemoryQueue) Nack(
	ctx context.Context,
	msgID string,
) error {
	if msgID == "" {
		return errors.New("message id is empty")
	}

	q.mu.Lock()

	msg, ok := q.inflight[msgID]
	if !ok {
		q.mu.Unlock()
		return errors.New("message not found in inflight")
	}

	if _, retrying := q.retrying[msgID]; retrying {
		q.mu.Unlock()
		return errors.New("message is already retrying")
	}

	q.retrying[msgID] = struct{}{}
	q.mu.Unlock()

	select {
	case q.messages <- msg:
	case <-ctx.Done():
		q.mu.Lock()
		delete(q.retrying, msgID)
		q.mu.Unlock()
		return ctx.Err()
	}

	q.mu.Lock()
	delete(q.inflight, msgID)
	delete(q.retrying, msgID)
	q.mu.Unlock()

	return nil
}
