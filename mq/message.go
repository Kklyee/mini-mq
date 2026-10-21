package mq

import "time"

type Message struct {
	ID        string
	Payload   []byte
	CreatedAt time.Time
}
