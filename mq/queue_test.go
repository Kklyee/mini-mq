package mq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryQueueFIFO(t *testing.T) {
	q, err := NewMemoryQueue(3)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	ids := []string{"M1", "M2", "M3"}

	for _, id := range ids {
		err := q.Publish(ctx, Message{
			ID:      id,
			Payload: []byte(id),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, want := range ids {
		msg, err := q.Consume(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if msg.ID != want {
			t.Fatalf("got %s, want %s", msg.ID, want)
		}
	}
}

func TestMemoryQueueFullBlocksProducer(t *testing.T) {
	q, err := NewMemoryQueue(1)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	// 先填满队列
	err = q.Publish(ctx, Message{
		ID:      "M1",
		Payload: []byte("first"),
	})
	if err != nil {
		t.Fatal(err)
	}

	finished := make(chan error, 1)

	go func() {
		finished <- q.Publish(ctx, Message{
			ID:      "M2",
			Payload: []byte("second"),
		})
	}()

	// 队列仍然是满的，第二次 Publish 不应完成
	select {
	case err := <-finished:
		t.Fatalf("Publish returned before space was available: %v", err)
	case <-time.After(20 * time.Millisecond):
		// 预期：仍在等待
	}

	// 消费 M1，腾出空位
	msg, err := q.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if msg.ID != "M1" {
		t.Fatalf("got %s, want M1", msg.ID)
	}

	// Producer 应该能够完成发送
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Producer did not resume after space became available")
	}

	msg, err = q.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if msg.ID != "M2" {
		t.Fatalf("got %s, want M2", msg.ID)
	}
}

func TestMemoryQueueEmptyBlockConsumer(t *testing.T) {
	q, err := NewMemoryQueue(1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	type consumeResult struct {
		msg Message
		err error
	}

	result := make(chan consumeResult, 1)

	go func() {
		msg, err := q.Consume(ctx)
		result <- consumeResult{msg: msg, err: err}
	}()

	select {
	case res := <-result:
		t.Fatalf("Consume should be blocked,got %+v", res)
	case <-time.After(time.Millisecond * 20):
	}

	err = q.Publish(ctx, Message{ID: "M1", Payload: []byte("Message 1")})

	if err != nil {
		t.Fatal(err)
	}

	select {
	case res := <-result:
		if res.err != nil {
			t.Fatal(res.err)
		}
		if res.msg.ID != "M1" {
			t.Fatalf("Expected Message 1,got %s", res.msg.ID)
		}
	case <-ctx.Done():
		t.Fatal("Consumer did not resume")

	}
}

func TestMemoryQueueAck(t *testing.T) {
	q, err := NewMemoryQueue(2)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	msg := Message{ID: "M1", Payload: []byte("hello")}

	if err := q.Publish(ctx, msg); err != nil {
		t.Fatal(err)
	}

	got, err := q.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "M1" {
		t.Fatalf("got %s, want M1", got.ID)
	}

	if _, ok := q.inflight["M1"]; !ok {
		t.Fatal("M1 should be in inflight")
	}

	if err := q.Ack("M1"); err != nil {
		t.Fatal(err)
	}

	if _, ok := q.inflight["M1"]; ok {
		t.Fatal("M1 should be removed from inflight")
	}

	if _, ok := q.ids["M1"]; ok {
		t.Fatal("M1 should be removed from ids")
	}

	if err := q.Ack("M1"); err == nil {
		t.Fatal("duplicate ACK should return error")
	}
}

// 2. 重复消息 ID 必须被拒绝
func TestMemoryQueueDuplicateID(t *testing.T) {
	q, err := NewMemoryQueue(2)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if err := q.Publish(ctx, Message{
		ID: "M1", Payload: []byte("first"),
	}); err != nil {
		t.Fatal(err)
	}

	if err := q.Publish(ctx, Message{
		ID: "M1", Payload: []byte("second"),
	}); err == nil {
		t.Fatal("duplicate message ID should be rejected")
	}

	msg, err := q.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Payload) != "first" {
		t.Fatalf("unexpected payload: %s", msg.Payload)
	}

	// 未 ACK 时仍不能重复发布
	if err := q.Publish(ctx, Message{
		ID: "M1", Payload: []byte("third"),
	}); err == nil {
		t.Fatal("inflight message ID should remain reserved")
	}
}

// 3. 发布取消后必须回滚预留 ID
func TestMemoryQueuePublishRollback(t *testing.T) {
	q, err := NewMemoryQueue(1)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// 队列容量为 1，先填满
	if err := q.Publish(ctx, Message{
		ID: "M1", Payload: []byte("first"),
	}); err != nil {
		t.Fatal(err)
	}

	publishCtx, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)

	go func() {
		result <- q.Publish(publishCtx, Message{
			ID: "M2", Payload: []byte("second"),
		})
	}()

	// 等待 M2 的 ID 被预留
	deadline := time.After(time.Second)
	for {
		q.mu.Lock()
		_, reserved := q.ids["M2"]
		q.mu.Unlock()

		if reserved {
			break
		}

		select {
		case <-deadline:
			cancel()
			t.Fatal("M2 was not reserved")
		case <-time.After(time.Millisecond):
		}
	}

	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Publish did not exit after cancellation")
	}

	// 取消后 ID 应被释放
	q.mu.Lock()
	_, exists := q.ids["M2"]
	q.mu.Unlock()

	if exists {
		t.Fatal("M2 ID should be released after rollback")
	}

	// 消费 M1，释放容量
	if _, err := q.Consume(ctx); err != nil {
		t.Fatal(err)
	}

	// M2 应该能够重新发布
	if err := q.Publish(ctx, Message{
		ID: "M2", Payload: []byte("retry"),
	}); err != nil {
		t.Fatalf("M2 should be publishable after rollback: %v", err)
	}
}

// 4. 并发发布同一个 ID，只允许一个成功
func TestMemoryQueueConcurrentDuplicateID(t *testing.T) {
	const producers = 10

	q, err := NewMemoryQueue(producers)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make(chan error, producers)

	for i := 0; i < producers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			results <- q.Publish(
				context.Background(),
				Message{ID: "M1", Payload: []byte("hello")},
			)
		}()
	}

	wg.Wait()
	close(results)

	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}

	if successes != 1 {
		t.Fatalf("successful publishes = %d, want 1", successes)
	}

	if got := len(q.messages); got != 1 {
		t.Fatalf("queued messages = %d, want 1", got)
	}
}
