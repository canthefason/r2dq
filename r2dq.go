package r2dq

import (
	"errors"
	"fmt"

	"gopkg.in/redis.v2"
)

const (
	WAITING_QUEUE    = "waitingQueue"
	PROCESSING_QUEUE = "processingQueue"
)

var (
	ErrNotFound = errors.New("not found")
)

type Queue struct {
	prefix    string
	redisConn *redis.Client
}

func NewQueue(addr string, db int, prefix string) *Queue {
	q := new(Queue)
	q.prefix = prefix
	q.redisConn = redis.NewTCPClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       int64(db),
	})

	return q
}

func (q *Queue) Queue(value string) error {
	res := q.redisConn.LPush(q.waitingQueueKey(), value)

	return res.Err()
}

func (q *Queue) Dequeue() (string, error) {
	res := q.redisConn.BRPopLPush(q.waitingQueueKey(), q.procQueueKey(), 0)

	if res.Err() != nil && res.Err() != redis.Nil {
		return "", res.Err()
	}

	return res.Val(), nil
}

func (q *Queue) Ack(val string) error {
	res := q.redisConn.LRem(q.procQueueKey(), 1, val)

	if res.Err() != nil {
		if res.Err() == redis.Nil {
			return ErrNotFound
		}
		return res.Err()
	}

	return nil
}

func (q *Queue) Close() {
	q.gracefulShutdown()
	q.redisConn.Close()
}

func (q *Queue) gracefulShutdown() {
	res := q.redisConn.RPopLPush(q.procQueueKey(), q.waitingQueueKey())
	for res.Val() != "" {
		res = q.redisConn.RPopLPush(q.procQueueKey(), q.waitingQueueKey())
	}

	if res.Err() != redis.Nil {
		panic(res.Err())
	}
}

func (q *Queue) waitingQueueKey() string {
	return q.keyWithPrefix(WAITING_QUEUE)
}

func (q *Queue) procQueueKey() string {
	return q.keyWithPrefix(PROCESSING_QUEUE)
}

func (q *Queue) keyWithPrefix(queue string) string {
	return fmt.Sprintf("%s:%s", q.prefix, queue)
}
