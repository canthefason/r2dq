package r2dq

import (
	"errors"
	"fmt"
	"log"

	"gopkg.in/redis.v2"
)

const (
	WAITING_QUEUE    = "waitingQueue"
	PROCESSING_QUEUE = "processingQueue"
)

var (
	ErrNotFound   = errors.New("not found")
	ErrConnClosed = errors.New("connection closed")
)

type Queue struct {
	prefix       string
	redisConnIn  *redis.Client
	redisConnOut *redis.Client
	closeChan    chan bool
}

func NewQueue(addr string, db int, prefix string) *Queue {
	q := new(Queue)
	q.closeChan = make(chan bool, 1)
	q.prefix = prefix
	q.redisConnIn = redis.NewTCPClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       int64(db),
	})

	q.redisConnOut = redis.NewTCPClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       int64(db),
	})

	return q
}

func (q *Queue) Queue(value string) error {
	res := q.redisConnIn.LPush(q.waitingQueueKey(), value)

	return res.Err()
}

func (q *Queue) Dequeue() (string, error) {
	res := q.redisConnOut.BRPopLPush(q.waitingQueueKey(), q.procQueueKey(), 0)

	if res.Err() != nil && res.Err() != redis.Nil {
		select {
		case <-q.closeChan:
			return "", ErrConnClosed
		default:
			return "", res.Err()
		}
	}

	return res.Val(), nil
}

func (q *Queue) Ack(val string) error {
	return q.removeProcItem(val)
}

func (q *Queue) NAck(val string) error {
	if err := q.removeProcItem(val); err != nil {
		return err
	}

	err := q.Queue(val)
	if err != nil {
		log.Printf("An error occurred while sending NAck for %s: %s", val, err)
	}

	return err
}

func (q *Queue) removeProcItem(val string) error {
	res := q.redisConnIn.LRem(q.procQueueKey(), 1, val)
	if res.Err() != nil {
		return res.Err()
	}

	// not found
	if res.Val() == 0 {
		return ErrNotFound
	}

	return nil
}

func (q *Queue) Close() {
	// first stop dequeing
	q.StopDequeue()
	q.gracefulShutdown()
	q.redisConnIn.Close()
}

func (q *Queue) StopDequeue() {
	q.closeChan <- true
	q.redisConnOut.Close()
}

func (q *Queue) Purge() {
	res := q.redisConnIn.Del(q.procQueueKey())
	if res.Err() != nil {
		log.Panicf("Could not purge unacked message queue: %s", res.Err())
	}
	res = q.redisConnIn.Del(q.waitingQueueKey())
	if res.Err() != nil {
		log.Panicf("Could not purge message queue: %s", res.Err())
	}
}

func (q *Queue) Len() (int64, error) {
	res := q.redisConnIn.LLen(q.waitingQueueKey())

	return res.Val(), res.Err()
}
func (q *Queue) gracefulShutdown() {
	res := q.redisConnIn.RPopLPush(q.procQueueKey(), q.waitingQueueKey())
	for res.Val() != "" {
		res = q.redisConnIn.RPopLPush(q.procQueueKey(), q.waitingQueueKey())
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
