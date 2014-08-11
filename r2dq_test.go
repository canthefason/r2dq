package r2dq

import (
	"log"
	"os"
	"testing"
)

func tearUp() *Queue {
	redisAddr := "localhost:6379"
	env := os.Getenv("REDIS_ADDR")
	if env != "" {
		redisAddr = env
	}

	return NewQueue(redisAddr, 0, "test")
}

func tearDown(q *Queue) {
	res := q.redisConn.Del(q.waitingQueueKey())

	if res.Err() != nil {
		log.Printf("An error occurred in tearDown: %s", res.Err())
	}

	res = q.redisConn.Del(q.procQueueKey())
	if res.Err() != nil {
		log.Printf("An error occurred in tearDown: %s", res.Err())
	}

	q.Close()
}

func TestQueue(t *testing.T) {
	q := tearUp()
	defer tearDown(q)

	err := q.Queue("drteeth")
	if err != nil {
		t.Errorf("Expected nil but got %s", err)
	}

	res := q.redisConn.LLen(q.waitingQueueKey())
	if res.Val() != 1 {
		t.Errorf("Expected %d but got %d", 1, res.Val())
	}

	q.Queue("floyd")
	res = q.redisConn.LLen(q.waitingQueueKey())
	if res.Val() != 2 {
		t.Errorf("Expected %d but got %d", 2, res.Val())
	}
}

func TestDequeue(t *testing.T) {
	q := tearUp()
	defer tearDown(q)

	q.Queue("drteeth")
	length := q.redisConn.LLen(q.waitingQueueKey())
	if length.Val() != 1 {
		t.Errorf("Expected %d but got %d", 1, length.Val())
	}

	q.Queue("floyd")
	length = q.redisConn.LLen(q.waitingQueueKey())
	if length.Val() != 2 {
		t.Errorf("Expected %d but got %d", 2, length.Val())
	}

	res, err := q.Dequeue()
	if err != nil {
		t.Errorf("Expected nil but got %s", err)
		t.FailNow()
	}

	if res != "drteeth" {
		t.Errorf("Expected %s but got %s", "drteeth", res)
	}

	length = q.redisConn.LLen(q.procQueueKey())
	if length.Val() != 1 {
		t.Errorf("Expected %d but got %d", 1, length.Val())
	}

	res, err = q.Dequeue()
	if err != nil {
		t.Errorf("Expected nil but got %s", err)
	}

	if res != "floyd" {
		t.Errorf("Expected %s but got %s", "floyd", res)
	}

	length = q.redisConn.LLen(q.procQueueKey())
	if length.Val() != 2 {
		t.Errorf("Expected %d but got %d", 2, length.Val())
	}

	res, err = q.Dequeue()
	if err != nil {
		t.Errorf("Expected nil but got %s", err)
	}

	if res != "" {
		t.Errorf("Expected empty queue, but got %s", res)
	}

}

func TestAck(t *testing.T) {
	q := tearUp()
	defer tearDown(q)

	q.Queue("drteeth")
	q.Queue("floyd")

	q.Dequeue()
	q.Dequeue()

	err := q.Ack("floyd")
	if err != nil {
		t.Errorf("Expected nil but got %s", err)
	}

	length := q.redisConn.LLen(q.procQueueKey())
	if length.Val() != 1 {
		t.Errorf("Expected %d but got %d", 1, length.Val())
	}

	err = q.Ack("animal")
	if err != nil {
		t.Errorf("Expected %s but got %s", ErrNotFound, err)
	}

	err = q.Ack("drteeth")
	if err != nil {
		t.Errorf("Expected %s but got %s", ErrNotFound, err)
	}

	length = q.redisConn.LLen(q.procQueueKey())
	if length.Val() != 0 {
		t.Errorf("Expected %d but got %d", 0, length.Val())
	}
}

func TestNAck(t *testing.T) {
	q := tearUp()
	defer tearDown(q)

	q.Queue("drteeth")
	q.Queue("floyd")

	q.Dequeue()

	err := q.NAck("drteeth")
	if err != nil {
		t.Errorf("Expected nil but got %s", err)
	}

	length := q.redisConn.LLen(q.procQueueKey())
	if length.Val() != 0 {
		t.Errorf("Expected %d but got %d", 0, length.Val())
	}

	length = q.redisConn.LLen(q.waitingQueueKey())
	if length.Val() != 2 {
		t.Errorf("Expected %d but got %d", 2, length.Val())
	}
}

func TestGracefulShutdown(t *testing.T) {
	q := tearUp()
	defer tearDown(q)

	q.Queue("drteeth")
	q.Queue("floyd")

	q.Dequeue()
	q.Dequeue()

	q.gracefulShutdown()

	length := q.redisConn.LLen(q.procQueueKey())
	if length.Val() != 0 {
		t.Errorf("Expected %d but got %d", 0, length.Val())
	}

	length = q.redisConn.LLen(q.waitingQueueKey())
	if length.Val() != 2 {
		t.Errorf("Expected %d but got %d", 2, length.Val())
	}

}
