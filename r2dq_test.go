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
	res := q.redisConnIn.Del(q.waitingQueueKey())

	if res.Err() != nil {
		log.Printf("An error occurred in tearDown: %s", res.Err())
	}

	res = q.redisConnIn.Del(q.procQueueKey())
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

	res := q.redisConnIn.LLen(q.waitingQueueKey())
	if res.Val() != 1 {
		t.Errorf("Expected %d but got %d", 1, res.Val())
	}

	q.Queue("floyd")
	res = q.redisConnIn.LLen(q.waitingQueueKey())
	if res.Val() != 2 {
		t.Errorf("Expected %d but got %d", 2, res.Val())
	}
}

func TestDequeue(t *testing.T) {
	q := tearUp()
	defer tearDown(q)

	q.Queue("drteeth")
	length := q.redisConnIn.LLen(q.waitingQueueKey())
	if length.Val() != 1 {
		t.Errorf("Expected %d but got %d", 1, length.Val())
	}

	q.Queue("floyd")
	length = q.redisConnIn.LLen(q.waitingQueueKey())
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

	length = q.redisConnIn.LLen(q.procQueueKey())
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

	length = q.redisConnIn.LLen(q.procQueueKey())
	if length.Val() != 2 {
		t.Errorf("Expected %d but got %d", 2, length.Val())
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

	length := q.redisConnIn.LLen(q.procQueueKey())
	if length.Val() != 1 {
		t.Errorf("Expected %d but got %d", 1, length.Val())
	}

	err = q.Ack("animal")
	if err != nil && err != ErrNotFound {
		t.Errorf("Expected %s but got %s", ErrNotFound, err)
	}

	err = q.Ack("drteeth")
	if err != nil {
		t.Errorf("Expected nil but got %s", err)
	}

	length = q.redisConnIn.LLen(q.procQueueKey())
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

	length := q.redisConnIn.LLen(q.procQueueKey())
	if length.Val() != 0 {
		t.Errorf("Expected %d but got %d", 0, length.Val())
	}

	length = q.redisConnIn.LLen(q.waitingQueueKey())
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

	length := q.redisConnIn.LLen(q.procQueueKey())
	if length.Val() != 0 {
		t.Errorf("Expected %d but got %d", 0, length.Val())
	}

	length = q.redisConnIn.LLen(q.waitingQueueKey())
	if length.Val() != 2 {
		t.Errorf("Expected %d but got %d", 2, length.Val())
	}

}

func TestPurge(t *testing.T) {
	q := tearUp()
	defer tearDown(q)
	q.Queue("gonzo")
	q.Queue("fuzzy")
	q.Dequeue()

	q.Purge()
	res := q.redisConnIn.LLen(q.procQueueKey())
	if res.Err() != nil {
		t.Errorf("Expected nil but got %s", res.Err())
		t.FailNow()
	}
	if res.Val() != 0 {
		t.Errorf("Expected %d but got %d", 0, res.Val())
	}

	res = q.redisConnIn.LLen(q.waitingQueueKey())
	if res.Err() != nil {
		t.Errorf("Expected nil but got %s", res.Err())
		t.FailNow()
	}
	if res.Val() != 0 {
		t.Errorf("Expected %d but got %d", 0, res.Val())
	}
}

func TestLength(t *testing.T) {
	q := tearUp()
	defer tearDown(q)
	q.Queue("gonzo")
	length, err := q.Len()
	if err != nil {
		t.Errorf("Expected nil but got %s", err)
	}

	if length != 1 {
		t.Errorf("Expected %d but got %d", 1, length)
	}
}

func TestRepetitiveDequeue(t *testing.T) {
	q := tearUp()
	defer tearDown(q)
	first := "gonzo"
	second := "bigbird"
	count := 0

	go func() {
		for {
			res, err := q.Dequeue()
			if err == ErrConnClosed {
				return
			}

			if err != nil {
				t.Errorf("Expected nil but got %s", err)
				t.FailNow()
			}
			if res != first && res != second {
				t.Errorf("Expected %s or %s but got %s", first, second, res)
				t.FailNow()
			}
			count++

			if count > 2 {
				t.Errorf("Expected only 2 dequeue operations but got %d", count)
			}
		}

	}()

	q.Queue(first)
	q.Queue(second)
	q.StopDequeue()
	q.gracefulShutdown()
}
