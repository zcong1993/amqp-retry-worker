package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zcong1993/amqp-retry-worker"
	"github.com/zcong1993/amqp-retry-worker/_example/backoff"
	"github.com/zcong1993/amqp-retry-worker/_example/common"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func random() int {
	return rand.Intn(10)
}

type TestWorker struct {

}

func (tw *TestWorker) Do(payload []byte, routerKey string)(error, bool) {
	var msg common.Msg
	err := json.Unmarshal(payload, &msg)
	if err != nil {
		return err, false
	}

	if random() < 2 {
		fmt.Printf("id: %s, msg: %s - %s\n", msg.ID, msg.Message, "success! ")
		return nil, false
	}

	fmt.Printf("id: %s, msg: %s - %s\n", msg.ID, msg.Message, "failed! ")
	return errors.New("failed, retry. "), true
}

func main() {
	bf := backoff.NewBackoff(3000.0, 3.0)
	rw := worker.NewRetryWorker("amqp://guest:guest@localhost:5672/", "test_retry_worker", "retry_queue", []string{"test"}, &TestWorker{}, bf, 2, true)
	rw.Run()
}
