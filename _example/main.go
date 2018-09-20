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

func random() int {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	return r1.Intn(10)
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
	rw := worker.NewRetryWorker("amqp://guest:guest@localhost:5672/", "test_retry_worker", []string{"test"}, &TestWorker{}, bf, 2)
	rw.Run()
}
