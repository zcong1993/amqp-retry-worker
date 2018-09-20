package backoff

import (
	"encoding/json"
	"fmt"
	"github.com/zcong1993/amqp-retry-worker/_example/common"
	"sync"
)

type Backoff struct {
	store map[string] float64
	base float64
	rate float64
	mu sync.Mutex
}

func NewBackoff(base, rate float64) *Backoff {
	return &Backoff{
		store: make(map[string] float64, 0),
		base:base,
		rate:rate,
	}
}

func (bf *Backoff) GetDelay(payload []byte, routerKey string) string {
	id, err := getID(payload, routerKey)
	if err != nil {
		return float2string(bf.base)
	}
	bf.mu.Lock()
	defer bf.mu.Unlock()
	base, ok := bf.store[id]
	if !ok {
		bf.store[id] = bf.base
		return float2string(bf.base)
	}

	r := base * bf.rate
	bf.store[id] = r

	return float2string(r)
}

// Reset impl Backoff Reset
func (bf *Backoff) Reset(payload []byte, routerKey string) {
	id, err := getID(payload, routerKey)
	if err != nil {
		return
	}

	bf.mu.Lock()
	defer bf.mu.Unlock()

	delete(bf.store, id)
}

func float2string(f float64) string {
	return fmt.Sprintf("%d", int(f))
}

func getID(payload []byte, routerKey string) (string, error) {
	var msg common.Msg
	err := json.Unmarshal(payload, &msg)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s-%s", routerKey, msg.ID), nil
}
