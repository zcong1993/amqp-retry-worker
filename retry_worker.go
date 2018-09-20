package worker

import (
	"github.com/streadway/amqp"
	"github.com/zcong1993/amqp-helpers"
	"github.com/zcong1993/debugo"
)

var debug = debugo.NewDebug("retry-worker")

// RetryWorker is our retry worker struct
type RetryWorker struct {
	url               string
	ExchangeName      string
	retryExchangeName string
	routerKeys        []string
	worker            Worker
	backoff           Backoff
	maxRetry          int
}

// NewRetryWorker return a new RetryWorker instance
func NewRetryWorker(url string, exchangeName string, routerKeys []string, worker Worker, backoff Backoff, maxRetry int) *RetryWorker {
	rw := &RetryWorker{
		ExchangeName:      exchangeName,
		worker:            worker,
		backoff:           backoff,
		maxRetry:          maxRetry,
		retryExchangeName: exchangeName + "_retry",
		routerKeys:        routerKeys,
		url:               url,
	}

	return rw
}

// Run start worker
func (rw *RetryWorker) Run() {
	conn := helpers.MustDeclareConn(rw.url)
	exCh := helpers.MustDeclareExchange(conn, rw.ExchangeName, nil)
	retryCh := helpers.MustDeclareExchange(conn, rw.retryExchangeName, nil)

	defer conn.Close()
	defer exCh.Close()
	defer retryCh.Close()

	helpers.MustBindQueue(retryCh, rw.retryExchangeName, rw.routerKeys, amqp.Table{"x-dead-letter-exchange": rw.ExchangeName})

	_, msgs := helpers.MustDeclareConsumer(exCh, rw.ExchangeName, rw.routerKeys)

	for msg := range msgs {
		err, retry := rw.worker.Do(msg.Body, msg.RoutingKey)

		// success work
		if err == nil {
			debug.Debugf("success %+v", string(msg.Body))
			msg.Ack(false)
			rw.backoff.Reset(msg.Body, msg.RoutingKey)
			continue
		}

		// failed work

		// not need retry
		if !retry {
			debug.Debugf("no need retry %+v", string(msg.Body))
			msg.Nack(false, false)
			rw.backoff.Reset(msg.Body, msg.RoutingKey)
			continue
		}

		// handle retry
		p := helpers.CopyMsgToPublishing(msg)
		h := helpers.ParseDeathHeader(p.Headers)
		timeout := rw.backoff.GetDelay(p.Body, msg.RoutingKey)
		p.Expiration = timeout

		if h != nil {
			// check if hit max retry, reject
			c := h.Count + 1
			if c > rw.maxRetry {
				debug.Debugf("hit max retry, now %d max %d, give up", c, rw.maxRetry)
				msg.Nack(false, false)
				rw.backoff.Reset(p.Body, msg.RoutingKey)
				continue
			}
			debug.Debugf("retry %d with timeout %s %+v", c, timeout, string(msg.Body))
		} else {
			debug.Debugf("retry %d with timeout %s %+v", 1, timeout, string(msg.Body))
		}

		// handle retry
		retryCh.Publish(rw.retryExchangeName, msg.RoutingKey, false, false, *p)
		msg.Ack(false)
	}
}
