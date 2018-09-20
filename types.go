package worker

// Worker is worker interface
type Worker interface {
	// Do do the worker logic, return two args, first is err, second is if should retry
	// if err is nil, means work success, retry is optional
	// when err is not nil, if retry will copy msg to delay queue, if not, will handle msg with nack and not requeue
	Do(payload []byte, routerKey string) (err error, retry bool)
}

// Backoff control the work retry delay time
type Backoff interface {
	// GetDelay get retry delay timeout(ms) by payload
	// get identity from msg payload yourself
	GetDelay(payload []byte, routerKey string) string
	// Reset reset retry delay timeout, be called when work retry success or hit max retry
	// can do some gc here
	Reset(payload []byte, routerKey string)
}
