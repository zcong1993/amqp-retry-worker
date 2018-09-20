package worker

import "fmt"

// DefaultBackoff always return base
type DefaultBackoff struct {
	base int
}

// NewDefaultBackoff return a new DefaultBackoff instance
func NewDefaultBackoff(base int) *DefaultBackoff {
	return &DefaultBackoff{
		base: base,
	}
}

// GetDelay impl Backoff GetDelay
func (db *DefaultBackoff) GetDelay(payload []byte, routerKey string) string {
	return int2string(db.base)
}

// Reset impl Backoff Reset
func (db *DefaultBackoff) Reset(payload []byte, routerKey string) {

}

func int2string(i int) string {
	return fmt.Sprintf("%d", i)
}
