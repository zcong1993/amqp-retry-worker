package worker

import "fmt"

// DefaultBackoff always return base
type DefaultBackoff struct {
	base float64
}

// NewDefaultBackoff return a new DefaultBackoff instance
func NewDefaultBackoff(base float64) *DefaultBackoff {
	return &DefaultBackoff{
		base: base,
	}
}

// GetDelay impl Backoff GetDelay
func (db *DefaultBackoff) GetDelay(payload []byte, routerKey string) string {
	return float2string(db.base)
}

// Reset impl Backoff Reset
func (db *DefaultBackoff) Reset(payload []byte, routerKey string) {

}

func float2string(f float64) string {
	return fmt.Sprintf("%f", f)
}
