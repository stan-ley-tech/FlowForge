package engine

import (
	"math"
	"math/rand"
	"time"

	"github.com/stan-ley-tech/flowforge/engine/internal/model"
)

// nextBackoff computes the delay before retry attempt n+1, where attempt
// is the attempt number that just failed (1-indexed). It grows
// exponentially from BackoffBaseMs by BackoffMultiplier, caps at
// BackoffMaxMs, then applies +/- JitterFraction so many steps retrying
// at once don't all wake up in the same instant.
func nextBackoff(s model.Step) time.Duration {
	base := float64(s.BackoffBaseMs)
	if base <= 0 {
		base = float64(model.DefaultRetryPolicy().BackoffBaseMs)
	}
	mult := s.BackoffMultiplier
	if mult <= 0 {
		mult = model.DefaultRetryPolicy().BackoffMultiplier
	}
	maxMs := float64(s.BackoffMaxMs)
	if maxMs <= 0 {
		maxMs = float64(model.DefaultRetryPolicy().BackoffMaxMs)
	}

	delay := base * math.Pow(mult, float64(s.Attempt-1))
	if delay > maxMs {
		delay = maxMs
	}

	if s.JitterFraction > 0 {
		jitter := delay * s.JitterFraction
		delay += (rand.Float64()*2 - 1) * jitter
		if delay < 0 {
			delay = 0
		}
	}

	return time.Duration(delay) * time.Millisecond
}
