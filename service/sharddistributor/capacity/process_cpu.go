package capacity

// ProcessCPUSampler reads cumulative CPU seconds consumed by the current process.
type ProcessCPUSampler interface {
	Sample() (seconds float64, ok bool)
}

// NewProcessCPUSampler returns a platform-specific sampler for cumulative process CPU time.
func NewProcessCPUSampler() ProcessCPUSampler {
	return newProcessCPUSampler()
}
