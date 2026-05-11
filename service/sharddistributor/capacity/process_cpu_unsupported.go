//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd

package capacity

type noopProcessCPUSampler struct{}

func newProcessCPUSampler() ProcessCPUSampler {
	return noopProcessCPUSampler{}
}

func (noopProcessCPUSampler) Sample() (float64, bool) {
	return 0, false
}
