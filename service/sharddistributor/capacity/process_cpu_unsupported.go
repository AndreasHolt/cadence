//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd

package capacity

type unsupportedProcessCPUSampler struct{}

func newProcessCPUSampler() ProcessCPUSampler {
	return unsupportedProcessCPUSampler{}
}

func (unsupportedProcessCPUSampler) Sample() (float64, bool) {
	return 0, false
}
