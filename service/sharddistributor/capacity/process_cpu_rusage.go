//go:build linux || darwin || freebsd || netbsd || openbsd

package capacity

import "golang.org/x/sys/unix"

type rusageProcessCPUSampler struct{}

func newProcessCPUSampler() ProcessCPUSampler {
	return rusageProcessCPUSampler{}
}

func (rusageProcessCPUSampler) Sample() (float64, bool) {
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}

	userSeconds := float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6
	systemSeconds := float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6
	return userSeconds + systemSeconds, true
}
