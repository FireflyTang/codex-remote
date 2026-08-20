package runtime

import (
	"io"
	"net"
	"time"
)

func netDial(path string, timeout time.Duration) (io.Closer, error) {
	return net.DialTimeout("unix", path, timeout)
}
