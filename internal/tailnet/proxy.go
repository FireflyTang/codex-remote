package tailnet

import (
	"net/url"
	"sync"

	"tailscale.com/net/tshttpproxy"
)

var (
	directProxyPolicyOnce sync.Once
	directProxyPolicyErr  error
)

// configureDirectProxyPolicy keeps the embedded Tailscale node off process
// HTTP proxies without changing the environment inherited by other children.
func configureDirectProxyPolicy() error {
	directProxyPolicyOnce.Do(func() {
		directProxyPolicyErr = tshttpproxy.SetProxyFunc(func(*url.URL) (*url.URL, error) {
			return nil, nil
		})
	})
	return directProxyPolicyErr
}
