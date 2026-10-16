package actions

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
)

// errBlocked refuses a fetch of a private or internal address.
var errBlocked = errors.New("address not allowed")

// metadataV6 is the IPv6 cloud metadata address, not covered by the ranges
// below.
var metadataV6 = net.ParseIP("fd00:ec2::254")

// sharedSpace is 100.64.0.0/10 (RFC 6598), which IsPrivate leaves out but
// platform private networks use (Railway's internal addresses are in it).
var _, sharedSpace, _ = net.ParseCIDR("100.64.0.0/10")

// blocked reports whether ip is loopback, private or shared address space,
// link-local (including the 169.254.169.254 metadata address), unspecified or
// multicast: anything a fetch of outside content must never reach, such as
// the private network river-service runs on.
func blocked(ip net.IP) bool {
	return ip == nil || ip.IsLoopback() || ip.IsPrivate() || sharedSpace.Contains(ip) || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.Equal(metadataV6)
}

// dialGuard checks the address actually dialled, after DNS resolution, so a
// hostname that resolves to an internal address (or rebinds to one) is
// refused too. Redirects dial again and pass the same check.
func dialGuard(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: %s", errBlocked, address)
	}
	if blocked(net.ParseIP(host)) {
		return fmt.Errorf("%w: %s", errBlocked, host)
	}
	return nil
}

// guardedClient fetches outside content: no proxy, at most five redirects,
// and no connection to a blocked address.
func guardedClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, Control: dialGuard}
	return &http.Client{
		Timeout: timeout,
		Transport: untracedOutbound(&http.Transport{
			Proxy:               nil,
			DialContext:         dialer.DialContext,
			ForceAttemptHTTP2:   true,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     30 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		}),
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many redirects")
			}
			return nil
		},
	}
}

// untracedOutbound makes each request through rt a client span in the job's
// trace, without sending the trace context: fetches go to arbitrary outside
// hosts.
func untracedOutbound(rt http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(rt, otelhttp.WithPropagators(propagation.NewCompositeTextMapPropagator()))
}
