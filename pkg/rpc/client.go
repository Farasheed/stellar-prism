// Package rpc defines the narrow view of Soroban RPC that prism depends on,
// and an adapter over the Stellar SDK client that implements it.
//
// The Client interface exists so the streaming state machine can be tested
// without a network. Everything prism does against RPC goes through these
// three methods, which keeps the fake in internal/fakerpc small enough to
// reason about.
package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// DefaultURL is the public testnet endpoint, so prism works with no
// configuration. Mainnet and local networks are selected with --rpc-url or
// SOROBAN_RPC_URL.
const DefaultURL = "https://soroban-testnet.stellar.org"

// URLEnvVar is the environment variable consulted when --rpc-url is not given.
const URLEnvVar = "SOROBAN_RPC_URL"

// Client is the subset of Soroban RPC that prism uses.
//
// It is deliberately tiny: four read methods, all taking a context. New
// stream sources should prefer composing these over widening the interface,
// because every method added here must also be faked in tests.
type Client interface {// GetEvents returns contract events matching the request. Either
// StartLedger or Pagination.Cursor must be set, never both — the RPC
// rejects requests carrying both.
	GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error)

	// GetEventsBatch returns contract events matching the request, combining
	// multiple requests into a single HTTP roundtrip. Each entry's StartLedger
	// or Pagination.Cursor must be mutually exclusive; at least one must be set.
	GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error)

	// GetLedgers returns ledger metadata starting at the requested point.
	GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error)

	// GetTransaction returns a single transaction by hash.
	GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error)

	// GetTransactions returns transactions matching the request.
	GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error)

	// GetLatestLedger reports the head of the chain, which is how a stream
	// with no explicit start ledger works out where "now" is.
	GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error)
}

// ResolveURL picks a single RPC endpoint from, in order of precedence: an
// explicit flag value, the SOROBAN_RPC_URL environment variable, then
// DefaultURL.
//
// It is kept for callers that only support one endpoint; ResolveURLs is the
// form used by the CLI, where a failover pool may be configured.
func ResolveURL(flagValue string) string {
	return ResolveURLs([]string{flagValue})[0]
}

// ResolveURLs picks the RPC endpoints from, in order of precedence: explicit
// flag values, the SOROBAN_RPC_URL environment variable, then DefaultURL.
//
// Multiple endpoints may be configured by repeating --rpc-url, or by
// separating them with commas in SOROBAN_RPC_URL. Blank entries are ignored.
func ResolveURLs(flagValues []string) []string {
	urls := cleanURLs(flagValues)
	if len(urls) > 0 {
		return urls
	}
	if env := strings.TrimSpace(os.Getenv(URLEnvVar)); env != "" {
		if urls := cleanURLs(strings.Split(env, ",")); len(urls) > 0 {
			return urls
		}
	}
	return []string{DefaultURL}
}

func cleanURLs(values []string) []string {
	urls := make([]string, 0, len(values))
	for _, value := range values {
		if url := strings.TrimSpace(value); url != "" {
			urls = append(urls, url)
		}
	}
	return urls
}

// resolveSRV resolves DNS SRV records for the given domain and returns
// the resolved URLs. This enables discovering RPC cluster nodes via
// SRV records, allowing automatic load-balancing across multiple endpoints.
func resolveSRV(domain string) ([]string, error) {
	// Look up SRV records: _rpc._tcp.domainname
	srvRecords, err := net.LookupSRV("", "_rpc._tcp", domain)
	if err != nil {
		return nil, fmt.Errorf("SRV lookup for %s: %w", domain, err)
	}

	urls := make([]string, 0, len(srvRecords))
	for _, srv := range srvRecords {
		// Construct URL from SRV record: scheme://host:port
		url := fmt.Sprintf("%s://%s:%d", "https", srv.Target, srv.Port)
		urls = append(urls, url)
	}
	return urls, nil
}

// SDKClient adapts the Stellar SDK's RPC client to the Client interface.
type SDKClient struct {
	inner *rpcclient.Client
}

// New returns a Client backed by the Stellar SDK, talking to url.
//
// The HTTP client carries a per-request timeout so a hung connection surfaces
// as a retryable error rather than stalling the stream indefinitely.
//
// proxyURL, when provided, is used to configure an HTTP proxy (HTTP, HTTPS,
// or SOCKS5) for the underlying requests. This enables routing RPC traffic
// through corporate proxies, firewall tunnels, or SOCKS gateways.
func New(url string, timeout time.Duration, opts ...NewClientOption) *SDKClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	options := applyNewClientOptions(opts...)
	return &SDKClient{
		inner: rpcclient.NewClient(url, &http.Client{
			Timeout:   timeout,
			Transport: options.transport,
		}),
	}
}

// GetEvents implements Client.
func (c *SDKClient) GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	resp, err := c.inner.GetEvents(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getEvents: %w", err)
	}
	return resp, nil
}

// GetEventsBatch implements Client.
func (c *SDKClient) GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	responses := make([]protocol.GetEventsResponse, 0, len(reqs))
	for _, req := range reqs {
		resp, err := c.inner.GetEvents(ctx, req)
		if err != nil {
			// Return what we have so far, marking this as a partial failure
			// by returning an empty response with the error info
			responses = append(responses, protocol.GetEventsResponse{
				Events:       nil,
				Error:        err.Error(),
			})
		} else {
			responses = append(responses, resp)
		}
	}
	return responses, nil
}

// GetLedgers implements Client.
func (c *SDKClient) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	resp, err := c.inner.GetLedgers(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getLedgers: %w", err)
	}
	return resp, nil
}

// GetLedgerEntries returns the requested ledger entries.
//
// It is deliberately not part of Client: the streaming core only needs the
// four methods above, and keeping the interface small avoids forcing every
// test double to implement a method it never uses. Callers that need it, such
// as contract ABI resolution, assert for LedgerEntriesClient instead.
func (c *SDKClient) GetLedgerEntries(ctx context.Context, req protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error) {
	resp, err := c.inner.GetLedgerEntries(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getLedgerEntries: %w", err)
	}
	return resp, nil
}

// LedgerEntriesClient is the optional capability used to resolve contract
// specs from on-chain WASM. SDKClient implements it.
type LedgerEntriesClient interface {
	GetLedgerEntries(ctx context.Context, req protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error)
}

// GetTransaction implements Client.
func (c *SDKClient) GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	resp, err := c.inner.GetTransaction(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getTransaction: %w", err)
	}
	return resp, nil
}

// GetTransactions implements Client.
func (c *SDKClient) GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	resp, err := c.inner.GetTransactions(ctx, req)
	if err != nil {
		return resp, fmt.Errorf("rpc getTransactions: %w", err)
	}
	return resp, nil
}

// GetLatestLedger implements Client.
func (c *SDKClient) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	resp, err := c.inner.GetLatestLedger(ctx)
	if err != nil {
		return resp, fmt.Errorf("rpc getLatestLedger: %w", err)
	}
	return resp, nil
}

// Close releases the underlying HTTP resources.
func (c *SDKClient) Close() error {
	if err := c.inner.Close(); err != nil {
		return fmt.Errorf("rpc close: %w", err)
	}
	return nil
}

// NewClientOption configures the Stellar SDK RPC client creation.
type NewClientOption struct {
	// ProxyURL, when set, configures an HTTP proxy (HTTP, HTTPS, or SOCKS5)
	// for underlying RPC requests. This enables routing traffic through
// corporate proxies, firewall tunnels, or SOCKS gateways.
	ProxyURL string
}

// newClientOptions collects options for SDK client creation.
type newClientOptions struct {
	transport http.RoundTripper
}

func applyNewClientOptions(opts ...NewClientOption) newClientOptions {
	optsDefault := newClientOptions{}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case *NewClientOption:
			if opt.ProxyURL != "" {
				transport, err := proxyURLToTransport(opt.ProxyURL)
				if err != nil {
					fmt.Fprintf(os.Stderr, "rpc: invalid proxy URL %q: %v\n", opt.ProxyURL, err)
				break
				}
				optsDefault.transport = transport
			}
		}
	}
	return optsDefault
}

// proxyURLToTransport converts a proxy URL to an HTTP RoundTripper that
// includes the proxy configuration. Supported schemes: http, https, socks5.
func proxyURLToTransport(url string) (http.RoundTripper, error) {
	u, err := url.Parse(url)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy URL: %w", err)
	}
	switch u.Scheme {
	case "http", "https":
		return &proxyRoundTripper{
			proxy: url.URL{
				Scheme:   u.Scheme,
				Host:     u.Host,
				Path:     u.Path,
				User:     u.User,
			},
		}, nil
	case "socks5":
		// SOCKS5 support requires golang.org/x/proxy
		return &proxyRoundTripper{
			proxy: url.URL{
				Scheme:   "socks5",
				Host:     u.Host,
				Path:     u.Path,
				User:     u.User,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported proxy scheme %q, use http, https, or socks5", u.Scheme)
	}
}

type proxyRoundTripper struct {
	proxy url.URL
}

func (pr *proxyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = pr.proxy.Host
	req.URL.Opaque = ""
	req.Proxy = func(_ *http.Request) (*url.URL, error) {
		return &pr.proxy, nil
	}
	return http.DefaultTransport.RoundTrip(req)
}

// PermanentError marks a failure that retrying cannot fix, such as a
// malformed filter or a cursor outside the server's retention window.
//
// The streaming loop retries anything not marked permanent, so misclassifying
// a permanent error merely wastes backoff attempts, while misclassifying a
// transient one aborts a stream that would have recovered. When in doubt,
// leave it transient.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the streaming loop stops rather than retrying.
func Permanent(err error) error { return &PermanentError{Err: err} }

// IsPermanent reports whether err should abort the stream instead of being
// retried.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}

// IsRetryable reports whether err looks like a transient condition worth
// retrying: a dropped connection, a timeout, or a server-side hiccup.
//
// A context cancellation is never retryable — that is the operator asking us
// to stop.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if IsPermanent(err) {
		return false
	}
	// Network-level failures are the common case and are always worth a retry.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// The SDK surfaces JSON-RPC and transport errors as plain strings, so fall
	// back to matching the shapes that indicate a server or connection problem
	// rather than a malformed request.
	msg := strings.ToLower(err.Error())
	for _, frag := range []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"eof",
		"timeout",
		"timed out",
		"temporary failure",
		"no such host",
		"server error",
		"bad gateway",
		"service unavailable",
		"gateway timeout",
		"too many requests",
		"502", "503", "504", "429",
	} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}
