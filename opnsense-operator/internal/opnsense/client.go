// Package opnsense wraps the generated OPNsense SDK client with a small, typed
// surface tailored to this operator: managed DNS host overrides and WAN
// port-forward (DNAT) rules, plus the find-by-description ownership model and
// the batched apply/reconfigure steps OPNsense requires after mutations.
//
// The generated client returns raw *http.Response values (only request bodies
// are typed), so this package centralises status checking and JSON decoding.
package opnsense

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	opnsensesdk "github.com/hzhou0/opnsense-sdk/go-sdk"
	"github.com/hzhou0/opnsense-sdk/go-sdk/generated"

	"github.com/hzhou0/homelab/opnsense-operator/internal/config"
)

// Client is the operator's view of OPNsense.
type Client struct {
	gen *generated.Client
	wan string

	// applyMu serialises mutate+apply cycles. OPNsense's apply/reconfigure is a
	// global operation, so concurrent reconciles must not interleave.
	applyMu sync.Mutex
}

// New builds a Client from operator configuration.
func New(cfg *config.Config) (*Client, error) {
	var httpClient *http.Client
	if cfg.InsecureTLS {
		httpClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // self-signed OPNsense cert, opt-in
			},
		}
	}
	gen, err := opnsensesdk.NewClient(opnsensesdk.Options{
		BaseURL:    cfg.OPNsenseURL,
		APIKey:     cfg.APIKey,
		APISecret:  cfg.APISecret,
		HTTPClient: httpClient,
	})
	if err != nil {
		return nil, fmt.Errorf("opnsense: new client: %w", err)
	}
	return &Client{gen: gen, wan: cfg.WANInterface}, nil
}

// Sync converges OPNsense to the desired DNS overrides and (optional)
// port-forward for a single owner, then applies the relevant subsystems. It is
// safe for concurrent callers.
func (c *Client) Sync(ctx context.Context, owner Owner, dns []HostOverride, pf *PortForward) error {
	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	dnsChanged, err := c.syncHostOverrides(ctx, owner, dns)
	if err != nil {
		return err
	}
	natChanged, err := c.syncPortForward(ctx, owner, pf)
	if err != nil {
		return err
	}
	return c.apply(ctx, dnsChanged, natChanged)
}

// Delete removes every OPNsense object owned by the given Kubernetes object and
// applies. Used by the finalizer on deletion.
func (c *Client) Delete(ctx context.Context, owner Owner) error {
	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	dnsChanged, err := c.syncHostOverrides(ctx, owner, nil)
	if err != nil {
		return err
	}
	natChanged, err := c.syncPortForward(ctx, owner, nil)
	if err != nil {
		return err
	}
	return c.apply(ctx, dnsChanged, natChanged)
}

func (c *Client) apply(ctx context.Context, dnsChanged, natChanged bool) error {
	if dnsChanged {
		if err := c.decodeVoid(c.gen.UnboundServiceControllerReconfigureAction(ctx)); err != nil {
			return fmt.Errorf("opnsense: reconfigure unbound: %w", err)
		}
	}
	if natChanged {
		if err := c.decodeVoid(c.gen.FirewallDNatControllerApplyAction(ctx, "")); err != nil {
			return fmt.Errorf("opnsense: apply dnat: %w", err)
		}
	}
	return nil
}

// --- low-level response helpers -------------------------------------------------

// searchResponse is the shape OPNsense returns from *SearchAction endpoints.
type searchResponse struct {
	Rows []row `json:"rows"`
}

// row carries only the fields the operator relies on. The description is
// authored by this operator, so it is the source of truth for ownership and
// change detection; uuid is needed to address the row for Set/Del.
type row struct {
	UUID        string `json:"uuid"`
	Description string `json:"description"`
}

// writeResult is the shape OPNsense returns from Add/Set actions.
type writeResult struct {
	Result      string          `json:"result"`
	UUID        string          `json:"uuid"`
	Validations json.RawMessage `json:"validations"`
}

func readBody(resp *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("opnsense: unexpected status %d: %s", resp.StatusCode, truncate(body))
	}
	return body, nil
}

// decodeSearch decodes a search response into its rows.
func (c *Client) decodeSearch(resp *http.Response, err error) ([]row, error) {
	body, err := readBody(resp, err)
	if err != nil {
		return nil, err
	}
	var sr searchResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		return nil, fmt.Errorf("opnsense: decode search: %w", err)
	}
	return sr.Rows, nil
}

// decodeWrite decodes an Add/Set response, surfacing OPNsense validation errors
// and returning the new UUID (empty for Set).
func (c *Client) decodeWrite(resp *http.Response, err error) (string, error) {
	body, err := readBody(resp, err)
	if err != nil {
		return "", err
	}
	var wr writeResult
	if err := json.Unmarshal(body, &wr); err != nil {
		return "", fmt.Errorf("opnsense: decode write: %w", err)
	}
	if wr.Result == "failed" || len(wr.Validations) > 0 {
		return "", fmt.Errorf("opnsense: write rejected: %s", truncate(body))
	}
	return wr.UUID, nil
}

// decodeVoid checks a response that carries no payload the operator needs.
func (c *Client) decodeVoid(resp *http.Response, err error) error {
	_, err = readBody(resp, err)
	return err
}

func truncate(b []byte) string {
	const max = 512
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}
