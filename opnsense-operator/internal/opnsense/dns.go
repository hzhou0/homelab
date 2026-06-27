package opnsense

import (
	"context"
	"fmt"

	"github.com/hzhou0/opnsense-sdk/go-sdk/generated"
)

// syncHostOverrides converges the set of Unbound host overrides owned by
// `owner` to `desired`. It returns whether any mutation was made (so the caller
// knows whether an Unbound reconfigure is needed). Passing a nil/empty desired
// set deletes all of the owner's overrides.
//
// Matching is done entirely through the operator-authored description: existing
// rows are keyed by their "host=" token, and a row is left untouched only when
// its full description already equals the desired one (every mutable field is
// encoded there).
func (c *Client) syncHostOverrides(ctx context.Context, owner Owner, desired []HostOverride) (bool, error) {
	rows, err := c.decodeSearch(c.gen.UnboundSettingsControllerSearchHostOverrideAction(ctx))
	if err != nil {
		return false, fmt.Errorf("opnsense: search host overrides: %w", err)
	}

	// Existing rows owned by this object, keyed by FQDN.
	existing := map[string]row{}
	for _, r := range rows {
		if describesOwner(r.Description, owner) {
			existing[hostFromDescription(r.Description)] = r
		}
	}

	changed := false
	desiredFQDNs := map[string]struct{}{}

	for _, h := range desired {
		fqdn := h.FQDN()
		desiredFQDNs[fqdn] = struct{}{}
		wantDesc := dnsDescription(owner, h)

		if cur, ok := existing[fqdn]; ok {
			if cur.Description == wantDesc {
				continue // already correct
			}
			if _, err := c.decodeWrite(c.gen.UnboundSettingsControllerSetHostOverrideAction(
				ctx, cur.UUID, setHostBody(h, wantDesc))); err != nil {
				return changed, fmt.Errorf("opnsense: set host override %s: %w", fqdn, err)
			}
			changed = true
			continue
		}

		if _, err := c.decodeWrite(c.gen.UnboundSettingsControllerAddHostOverrideAction(
			ctx, addHostBody(h, wantDesc))); err != nil {
			return changed, fmt.Errorf("opnsense: add host override %s: %w", fqdn, err)
		}
		changed = true
	}

	// Delete owned rows no longer desired.
	for fqdn, r := range existing {
		if _, ok := desiredFQDNs[fqdn]; ok {
			continue
		}
		if err := c.decodeVoid(c.gen.UnboundSettingsControllerDelHostOverrideAction(ctx, r.UUID)); err != nil {
			return changed, fmt.Errorf("opnsense: del host override %s: %w", fqdn, err)
		}
		changed = true
	}

	return changed, nil
}

func addHostBody(h HostOverride, desc string) generated.UnboundSettingsControllerAddHostOverrideActionJSONRequestBody {
	var body generated.UnboundSettingsControllerAddHostOverrideActionJSONRequestBody
	body.Host.Enabled = "1"
	body.Host.Addptr = "0"
	body.Host.Hostname = strptr(h.Host)
	body.Host.Domain = h.Domain
	body.Host.Rr = generated.UnboundSettingsControllerAddHostOverrideActionJSONBodyHostRr("A")
	body.Host.Server = strptr(h.Address)
	body.Host.Description = strptr(desc)
	return body
}

func setHostBody(h HostOverride, desc string) generated.UnboundSettingsControllerSetHostOverrideActionJSONRequestBody {
	var body generated.UnboundSettingsControllerSetHostOverrideActionJSONRequestBody
	body.Host.Enabled = "1"
	body.Host.Addptr = "0"
	body.Host.Hostname = strptr(h.Host)
	body.Host.Domain = h.Domain
	body.Host.Rr = generated.UnboundSettingsControllerSetHostOverrideActionJSONBodyHostRr("A")
	body.Host.Server = strptr(h.Address)
	body.Host.Description = strptr(desc)
	return body
}
