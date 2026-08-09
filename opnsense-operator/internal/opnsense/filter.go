package opnsense

import (
	"context"
	"fmt"
	"net/netip"
	"strings"

	"github.com/hzhou0/opnsense-sdk/go-sdk/generated"
)

// As with the DNAT sequence, only display ordering depends on this; the operator
// keeps one rule per destination and identifies rules by description.
const filterSequence = "1"

// syncPassRules converges the WAN filter rules owned by `owner` to `desired`,
// returning whether any mutation occurred (so the caller knows whether to apply
// the filter). A nil/empty desired set removes all of the owner's rules.
//
// Rules land in OPNsense's automation ruleset, which is evaluated ahead of the
// hand-maintained per-interface rules, so an operator-owned rule cannot be
// shadowed by one a human wrote.
func (c *Client) syncPassRules(ctx context.Context, owner Owner, desired []PassRule) (bool, error) {
	rows, err := c.decodeSearch(c.gen.FirewallFilterControllerSearchRuleAction(ctx))
	if err != nil {
		return false, fmt.Errorf("opnsense: search filter rules: %w", err)
	}

	existing := map[string]row{}
	for _, r := range rows {
		if describesOwner(r.desc(), owner) {
			existing[passRuleKeyFromDescription(r.desc())] = r
		}
	}

	changed := false
	desiredKeys := map[string]struct{}{}

	for _, p := range desired {
		key := passRuleKey(p)
		desiredKeys[key] = struct{}{}
		wantDesc := passDescription(owner, p)

		if cur, ok := existing[key]; ok {
			if cur.desc() == wantDesc {
				continue
			}
			if _, err := c.decodeWrite(c.gen.FirewallFilterControllerSetRuleAction(
				ctx, cur.UUID, setFilterBody(p, wantDesc))); err != nil {
				return changed, fmt.Errorf("opnsense: set filter rule %s: %w", key, err)
			}
			changed = true
			continue
		}

		if _, err := c.decodeWrite(c.gen.FirewallFilterControllerAddRuleAction(
			ctx, addFilterBody(p, wantDesc))); err != nil {
			return changed, fmt.Errorf("opnsense: add filter rule %s: %w", key, err)
		}
		changed = true
	}

	for key, r := range existing {
		if _, ok := desiredKeys[key]; ok {
			continue
		}
		if err := c.decodeVoid(c.gen.FirewallFilterControllerDelRuleAction(ctx, r.UUID)); err != nil {
			return changed, fmt.Errorf("opnsense: del filter rule %q: %w", key, err)
		}
		changed = true
	}

	return changed, nil
}

// A routable IPv4 destination would still need DNAT in this topology, so an
// IPv4 pass rule always means the caller mixed up the two mechanisms.
func validatePassRules(rules []PassRule) error {
	for _, p := range rules {
		dest, err := netip.ParseAddr(p.Destination)
		if err != nil || dest.Unmap().Is4() {
			return fmt.Errorf("opnsense: pass rule destination %q is not IPv6", p.Destination)
		}
	}
	return nil
}

// The filter model spells protocols in upper case, unlike the DNAT model.
func filterProtocol(proto string) string {
	return strings.ToUpper(proto)
}

func addFilterBody(p PassRule, desc string) generated.FirewallFilterControllerAddRuleActionJSONRequestBody {
	var body generated.FirewallFilterControllerAddRuleActionJSONRequestBody
	body.Rule.Action = generated.FirewallFilterControllerAddRuleActionJSONBodyRuleActionPass
	body.Rule.Direction = generated.FirewallFilterControllerAddRuleActionJSONBodyRuleDirectionIn
	body.Rule.Ipprotocol = generated.FirewallFilterControllerAddRuleActionJSONBodyRuleIpprotocolInet6
	body.Rule.Protocol = generated.FirewallFilterControllerAddRuleActionJSONBodyRuleProtocol(filterProtocol(p.Protocol))
	body.Rule.Statetype = generated.FirewallFilterControllerAddRuleActionJSONBodyRuleStatetypeKeep
	body.Rule.Interface = strptr(p.Interface)
	body.Rule.SourceNet = "any"
	body.Rule.DestinationNet = p.Destination
	body.Rule.DestinationPort = strptr(p.Port)
	body.Rule.Description = strptr(desc)
	// These flags are required and non-omitempty: leaving them zero submits ""
	// and OPNsense rejects the rule.
	body.Rule.Enabled = "1"
	body.Rule.Quick = "1"
	body.Rule.Sequence = filterSequence
	body.Rule.SourceNot = "0"
	body.Rule.DestinationNot = "0"
	body.Rule.Interfacenot = "0"
	body.Rule.Log = "0"
	body.Rule.Allowopts = "0"
	body.Rule.Disablereplyto = "0"
	body.Rule.Nopfsync = "0"
	body.Rule.Nosync = "0"
	return body
}

func setFilterBody(p PassRule, desc string) generated.FirewallFilterControllerSetRuleActionJSONRequestBody {
	var body generated.FirewallFilterControllerSetRuleActionJSONRequestBody
	body.Rule.Action = generated.FirewallFilterControllerSetRuleActionJSONBodyRuleActionPass
	body.Rule.Direction = generated.FirewallFilterControllerSetRuleActionJSONBodyRuleDirectionIn
	body.Rule.Ipprotocol = generated.FirewallFilterControllerSetRuleActionJSONBodyRuleIpprotocolInet6
	body.Rule.Protocol = generated.FirewallFilterControllerSetRuleActionJSONBodyRuleProtocol(filterProtocol(p.Protocol))
	body.Rule.Statetype = generated.FirewallFilterControllerSetRuleActionJSONBodyRuleStatetypeKeep
	body.Rule.Interface = strptr(p.Interface)
	body.Rule.SourceNet = "any"
	body.Rule.DestinationNet = p.Destination
	body.Rule.DestinationPort = strptr(p.Port)
	body.Rule.Description = strptr(desc)
	body.Rule.Enabled = "1"
	body.Rule.Quick = "1"
	body.Rule.Sequence = filterSequence
	body.Rule.SourceNot = "0"
	body.Rule.DestinationNot = "0"
	body.Rule.Interfacenot = "0"
	body.Rule.Log = "0"
	body.Rule.Allowopts = "0"
	body.Rule.Disablereplyto = "0"
	body.Rule.Nopfsync = "0"
	body.Rule.Nosync = "0"
	return body
}
