package opnsense

import (
	"context"
	"fmt"
	"net/netip"

	"github.com/hzhou0/opnsense-sdk/go-sdk/generated"
)

// OPNsense requires a non-empty rule sequence but only uses it for display ordering; the operator
// keeps one rule per owner, so a constant is sufficient.
const natSequence = "1"

// syncPortForwards converges the WAN DNAT rules owned by `owner` to `desired`,
// returning whether any mutation occurred (so the caller knows whether to apply
// the firewall). A nil/empty desired set removes all of the owner's rules.
//
// As with DNS, the operator-authored description is both the ownership marker
// and the change detector: a matching description means the rule is already
// correct.
func (c *Client) syncPortForwards(ctx context.Context, owner Owner, desired []PortForward) (bool, error) {
	rows, err := c.decodeSearch(c.gen.FirewallDNatControllerSearchRuleAction(ctx))
	if err != nil {
		return false, fmt.Errorf("opnsense: search dnat rules: %w", err)
	}

	existing := map[string]row{}
	for _, r := range rows {
		if describesOwner(r.desc(), owner) {
			existing[portForwardKeyFromDescription(r.desc())] = r
		}
	}

	changed := false
	desiredKeys := map[string]struct{}{}

	for _, p := range desired {
		key := portForwardKey(p)
		desiredKeys[key] = struct{}{}
		wantDesc := natDescription(owner, p)

		if cur, ok := existing[key]; ok {
			if cur.desc() == wantDesc {
				continue
			}
			if _, err := c.decodeWrite(c.gen.FirewallDNatControllerSetRuleAction(
				ctx, cur.UUID, setNATBody(p, wantDesc))); err != nil {
				return changed, fmt.Errorf("opnsense: set dnat rule %s: %w", key, err)
			}
			changed = true
			continue
		}

		if _, err := c.decodeWrite(c.gen.FirewallDNatControllerAddRuleAction(
			ctx, addNATBody(p, wantDesc))); err != nil {
			return changed, fmt.Errorf("opnsense: add dnat rule %s: %w", key, err)
		}
		changed = true
	}

	for key, r := range existing {
		if _, ok := desiredKeys[key]; ok {
			continue
		}
		if err := c.decodeVoid(c.gen.FirewallDNatControllerDelRuleAction(ctx, r.UUID)); err != nil {
			return changed, fmt.Errorf("opnsense: del dnat rule %q: %w", key, err)
		}
		changed = true
	}

	return changed, nil
}

func validatePortForwards(rules []PortForward) error {
	for _, pf := range rules {
		target, err := netip.ParseAddr(pf.TargetIP)
		if err != nil || !target.Unmap().Is4() {
			return fmt.Errorf("opnsense: DNAT target %q is not IPv4", pf.TargetIP)
		}
	}
	return nil
}

func addNATBody(p PortForward, desc string) generated.FirewallDNatControllerAddRuleActionJSONRequestBody {
	var body generated.FirewallDNatControllerAddRuleActionJSONRequestBody
	proto := generated.FirewallDNatControllerAddRuleActionJSONBodyRuleProtocol(p.Protocol)
	ipproto := generated.FirewallDNatControllerAddRuleActionJSONBodyRuleIpprotocol("inet")
	pass := generated.FirewallDNatControllerAddRuleActionJSONBodyRulePass("pass")
	body.Rule.Disabled = strptr("0")
	body.Rule.Interface = strptr(p.Interface)
	body.Rule.Ipprotocol = &ipproto
	body.Rule.Protocol = &proto
	// The match port is destination.port, NOT a flat rule-level port; leaving source/destination
	// address nil matches any (correct for a public WAN forward). local-port/target are the redirect.
	body.Rule.Destination = &generated.FirewallDNatDestinationRequest{Port: strptr(p.ExternalPort)}
	body.Rule.Target = strptr(p.TargetIP)
	body.Rule.LocalPort = strptr(p.LocalPort)
	body.Rule.Pass = &pass
	// OPNsense persists the DNAT description in `descr`; the model's separate `description` field is
	// ignored, so writing it leaves the stored rule blank and breaks description-based ownership.
	body.Rule.Descr = strptr(desc)
	// Required, non-omitempty field: OPNsense rejects an empty sequence. It is only a display-order
	// hint, so a constant is fine — rule identity/dedup is by description, not sequence.
	body.Rule.Sequence = natSequence
	return body
}

func setNATBody(p PortForward, desc string) generated.FirewallDNatControllerSetRuleActionJSONRequestBody {
	var body generated.FirewallDNatControllerSetRuleActionJSONRequestBody
	proto := generated.FirewallDNatControllerSetRuleActionJSONBodyRuleProtocol(p.Protocol)
	ipproto := generated.FirewallDNatControllerSetRuleActionJSONBodyRuleIpprotocol("inet")
	pass := generated.FirewallDNatControllerSetRuleActionJSONBodyRulePass("pass")
	body.Rule.Disabled = strptr("0")
	body.Rule.Interface = strptr(p.Interface)
	body.Rule.Ipprotocol = &ipproto
	body.Rule.Protocol = &proto
	body.Rule.Destination = &generated.FirewallDNatDestinationRequest{Port: strptr(p.ExternalPort)}
	body.Rule.Target = strptr(p.TargetIP)
	body.Rule.LocalPort = strptr(p.LocalPort)
	body.Rule.Pass = &pass
	// OPNsense persists the DNAT description in `descr`; the model's separate `description` field is
	// ignored, so writing it leaves the stored rule blank and breaks description-based ownership.
	body.Rule.Descr = strptr(desc)
	body.Rule.Sequence = natSequence
	return body
}
