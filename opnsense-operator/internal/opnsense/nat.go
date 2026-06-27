package opnsense

import (
	"context"
	"fmt"

	"github.com/hzhou0/opnsense-sdk/go-sdk/generated"
)

// syncPortForward converges the single WAN DNAT rule owned by `owner` to
// `desired`. A nil desired removes the owner's rule(s). Returns whether a
// mutation occurred (so the caller knows whether to apply the firewall).
//
// As with DNS, the operator-authored description is both the ownership marker
// and the change detector: a matching description means the rule is already
// correct.
func (c *Client) syncPortForward(ctx context.Context, owner Owner, desired *PortForward) (bool, error) {
	rows, err := c.decodeSearch(c.gen.FirewallDNatControllerSearchRuleAction(ctx))
	if err != nil {
		return false, fmt.Errorf("opnsense: search dnat rules: %w", err)
	}

	var owned []row
	for _, r := range rows {
		if describesOwner(r.Description, owner) {
			owned = append(owned, r)
		}
	}

	changed := false

	if desired == nil {
		for _, r := range owned {
			if err := c.decodeVoid(c.gen.FirewallDNatControllerDelRuleAction(ctx, r.UUID)); err != nil {
				return changed, fmt.Errorf("opnsense: del dnat rule: %w", err)
			}
			changed = true
		}
		return changed, nil
	}

	wantDesc := natDescription(owner, *desired)

	// Keep the first owned rule, update it if drifted; delete any extras.
	if len(owned) > 0 {
		keep := owned[0]
		if keep.Description != wantDesc {
			if _, err := c.decodeWrite(c.gen.FirewallDNatControllerSetRuleAction(
				ctx, keep.UUID, setNATBody(*desired, wantDesc))); err != nil {
				return changed, fmt.Errorf("opnsense: set dnat rule: %w", err)
			}
			changed = true
		}
		for _, extra := range owned[1:] {
			if err := c.decodeVoid(c.gen.FirewallDNatControllerDelRuleAction(ctx, extra.UUID)); err != nil {
				return changed, fmt.Errorf("opnsense: del extra dnat rule: %w", err)
			}
			changed = true
		}
		return changed, nil
	}

	if _, err := c.decodeWrite(c.gen.FirewallDNatControllerAddRuleAction(
		ctx, addNATBody(*desired, wantDesc))); err != nil {
		return changed, fmt.Errorf("opnsense: add dnat rule: %w", err)
	}
	return true, nil
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
	body.Rule.Port = strptr(p.ExternalPort)
	body.Rule.Target = strptr(p.TargetIP)
	body.Rule.LocalPort = strptr(p.LocalPort)
	body.Rule.Pass = &pass
	body.Rule.Description = strptr(desc)
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
	body.Rule.Port = strptr(p.ExternalPort)
	body.Rule.Target = strptr(p.TargetIP)
	body.Rule.LocalPort = strptr(p.LocalPort)
	body.Rule.Pass = &pass
	body.Rule.Description = strptr(desc)
	return body
}
