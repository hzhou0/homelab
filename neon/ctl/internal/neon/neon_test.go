package neon

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestIDRoundTrip(t *testing.T) {
	const hex = "1a2b3344556677881122334455667788"

	tenant, err := ParseTenantID(hex)
	if err != nil {
		t.Fatal(err)
	}
	if tenant.String() != hex {
		t.Errorf("String() = %q, want %q", tenant, hex)
	}

	encoded, err := json.Marshal(tenant)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `"`+hex+`"` {
		t.Errorf("Marshal = %s, want a bare hex string", encoded)
	}

	var decoded TenantID
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != tenant {
		t.Errorf("round trip changed the id: %q", decoded)
	}
}

func TestIDRejectsWrongLength(t *testing.T) {
	for _, malformed := range []string{"", "abc", "1a2b3344556677881122334455667788aa", "zzzz3344556677881122334455667788"} {
		if _, err := ParseTimelineID(malformed); err == nil {
			t.Errorf("ParseTimelineID(%q) accepted", malformed)
		}
	}
}

func TestLSNFormat(t *testing.T) {
	lsn, err := ParseLSN("16/B374D848")
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(0x16)<<32 | 0xB374D848; uint64(lsn) != want {
		t.Errorf("ParseLSN = %#x, want %#x", uint64(lsn), want)
	}
	if lsn.String() != "16/B374D848" {
		t.Errorf("String() = %q", lsn)
	}
}

// ComputeMode is a Rust enum: the unit variants are bare strings and Static is an object, so a
// naive string encoding would be silently accepted and then ignored.
func TestComputeModeEncoding(t *testing.T) {
	for _, tc := range []struct {
		mode ComputeMode
		want string
	}{
		{ComputeMode{}, `"Primary"`},
		{ComputeMode{Kind: ModePrimary}, `"Primary"`},
		{ComputeMode{Kind: ModeReplica}, `"Replica"`},
		{ComputeMode{Kind: ModeStatic, LSN: 0x16B374D848}, `{"Static":"16/B374D848"}`},
	} {
		encoded, err := json.Marshal(tc.mode)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != tc.want {
			t.Errorf("Marshal(%v) = %s, want %s", tc.mode, encoded, tc.want)
			continue
		}

		var decoded ComputeMode
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		want := tc.mode
		if want.Kind == "" {
			want.Kind = ModePrimary
		}
		if decoded != want {
			t.Errorf("round trip of %s produced %v", encoded, decoded)
		}
	}
}

func TestShardNumberOrdersConnstrings(t *testing.T) {
	const tenant = "1a2b3344556677881122334455667788"
	for _, tc := range []struct {
		shardID string
		want    int
	}{
		{tenant, 0},
		{tenant + "-0004", 0},
		{tenant + "-0104", 1},
		{tenant + "-0304", 3},
		{tenant + "-ff04", 255},
	} {
		if got := (TenantLocateShard{ShardID: tc.shardID}).ShardNumber(); got != tc.want {
			t.Errorf("ShardNumber(%q) = %d, want %d", tc.shardID, got, tc.want)
		}
	}
}

// The controller sends the joint membership during a migration: current members first, then
// incoming ones, deduplicated. A compute told anything else would connect to the wrong quorum.
func TestJointSkSet(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response TimelineLocateResponse
		want     []NodeID
	}{
		{"steady state", TimelineLocateResponse{SkSet: []NodeID{1, 2, 3}}, []NodeID{1, 2, 3}},
		{"migration", TimelineLocateResponse{SkSet: []NodeID{1, 2, 3}, NewSkSet: []NodeID{2, 3, 4}}, []NodeID{1, 2, 3, 4}},
		{"disjoint", TimelineLocateResponse{SkSet: []NodeID{1}, NewSkSet: []NodeID{2}}, []NodeID{1, 2}},
	} {
		if got := tc.response.JointSkSet(); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: JointSkSet() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestPageserverConnstringQuotesIPv6(t *testing.T) {
	if got := pageserverConnstring("fd00::1", 6400); got != "postgresql://no_user@[fd00::1]:6400" {
		t.Errorf("PageserverConnstring = %q", got)
	}
	if got := safekeeperConnstring("sk-0.neon", 5454); got != "sk-0.neon:5454" {
		t.Errorf("SafekeeperConnstring = %q", got)
	}
}
