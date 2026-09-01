// Package neon speaks to everything Neon ships. Nothing here knows about Kubernetes or about this
// deployment.
package neon

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// NodeID identifies a pageserver or a safekeeper to the storage controller.
type NodeID uint64

type rawID [16]byte

func (r rawID) String() string { return hex.EncodeToString(r[:]) }

func (r rawID) IsZero() bool { return r == rawID{} }

func (r rawID) MarshalText() ([]byte, error) {
	out := make([]byte, hex.EncodedLen(len(r)))
	hex.Encode(out, r[:])
	return out, nil
}

func (r *rawID) UnmarshalText(text []byte) error {
	if len(text) != hex.EncodedLen(len(r)) {
		return fmt.Errorf("neon: id must be %d hex characters, got %q", hex.EncodedLen(len(r)), text)
	}
	_, err := hex.Decode(r[:], text)
	return err
}

func newRawID() (rawID, error) {
	var r rawID
	_, err := rand.Read(r[:])
	return r, err
}

// TenantID and TimelineID share a representation but are deliberately distinct types: the two
// notifications are keyed differently and swapping them is otherwise silent.
type TenantID struct{ rawID }

type TimelineID struct{ rawID }

func ParseTenantID(s string) (TenantID, error) {
	var t TenantID
	return t, t.UnmarshalText([]byte(s))
}

func ParseTimelineID(s string) (TimelineID, error) {
	var t TimelineID
	return t, t.UnmarshalText([]byte(s))
}

func NewTenantID() (TenantID, error) {
	r, err := newRawID()
	return TenantID{r}, err
}

func NewTimelineID() (TimelineID, error) {
	r, err := newRawID()
	return TimelineID{r}, err
}

type LSN uint64

func (l LSN) String() string { return fmt.Sprintf("%X/%X", uint32(l>>32), uint32(l)) }

func (l LSN) MarshalText() ([]byte, error) { return []byte(l.String()), nil }

func (l *LSN) UnmarshalText(text []byte) error {
	var hi, lo uint32
	if _, err := fmt.Sscanf(string(text), "%x/%x", &hi, &lo); err != nil {
		return fmt.Errorf("neon: malformed lsn %q: %w", text, err)
	}
	*l = LSN(hi)<<32 | LSN(lo)
	return nil
}

func ParseLSN(s string) (LSN, error) {
	var l LSN
	return l, l.UnmarshalText([]byte(s))
}

var ErrNotFound = errors.New("neon: not found")

// APIError carries the status of a failed call so callers can distinguish a missing object from
// an unavailable server, which is the difference between a 404 and a retry.
type APIError struct {
	Status int
	Method string
	Path   string
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("neon: %s %s: %d: %s", e.Method, e.Path, e.Status, e.Body)
}

func (e *APIError) Is(target error) bool {
	return target == ErrNotFound && e.Status == http.StatusNotFound
}

// httpClient is the one JSON exchange every Neon component speaks, so the storage controller and
// compute_ctl clients differ only in their routes.
type httpClient struct {
	base  string
	token string
	http  *http.Client
}

func newHTTPClient(baseURL, token string, client *http.Client) *httpClient {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &httpClient{base: strings.TrimSuffix(baseURL, "/"), token: token, http: client}
}

// do issues one request. A nil body sends none, and a nil out discards the response.
func (c *httpClient) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("neon: encoding %s %s: %w", method, path, err)
		}
		reader = bytes.NewReader(encoded)
	}

	target := c.base + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("neon: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &APIError{Status: resp.StatusCode, Method: method, Path: path, Body: strings.TrimSpace(string(detail))}
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("neon: decoding %s %s: %w", method, path, err)
	}
	return nil
}

// ComputeSpec carries only the fields we set. Everything else upstream declares is optional or
// defaulted, so a field needed later has to be added here first.
type ComputeSpec struct {
	FormatVersion float32 `json:"format_version"`

	Cluster Cluster `json:"cluster"`

	// Set when the spec carries no catalog, which is how a compute still boots while the registry
	// is unreachable: the catalog lives in the timeline, so an empty one must mutate nothing.
	SkipPgCatalogUpdates bool `json:"skip_pg_catalog_updates"`

	TenantID   *TenantID   `json:"tenant_id,omitempty"`
	TimelineID *TimelineID `json:"timeline_id,omitempty"`

	// The modern equivalent is pageserver_connection_info. compute_ctl falls back to these two
	// while both are accepted, and they carry no shard-index encoding to keep in step.
	PageserverConnstring *string `json:"pageserver_connstring,omitempty"`
	ShardStripeSize      *uint32 `json:"shard_stripe_size,omitempty"`

	// Must never regress: walproposer compares generations to decide whether an incoming
	// membership configuration is newer than the one it is using.
	SafekeepersGeneration *uint32  `json:"safekeepers_generation,omitempty"`
	SafekeeperConnstrings []string `json:"safekeeper_connstrings"`

	// What the compute presents to its pageserver and safekeepers. They refuse the connection
	// without it unless they are running with auth off.
	StorageAuthToken *string `json:"storage_auth_token,omitempty"`

	Mode ComputeMode `json:"mode"`

	BranchID   *string `json:"branch_id,omitempty"`
	EndpointID *string `json:"endpoint_id,omitempty"`

	ReconfigureConcurrency int   `json:"reconfigure_concurrency"`
	SuspendTimeoutSeconds  int64 `json:"suspend_timeout_seconds"`
}

type Cluster struct {
	ClusterID *string `json:"cluster_id"`
	Name      *string `json:"name"`

	Roles     []Role     `json:"roles"`
	Databases []Database `json:"databases"`

	Settings []GenericOption `json:"settings"`
}

type Role struct {
	Name string `json:"name"`
	// A Postgres SCRAM verifier, not a password: the same string the proxy parses as its
	// role_secret, so one value serves authentication at both ends.
	EncryptedPassword *string         `json:"encrypted_password"`
	Options           []GenericOption `json:"options"`
}

type Database struct {
	Name    string          `json:"name"`
	Owner   string          `json:"owner"`
	Options []GenericOption `json:"options"`
}

type GenericOption struct {
	Name    string  `json:"name"`
	Value   *string `json:"value"`
	VarType string  `json:"vartype"`
}

type ComputeModeKind string

const (
	ModePrimary ComputeModeKind = "Primary"
	ModeReplica ComputeModeKind = "Replica"
	ModeStatic  ComputeModeKind = "Static"
)

// ComputeMode is a Rust enum on the wire: the unit variants are bare strings, Static is an
// object carrying the LSN it is pinned at.
type ComputeMode struct {
	Kind ComputeModeKind
	LSN  LSN
}

func (m ComputeMode) MarshalJSON() ([]byte, error) {
	switch m.Kind {
	case "", ModePrimary:
		return json.Marshal(string(ModePrimary))
	case ModeReplica:
		return json.Marshal(string(ModeReplica))
	case ModeStatic:
		return json.Marshal(map[string]LSN{string(ModeStatic): m.LSN})
	default:
		return nil, fmt.Errorf("neon: unknown compute mode %q", m.Kind)
	}
}

func (m *ComputeMode) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err == nil {
		switch ComputeModeKind(name) {
		case ModePrimary, ModeReplica:
			*m = ComputeMode{Kind: ComputeModeKind(name)}
			return nil
		}
		return fmt.Errorf("neon: unknown compute mode %q", name)
	}
	var static struct {
		Static LSN `json:"Static"`
	}
	if err := json.Unmarshal(data, &static); err != nil {
		return fmt.Errorf("neon: malformed compute mode: %w", err)
	}
	*m = ComputeMode{Kind: ModeStatic, LSN: static.Static}
	return nil
}

func ParseComputeMode(s string) (ComputeMode, error) {
	switch ComputeModeKind(s) {
	case "", ModePrimary:
		return ComputeMode{Kind: ModePrimary}, nil
	case ModeReplica:
		return ComputeMode{Kind: ModeReplica}, nil
	}
	return ComputeMode{}, fmt.Errorf("neon: unknown compute mode %q", s)
}

// ComputeCtlConfig configures compute_ctl itself rather than Postgres, and is returned alongside
// the spec whether or not a spec exists. An empty key set disables its request authentication.
type ComputeCtlConfig struct {
	JWKS JWKSet     `json:"jwks"`
	TLS  *TLSConfig `json:"tls"`
}

type JWKSet struct {
	Keys []json.RawMessage `json:"keys"`
}

type TLSConfig struct {
	KeyPath  string `json:"key_path"`
	CertPath string `json:"cert_path"`
}

// compute_ctl learns this key's public half from the spec it is served, so the two travel together
// and a compute trusts nothing it was not handed. Ed25519 because the only other path is RS256.
type SigningKey struct {
	private ed25519.PrivateKey
	kid     string
}

func NewSigningKey(seed []byte) (*SigningKey, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("neon: a signing key needs a %d byte seed", ed25519.SeedSize)
	}
	private := ed25519.NewKeyFromSeed(seed)
	sum := sha256.Sum256(private.Public().(ed25519.PublicKey))
	return &SigningKey{private: private, kid: base64.RawURLEncoding.EncodeToString(sum[:8])}, nil
}

func NewSeed() ([]byte, error) {
	seed := make([]byte, ed25519.SeedSize)
	_, err := rand.Read(seed)
	return seed, err
}

// ComputeCtlConfig is served with every spec, so a key change reaches a compute on its next pull.
func (k *SigningKey) ComputeCtlConfig() ComputeCtlConfig {
	public := k.private.Public().(ed25519.PublicKey)
	jwk := fmt.Sprintf(`{"kty":"OKP","crv":"Ed25519","alg":"EdDSA","use":"sig","kid":%q,"x":%q}`,
		k.kid, base64.RawURLEncoding.EncodeToString(public))
	return ComputeCtlConfig{JWKS: JWKSet{Keys: []json.RawMessage{json.RawMessage(jwk)}}}
}

// A token names the compute it is for: compute_ctl refuses one whose compute_id is not its own,
// so a token leaked from one compute cannot drive another.
func (k *SigningKey) Token(computeID string, lifetime time.Duration) (string, error) {
	header := fmt.Sprintf(`{"alg":"EdDSA","typ":"JWT","kid":%q}`, k.kid)
	claims := fmt.Sprintf(`{"compute_id":%q,"exp":%d}`, computeID, time.Now().Add(lifetime).Unix())

	signing := base64.RawURLEncoding.EncodeToString([]byte(header)) +
		"." + base64.RawURLEncoding.EncodeToString([]byte(claims))
	signature := ed25519.Sign(k.private, []byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

type ControlPlaneComputeStatus string

const (
	// StatusEmpty is a compute the control plane knows about that is not bound to a timeline. It
	// is a normal state, not an error, and must not be reported as 404.
	StatusEmpty    ControlPlaneComputeStatus = "empty"
	StatusAttached ControlPlaneComputeStatus = "attached"
)

// ControlPlaneConfigResponse is the body of the spec endpoint compute_ctl polls.
type ControlPlaneConfigResponse struct {
	Spec             *ComputeSpec              `json:"spec"`
	Status           ControlPlaneComputeStatus `json:"status"`
	ComputeCtlConfig ComputeCtlConfig          `json:"compute_ctl_config"`
}

// ComputeConfig is the body of compute_ctl's /configure.
type ComputeConfig struct {
	Spec             *ComputeSpec     `json:"spec"`
	ComputeCtlConfig ComputeCtlConfig `json:"compute_ctl_config"`
}

// StorageController is the authority on placement, and it is read live rather than cached: a
// stale answer on the spec path wedges a compute in a retry loop.
type StorageController struct {
	*httpClient
}

func NewStorageController(baseURL, token string, client *http.Client) (*StorageController, error) {
	if _, err := url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("neon: storage controller url: %w", err)
	}
	return &StorageController{newHTTPClient(baseURL, token, client)}, nil
}

// Only what a compute is told about. Pageserver addresses come with the shard that is attached,
// so nothing here describes one.
type SafekeeperDescribe struct {
	ID   NodeID `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
}

type TenantLocateShard struct {
	ShardID      string `json:"shard_id"`
	NodeID       NodeID `json:"node_id"`
	ListenPgAddr string `json:"listen_pg_addr"`
	ListenPgPort uint16 `json:"listen_pg_port"`
}

func (s TenantLocateShard) ShardNumber() int { return shardNumber(s.ShardID) }

type ShardParameters struct {
	Count      uint8  `json:"count"`
	StripeSize uint32 `json:"stripe_size"`
}

type TenantLocateResponse struct {
	Shards      []TenantLocateShard `json:"shards"`
	ShardParams ShardParameters     `json:"shard_params"`
}

type TimelineLocateResponse struct {
	Generation uint32   `json:"generation"`
	SkSet      []NodeID `json:"sk_set"`
	NewSkSet   []NodeID `json:"new_sk_set"`
}

// JointSkSet is the safekeeper set a compute must be told about, matching what the controller
// sends during a membership change: the current members followed by any incoming ones.
func (r TimelineLocateResponse) JointSkSet() []NodeID {
	seen := make(map[NodeID]struct{}, len(r.SkSet)+len(r.NewSkSet))
	joint := make([]NodeID, 0, len(r.SkSet)+len(r.NewSkSet))
	for _, id := range append(append([]NodeID{}, r.SkSet...), r.NewSkSet...) {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		joint = append(joint, id)
	}
	return joint
}

type TenantCreateRequest struct {
	NewTenantID TenantID `json:"new_tenant_id"`
}

// TimelineCreateRequest is an untagged Rust enum flattened into the body: supplying
// AncestorTimelineID selects branching, omitting it selects bootstrap.
type TimelineCreateRequest struct {
	NewTimelineID      TimelineID  `json:"new_timeline_id"`
	AncestorTimelineID *TimelineID `json:"ancestor_timeline_id,omitempty"`
	AncestorStartLSN   *LSN        `json:"ancestor_start_lsn,omitempty"`
	PgVersion          *int        `json:"pg_version,omitempty"`
}

// Ping is a readiness probe: the node list is the cheapest endpoint that proves the controller
// has a database behind it, and nothing here reads the answer.
func (c *StorageController) Ping(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/control/v1/node", nil, nil, nil)
}

func (c *StorageController) safekeepers(ctx context.Context) ([]SafekeeperDescribe, error) {
	var safekeepers []SafekeeperDescribe
	return safekeepers, c.do(ctx, http.MethodGet, "/control/v1/safekeeper", nil, nil, &safekeepers)
}

func (c *StorageController) LocateTenant(ctx context.Context, tenant TenantID) (*TenantLocateResponse, error) {
	var located TenantLocateResponse
	path := fmt.Sprintf("/debug/v1/tenant/%s/locate", tenant)
	return &located, c.do(ctx, http.MethodGet, path, nil, nil, &located)
}

func (c *StorageController) LocateTimeline(ctx context.Context, tenant TenantID, timeline TimelineID) (*TimelineLocateResponse, error) {
	var located TimelineLocateResponse
	path := fmt.Sprintf("/debug/v1/tenant/%s/timeline/%s/locate", tenant, timeline)
	return &located, c.do(ctx, http.MethodGet, path, nil, nil, &located)
}

// The whole record the controller keeps. Beyond the id it is reporting: the version is stored and
// never read back, and the `active` field it documents as ignored.
type SafekeeperUpsert struct {
	ID                 NodeID `json:"id"`
	RegionID           string `json:"region_id"`
	Version            int64  `json:"version"`
	Host               string `json:"host"`
	Port               int32  `json:"port"`
	HTTPPort           int32  `json:"http_port"`
	AvailabilityZoneID string `json:"availability_zone_id"`
}

// Nothing in Neon does this for itself. Repeating it is free, and it leaves a scheduling policy
// set by hand alone, which is what makes it safe on a timer.
func (c *StorageController) UpsertSafekeeper(ctx context.Context, safekeeper SafekeeperUpsert) error {
	path := fmt.Sprintf("/control/v1/safekeeper/%d", safekeeper.ID)
	return c.do(ctx, http.MethodPost, path, nil, safekeeper, nil)
}

func (c *StorageController) CreateTenant(ctx context.Context, tenant TenantID) error {
	return c.do(ctx, http.MethodPost, "/v1/tenant", nil, TenantCreateRequest{NewTenantID: tenant}, nil)
}

func (c *StorageController) CreateTimeline(ctx context.Context, tenant TenantID, req TimelineCreateRequest) error {
	path := fmt.Sprintf("/v1/tenant/%s/timeline", tenant)
	return c.do(ctx, http.MethodPost, path, nil, req, nil)
}

func (c *StorageController) DeleteTenant(ctx context.Context, tenant TenantID) error {
	err := c.do(ctx, http.MethodDelete, fmt.Sprintf("/v1/tenant/%s", tenant), nil, nil, nil)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (c *StorageController) DeleteTimeline(ctx context.Context, tenant TenantID, timeline TimelineID) error {
	path := fmt.Sprintf("/v1/tenant/%s/timeline/%s", tenant, timeline)
	err := c.do(ctx, http.MethodDelete, path, nil, nil, nil)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

// Placement is everything about a timeline's storage that a compute needs and that only the
// storage controller knows. Both halves are resolved together because a spec carries both.
type Placement struct {
	PageserverConnstring  string
	ShardStripeSize       *uint32
	SafekeeperConnstrings []string
	SafekeepersGeneration uint32
}

// ResolvePlacement reads placement live from the controller. Notifications carry node ids rather
// than addresses, so every path — push and pull alike — ends up here.
func (c *StorageController) ResolvePlacement(ctx context.Context, tenant TenantID, timeline TimelineID) (*Placement, error) {
	located, err := c.LocateTenant(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if len(located.Shards) == 0 {
		return nil, fmt.Errorf("neon: tenant %s has no attached shards", tenant)
	}

	shards := append([]TenantLocateShard{}, located.Shards...)
	sort.Slice(shards, func(i, j int) bool {
		return shardNumber(shards[i].ShardID) < shardNumber(shards[j].ShardID)
	})

	connstrings := make([]string, 0, len(shards))
	for _, shard := range shards {
		connstrings = append(connstrings, pageserverConnstring(shard.ListenPgAddr, shard.ListenPgPort))
	}

	placement := &Placement{PageserverConnstring: strings.Join(connstrings, ",")}
	if len(shards) > 1 {
		stripe := located.ShardParams.StripeSize
		placement.ShardStripeSize = &stripe
	}

	timelineLoc, err := c.LocateTimeline(ctx, tenant, timeline)
	if err != nil {
		return nil, err
	}
	placement.SafekeepersGeneration = timelineLoc.Generation

	safekeepers, err := c.safekeepers(ctx)
	if err != nil {
		return nil, err
	}
	byID := make(map[NodeID]SafekeeperDescribe, len(safekeepers))
	for _, sk := range safekeepers {
		byID[sk.ID] = sk
	}
	for _, id := range timelineLoc.JointSkSet() {
		sk, known := byID[id]
		if !known {
			return nil, fmt.Errorf("neon: timeline %s names safekeeper %d the controller does not list", timeline, id)
		}
		placement.SafekeeperConnstrings = append(placement.SafekeeperConnstrings, safekeeperConnstring(sk.Host, sk.Port))
	}
	if len(placement.SafekeeperConnstrings) == 0 {
		return nil, fmt.Errorf("neon: timeline %s has no safekeepers", timeline)
	}
	return placement, nil
}

func pageserverConnstring(host string, port uint16) string {
	return fmt.Sprintf("postgresql://no_user@%s", hostPort(host, int(port)))
}

// A bare address: walproposer's GUC is a host:port list, not a URI.
func safekeeperConnstring(host string, port int32) string {
	return hostPort(host, int(port))
}

func hostPort(host string, port int) string {
	if strings.Contains(host, ":") {
		return fmt.Sprintf("[%s]:%d", host, port)
	}
	return fmt.Sprintf("%s:%d", host, port)
}

// shardNumber recovers the ordering position from a TenantShardId, which suffixes the tenant with
// a shard number and count in hex. An unsharded id has no suffix and sorts first.
func shardNumber(shardID string) int {
	dash := strings.LastIndex(shardID, "-")
	if dash < 0 || len(shardID)-dash != 5 {
		return 0
	}
	number, err := strconv.ParseUint(shardID[dash+1:dash+3], 16, 8)
	if err != nil {
		return 0
	}
	return int(number)
}

// ComputeStatus is compute_ctl's own state machine, passed through to callers as it comes. Only
// the one state this service acts on is named.
type ComputeStatus string

const ComputeRunning ComputeStatus = "running"

type ComputeStatusResponse struct {
	StartTime  time.Time     `json:"start_time"`
	Tenant     *string       `json:"tenant"`
	Timeline   *string       `json:"timeline"`
	Status     ComputeStatus `json:"status"`
	LastActive *time.Time    `json:"last_active"`
	Error      *string       `json:"error"`
}

type TerminateMode string

const TerminateFast TerminateMode = "fast"

// ComputeCtl pushes configuration as the fast path. The compute never learns a push was
// attempted, so its own fetch of the spec endpoint is the only recovery.
type ComputeCtl struct {
	*httpClient
	key *SigningKey
}

// The key is passed rather than a token and a config, because the token a compute accepts and the
// key set it validates against have to come from the same place or a push authenticates nowhere.
func NewComputeCtl(baseURL, computeID string, key *SigningKey, client *http.Client) (*ComputeCtl, error) {
	token, err := key.Token(computeID, tokenLifetime)
	if err != nil {
		return nil, err
	}
	return &ComputeCtl{httpClient: newHTTPClient(baseURL, token, client), key: key}, nil
}

// Long enough to outlast a slow reconfigure, short enough that a captured token is worth little.
const tokenLifetime = 5 * time.Minute

func (c *ComputeCtl) Status(ctx context.Context) (*ComputeStatusResponse, error) {
	var status ComputeStatusResponse
	return &status, c.do(ctx, http.MethodGet, "/status", nil, nil, &status)
}

func (c *ComputeCtl) Configure(ctx context.Context, spec *ComputeSpec) error {
	config := ComputeConfig{Spec: spec, ComputeCtlConfig: c.key.ComputeCtlConfig()}
	return c.do(ctx, http.MethodPost, "/configure", nil, config, nil)
}

func (c *ComputeCtl) Terminate(ctx context.Context, mode TerminateMode) (*LSN, error) {
	var terminated struct {
		LSN *LSN `json:"lsn"`
	}
	query := url.Values{"mode": {string(mode)}}
	if err := c.do(ctx, http.MethodPost, "/terminate", query, nil, &terminated); err != nil {
		return nil, err
	}
	return terminated.LSN, nil
}

// NotifyAttachRequest is the body the storage controller PUTs when a tenant's shards change
// where they are attached. It is keyed by tenant, so it concerns every timeline of that tenant.
type NotifyAttachRequest struct {
	TenantID    TenantID            `json:"tenant_id"`
	PreferredAZ *string             `json:"preferred_az"`
	StripeSize  *uint32             `json:"stripe_size"`
	Shards      []NotifyAttachShard `json:"shards"`
}

type NotifyAttachShard struct {
	NodeID      NodeID `json:"node_id"`
	ShardNumber uint8  `json:"shard_number"`
}

// NotifySafekeepersRequest is keyed by timeline, because safekeeper membership is per timeline
// while shard attachment is per tenant.
type NotifySafekeepersRequest struct {
	TenantID    TenantID         `json:"tenant_id"`
	TimelineID  TimelineID       `json:"timeline_id"`
	Generation  uint32           `json:"generation"`
	Safekeepers []SafekeeperInfo `json:"safekeepers"`
}

// SafekeeperInfo carries Hostname for debuggability only; it may be absent, so addresses are
// resolved from the controller's safekeeper listing instead.
type SafekeeperInfo struct {
	ID       NodeID  `json:"id"`
	Hostname *string `json:"hostname"`
}

// EdDSA, and the claims carry no expiry: a storage token is valid until the key behind it
// changes.
type StorageScope string

const (
	ScopeTenant         StorageScope = "tenant"
	ScopeAdmin          StorageScope = "admin"
	ScopePageServerAPI  StorageScope = "pageserverapi"
	ScopeSafekeeperAPI  StorageScope = "safekeeperdata"
	ScopeGenerationsAPI StorageScope = "generations_api"
)

// Neon matches the scope exhaustively and answers an unknown one with the same 403 it gives a
// forged token, so a misspelling is indistinguishable from an attack until someone reads a log.
func (s StorageScope) valid() bool {
	switch s {
	case ScopeTenant, ScopeAdmin, ScopePageServerAPI, ScopeSafekeeperAPI, ScopeGenerationsAPI:
		return true
	}
	return false
}

type StorageClaims struct {
	TenantID *TenantID    `json:"tenant_id,omitempty"`
	Scope    StorageScope `json:"scope"`
}

// StorageKey signs those tokens. The key is supplied rather than generated: every storage
// component validates against its public half, so it has to exist before any of them start.
type StorageKey struct {
	private ed25519.PrivateKey
}

// NewStorageKey reads the PKCS#8 PEM that `openssl genpkey -algorithm ed25519` writes.
func NewStorageKey(pemBytes []byte) (*StorageKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("neon: storage key is not PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("neon: storage key: %w", err)
	}
	private, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("neon: storage key is %T, want ed25519", parsed)
	}
	return &StorageKey{private: private}, nil
}

// The public half is derived rather than configured: one PEM in, and the signer and the verifier
// cannot disagree about which key pair they belong to.
func (k *StorageKey) Verifier() *StorageVerifier {
	return &StorageVerifier{public: k.private.Public().(ed25519.PublicKey)}
}

// PublicKeyPEM is the SPKI PEM the storage components validate against, derived rather than
// carried alongside so the two can never be a mismatched pair.
func (k *StorageKey) PublicKeyPEM() ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(k.private.Public())
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}

func (k *StorageKey) Token(claims StorageClaims) (string, error) {
	if !claims.Scope.valid() {
		return "", fmt.Errorf("neon: unknown storage scope %q", claims.Scope)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	signing := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"EdDSA","typ":"JWT"}`)) +
		"." + base64.RawURLEncoding.EncodeToString(payload)
	return signing + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(k.private, []byte(signing))), nil
}

// StorageVerifier checks tokens minted by the same key, which is how an inbound notification is
// told apart from anything else that can reach the port.
type StorageVerifier struct {
	public ed25519.PublicKey
}

// NewStorageVerifier reads the SPKI PEM that `openssl pkey -pubout` writes.
func NewStorageVerifier(pemBytes []byte) (*StorageVerifier, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("neon: storage public key is not PEM")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("neon: storage public key: %w", err)
	}
	public, ok := parsed.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("neon: storage public key is %T, want ed25519", parsed)
	}
	return &StorageVerifier{public: public}, nil
}

func (v *StorageVerifier) Verify(token string) (StorageClaims, error) {
	var claims StorageClaims
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return claims, errors.New("neon: token is not a JWT")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return claims, errors.New("neon: token signature is not base64url")
	}
	if !ed25519.Verify(v.public, []byte(parts[0]+"."+parts[1]), signature) {
		return claims, errors.New("neon: token signature does not verify")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return claims, errors.New("neon: token claims are not base64url")
	}
	return claims, json.Unmarshal(payload, &claims)
}
