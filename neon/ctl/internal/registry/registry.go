// Package registry holds what a branch is and how branches are stored. It is the only state this
// service owns that cannot be rebuilt from object storage.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"go.etcd.io/bbolt"

	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	"github.com/hzhou0/homelab/neon/ctl/internal/scram"
)

var (
	ErrNotFound    = errors.New("registry: branch not found")
	ErrInvalidName = errors.New("registry: invalid branch name")
)

// A branch name reaches Kubernetes object names and a proxy endpoint id, so the narrowest of
// those wins.
var branchName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,38}[a-z0-9])?$`)

func ValidateName(name string) error {
	if !branchName.MatchString(name) {
		return fmt.Errorf("%w: %q must be lowercase alphanumerics and dashes, 1-40 characters, starting and ending alphanumeric", ErrInvalidName, name)
	}
	return nil
}

// A Branch is a timeline plus the facts about it that only we hold: its name, its credentials and
// its settings. The tags are the stored shape, so renaming a field here does not orphan a branch.
type Branch struct {
	Name       string          `json:"name"`
	TenantID   neon.TenantID   `json:"tenant_id"`
	TimelineID neon.TimelineID `json:"timeline_id"`

	// Both are kept because a fork keeps its lineage even if the parent is renamed or removed.
	Parent           string           `json:"parent,omitempty"`
	ParentTimelineID *neon.TimelineID `json:"parent_timeline_id,omitempty"`
	ParentLSN        *neon.LSN        `json:"parent_lsn,omitempty"`

	PgVersion int              `json:"pg_version"`
	Mode      neon.ComputeMode `json:"mode"`

	Roles     []Role     `json:"roles,omitempty"`
	Databases []Database `json:"databases,omitempty"`
	Settings  []Setting  `json:"settings,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Role holds a Postgres SCRAM verifier rather than a password. The same string authenticates at
// the proxy and provisions the role on the compute, and no plaintext is ever persisted.
type Role struct {
	Name     string `json:"name"`
	Verifier string `json:"verifier"`
}

type Database struct {
	Name  string `json:"name"`
	Owner string `json:"owner"`
}

type Setting struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	VarType string `json:"vartype"`
}

// Spec is a request for a branch, before anything has been created. It carries no Postgres
// version: the compute image decides that, and it is not a branch's to choose.
type Spec struct {
	Name      string
	Mode      neon.ComputeMode
	ParentLSN *neon.LSN

	Roles     []RoleSpec
	Databases []Database
	Settings  []Setting
}

// RoleSpec accepts either half of the credential so a caller may hand over a password to be
// hashed or a verifier it already holds.
type RoleSpec struct {
	Name     string
	Password string
	Verifier string
}

// New creates a root branch. Both trailing arguments are facts the caller holds and this package
// cannot invent; a branch recording a Postgres version its image does not run would not start.
func New(spec Spec, pgVersion int, tenant neon.TenantID) (*Branch, error) {
	if tenant.IsZero() {
		return nil, errors.New("registry: a root branch needs a tenant")
	}
	if pgVersion == 0 {
		return nil, errors.New("registry: a root branch needs the compute's postgres version")
	}
	return build(spec, pgVersion, tenant, nil)
}

// Fork derives a child. A timeline can only be branched inside its own tenant, and the catalog
// comes with the timeline, so both are inherited unless the spec says otherwise.
func (b *Branch) Fork(spec Spec) (*Branch, error) {
	if len(spec.Roles) == 0 {
		spec.Roles = b.roleSpecs()
	}
	if len(spec.Databases) == 0 {
		spec.Databases = b.Databases
	}
	if len(spec.Settings) == 0 {
		spec.Settings = b.Settings
	}
	// Neon's branching code always inherits the ancestor's version, which is why a fork is told
	// nothing about it.
	return build(spec, b.PgVersion, b.TenantID, b)
}

func build(spec Spec, pgVersion int, tenant neon.TenantID, parent *Branch) (*Branch, error) {
	if err := ValidateName(spec.Name); err != nil {
		return nil, err
	}

	roles, err := resolveRoles(spec.Roles)
	if err != nil {
		return nil, err
	}

	mode := spec.Mode
	if mode.Kind == "" {
		mode.Kind = neon.ModePrimary
	}

	timeline, err := neon.NewTimelineID()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	branch := &Branch{
		Name:       spec.Name,
		TenantID:   tenant,
		TimelineID: timeline,
		PgVersion:  pgVersion,
		Mode:       mode,
		Roles:      roles,
		Databases:  spec.Databases,
		Settings:   spec.Settings,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if parent != nil {
		ancestor := parent.TimelineID
		branch.Parent = parent.Name
		branch.ParentTimelineID = &ancestor
		branch.ParentLSN = spec.ParentLSN
	}

	if err := branch.Validate(); err != nil {
		return nil, err
	}
	return branch, nil
}

// Patch is a partial update. A nil field is untouched; an empty slice clears.
type Patch struct {
	Roles     *[]RoleSpec
	Databases *[]Database
	Settings  *[]Setting
}

func (b *Branch) Apply(patch Patch) error {
	updated := *b
	if patch.Roles != nil {
		roles, err := resolveRoles(*patch.Roles)
		if err != nil {
			return err
		}
		updated.Roles = roles
	}
	if patch.Databases != nil {
		updated.Databases = *patch.Databases
	}
	if patch.Settings != nil {
		updated.Settings = *patch.Settings
	}
	if err := updated.Validate(); err != nil {
		return err
	}
	updated.UpdatedAt = time.Now().UTC()
	*b = updated
	return nil
}

func (b *Branch) Validate() error {
	if err := ValidateName(b.Name); err != nil {
		return err
	}
	if b.TenantID.IsZero() {
		return errors.New("registry: a branch needs a tenant")
	}
	if len(b.Roles) == 0 {
		return errors.New("registry: a branch needs at least one role to be reachable")
	}
	owners := map[string]bool{}
	for _, role := range b.Roles {
		owners[role.Name] = true
	}
	for _, database := range b.Databases {
		if !owners[database.Owner] {
			return fmt.Errorf("registry: database %q is owned by %q, which is not a role on this branch", database.Name, database.Owner)
		}
	}
	return nil
}

// TimelineCreateRequest is how this branch asks the storage controller to exist. The request is an
// untagged enum: naming an ancestor selects branching, omitting one selects bootstrap.
func (b *Branch) TimelineCreateRequest() neon.TimelineCreateRequest {
	pgVersion := b.PgVersion
	request := neon.TimelineCreateRequest{
		NewTimelineID: b.TimelineID,
		PgVersion:     &pgVersion,
	}
	if b.ParentTimelineID != nil {
		request.AncestorTimelineID = b.ParentTimelineID
		request.AncestorStartLSN = b.ParentLSN
	}
	return request
}

func (b *Branch) Role(name string) (Role, bool) {
	for _, role := range b.Roles {
		if role.Name == name {
			return role, true
		}
	}
	return Role{}, false
}

// roleSpecs re-expresses the branch's roles as a spec, so a fork inherits credentials without
// anything having to hold a password.
func (b *Branch) roleSpecs() []RoleSpec {
	specs := make([]RoleSpec, 0, len(b.Roles))
	for _, role := range b.Roles {
		specs = append(specs, RoleSpec{Name: role.Name, Verifier: role.Verifier})
	}
	return specs
}

func resolveRoles(specs []RoleSpec) ([]Role, error) {
	roles := make([]Role, 0, len(specs))
	for _, spec := range specs {
		switch {
		case spec.Name == "":
			return nil, errors.New("registry: a role needs a name")
		case spec.Verifier != "":
			if !scram.IsVerifier(spec.Verifier) {
				return nil, fmt.Errorf("registry: role %q: verifier is not a SCRAM-SHA-256 secret", spec.Name)
			}
			roles = append(roles, Role{Name: spec.Name, Verifier: spec.Verifier})
		case spec.Password != "":
			verifier, err := scram.Verifier(spec.Password)
			if err != nil {
				return nil, err
			}
			roles = append(roles, Role{Name: spec.Name, Verifier: verifier})
		default:
			return nil, fmt.Errorf("registry: role %q needs a password or a verifier", spec.Name)
		}
	}
	return roles, nil
}

// Store is the branch registry: one bucket, one branch per key, each written and read whole.
// Nothing here queries inside a branch, so a key-value file is the whole of what is needed.
type Store struct {
	db *bbolt.DB
}

var (
	bucket = []byte("branches")
	// A compute trusts only the key set it was served, so a restart that minted a new signing key
	// could no longer reconfigure anything already running.
	metaBucket = []byte("meta")
)

// The file is held under an exclusive lock, so two overlapping deployments fail here rather than
// one of them waiting forever on the other.
func Open(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("registry: opening %s: %w", path, err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, name := range [][]byte{bucket, metaBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("registry: preparing %s: %w", path, err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Meta returns nil when nothing is stored, so a caller can generate and Put on first run.
func (s *Store) Meta(ctx context.Context, name string) ([]byte, error) {
	var value []byte
	err := s.db.View(func(tx *bbolt.Tx) error {
		if stored := tx.Bucket(metaBucket).Get([]byte(name)); stored != nil {
			value = append([]byte(nil), stored...)
		}
		return nil
	})
	return value, err
}

func (s *Store) PutMeta(ctx context.Context, name string, value []byte) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(metaBucket).Put([]byte(name), value)
	})
}

func (s *Store) Get(ctx context.Context, name string) (*Branch, error) {
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	var branch *Branch
	err := s.db.View(func(tx *bbolt.Tx) error {
		stored := tx.Bucket(bucket).Get([]byte(name))
		if stored == nil {
			return fmt.Errorf("%w: %s", ErrNotFound, name)
		}
		var decoded Branch
		if err := json.Unmarshal(stored, &decoded); err != nil {
			return fmt.Errorf("registry: branch %s: %w", name, err)
		}
		branch = &decoded
		return nil
	})
	return branch, err
}

// Branch names sort bytewise the same way they sort lexically, so the walk is already ordered.
func (s *Store) List(ctx context.Context) ([]Branch, error) {
	var branches []Branch
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).ForEach(func(name, stored []byte) error {
			var branch Branch
			if err := json.Unmarshal(stored, &branch); err != nil {
				return fmt.Errorf("registry: branch %s: %w", name, err)
			}
			branches = append(branches, branch)
			return nil
		})
	})
	return branches, err
}

func (s *Store) Put(ctx context.Context, branch *Branch) error {
	encoded, err := json.Marshal(branch)
	if err != nil {
		return fmt.Errorf("registry: writing %s: %w", branch.Name, err)
	}
	err = s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).Put([]byte(branch.Name), encoded)
	})
	if err != nil {
		return fmt.Errorf("registry: writing %s: %w", branch.Name, err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, name string) error {
	if err := ValidateName(name); err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		branches := tx.Bucket(bucket)
		if branches.Get([]byte(name)) == nil {
			return fmt.Errorf("%w: %s", ErrNotFound, name)
		}
		return branches.Delete([]byte(name))
	})
}
