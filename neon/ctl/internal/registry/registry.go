// Package registry holds what a branch is and how branches are stored. It is the only state this
// service owns that cannot be rebuilt from object storage.
package registry

import (
	"bytes"
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
	ErrNameTaken   = errors.New("registry: branch name already used in this project")
	// Never expected: an endpoint id carries enough entropy that a repeat is not a real outcome.
	// It is checked anyway because the alternative is silently overwriting somebody else's branch.
	ErrEndpointTaken = errors.New("registry: endpoint id already in use")
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
	Name      string `json:"name"`
	ProjectID string `json:"project_id"`

	// What the proxy resolves out of the SNI name. Opaque and immutable: renaming a branch must
	// not invalidate a connection string, and a name is only unique inside its project anyway.
	EndpointID string `json:"endpoint_id"`

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

	LastSeen *LastSeen `json:"last_seen,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Role holds a Postgres SCRAM verifier rather than a password. The same string authenticates at
// the proxy and provisions the role on the compute, and no plaintext is ever persisted.
type Role struct {
	Name     string `json:"name"`
	Verifier string `json:"verifier"`

	// Secret is the password the verifier was derived from, sealed. Empty is the normal state:
	// nothing here needs it, and it exists only so a person can be shown it more than once.
	Secret string `json:"secret,omitempty"`
}

// LastSeen is the names the compute had when it was last shut down. It is not a catalog and is
// never shown as one: a page with nothing to ask still has to offer a connection string, and a
// connection is what starts the compute back up.
type LastSeen struct {
	At        time.Time `json:"at"`
	Roles     []string  `json:"roles"`
	Databases []string  `json:"databases"`
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

	// Sealed elsewhere: this package holds no key and never reads what it stores here.
	Secret string
}

// New creates a root branch inside a project. The Postgres version is a fact the caller holds and
// this package cannot invent; a branch recording a version its image does not run would not start.
func New(spec Spec, pgVersion int, project *Project) (*Branch, error) {
	if project == nil {
		return nil, errors.New("registry: a branch needs a project")
	}
	if pgVersion == 0 {
		return nil, errors.New("registry: a root branch needs the compute's postgres version")
	}
	return build(spec, pgVersion, project, nil)
}

// Fork derives a child. A timeline can only be branched inside its own tenant, and the catalog
// comes with the timeline, so both are inherited unless the spec says otherwise.
func (b *Branch) Fork(spec Spec, project *Project) (*Branch, error) {
	if project == nil || project.ID != b.ProjectID {
		return nil, errors.New("registry: a fork belongs to its ancestor's project")
	}
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
	return build(spec, b.PgVersion, project, b)
}

func build(spec Spec, pgVersion int, project *Project, parent *Branch) (*Branch, error) {
	if err := ValidateName(spec.Name); err != nil {
		return nil, err
	}

	endpoint, err := NewEndpointID()
	if err != nil {
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
		ProjectID:  project.ID,
		EndpointID: endpoint,
		TenantID:   project.TenantID,
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
	if err := ValidateName(b.ProjectID); err != nil {
		return err
	}
	if err := ValidateName(b.EndpointID); err != nil {
		return err
	}
	if b.TenantID.IsZero() {
		return errors.New("registry: a branch needs a tenant")
	}
	if len(b.Roles) == 0 {
		return errors.New("registry: a branch needs at least one role to be reachable")
	}
	// A name repeated here reaches the compute as two entries for one object, which it cannot
	// provision and reports as a failure to start.
	owners := map[string]bool{}
	for _, role := range b.Roles {
		if owners[role.Name] {
			return fmt.Errorf("registry: role %q is named twice", role.Name)
		}
		owners[role.Name] = true
	}
	named := map[string]bool{}
	for _, database := range b.Databases {
		if named[database.Name] {
			return fmt.Errorf("registry: database %q is named twice", database.Name)
		}
		named[database.Name] = true
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
		specs = append(specs, RoleSpec{Name: role.Name, Verifier: role.Verifier, Secret: role.Secret})
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
			roles = append(roles, Role{Name: spec.Name, Verifier: spec.Verifier, Secret: spec.Secret})
		case spec.Password != "":
			verifier, err := scram.Verifier(spec.Password)
			if err != nil {
				return nil, err
			}
			roles = append(roles, Role{Name: spec.Name, Verifier: verifier, Secret: spec.Secret})
		default:
			return nil, fmt.Errorf("registry: role %q needs a password or a verifier", spec.Name)
		}
	}
	return roles, nil
}

// Store holds projects and branches. Branches are keyed by endpoint id rather than by name,
// because a name is only unique inside its project and the proxy resolves the endpoint alone.
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
		for _, name := range [][]byte{bucket, metaBucket, projectBucket, projectNameBucket, endpointBucket} {
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

// Endpoint is the proxy's lookup: an SNI label and nothing else.
func (s *Store) Endpoint(ctx context.Context, endpoint string) (*Branch, error) {
	if err := ValidateName(endpoint); err != nil {
		return nil, err
	}
	var branch *Branch
	err := s.db.View(func(tx *bbolt.Tx) error {
		decoded, err := readBranch(tx, endpoint)
		branch = decoded
		return err
	})
	return branch, err
}

func readBranch(tx *bbolt.Tx, endpoint string) (*Branch, error) {
	stored := tx.Bucket(bucket).Get([]byte(endpoint))
	if stored == nil {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, endpoint)
	}
	var branch Branch
	if err := json.Unmarshal(stored, &branch); err != nil {
		return nil, fmt.Errorf("registry: branch %s: %w", endpoint, err)
	}
	return &branch, nil
}

func (s *Store) Branch(ctx context.Context, projectID, name string) (*Branch, error) {
	if err := ValidateName(projectID); err != nil {
		return nil, err
	}
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	var branch *Branch
	err := s.db.View(func(tx *bbolt.Tx) error {
		endpoint := tx.Bucket(endpointBucket).Get(endpointKey(projectID, name))
		if endpoint == nil {
			return fmt.Errorf("%w: %s/%s", ErrNotFound, projectID, name)
		}
		decoded, err := readBranch(tx, string(endpoint))
		branch = decoded
		return err
	})
	return branch, err
}

// Branches walks the index rather than the branches themselves, so a project's members come back
// ordered by name and no other project's rows are touched.
func (s *Store) Branches(ctx context.Context, projectID string) ([]Branch, error) {
	if err := ValidateName(projectID); err != nil {
		return nil, err
	}
	var branches []Branch
	err := s.db.View(func(tx *bbolt.Tx) error {
		prefix := endpointKey(projectID, "")
		cursor := tx.Bucket(endpointBucket).Cursor()
		for key, endpoint := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, endpoint = cursor.Next() {
			branch, err := readBranch(tx, string(endpoint))
			if err != nil {
				return err
			}
			branches = append(branches, *branch)
		}
		return nil
	})
	return branches, err
}

// AllBranches is for work that spans projects, such as suspending idle computes.
func (s *Store) AllBranches(ctx context.Context) ([]Branch, error) {
	var branches []Branch
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucket).ForEach(func(key, stored []byte) error {
			var branch Branch
			if err := json.Unmarshal(stored, &branch); err != nil {
				return fmt.Errorf("registry: branch %s: %w", key, err)
			}
			branches = append(branches, branch)
			return nil
		})
	})
	return branches, err
}

// Create refuses to overwrite. Both checks and the write share one transaction, so two callers
// racing to claim the same name cannot both succeed.
func (s *Store) Create(ctx context.Context, branch *Branch) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(bucket).Get([]byte(branch.EndpointID)) != nil {
			return fmt.Errorf("%w: %s", ErrEndpointTaken, branch.EndpointID)
		}
		if tx.Bucket(endpointBucket).Get(endpointKey(branch.ProjectID, branch.Name)) != nil {
			return fmt.Errorf("%w: %s/%s", ErrNameTaken, branch.ProjectID, branch.Name)
		}
		return putBranch(tx, branch)
	})
}

// Put overwrites, and is for a branch that already exists.
func (s *Store) Put(ctx context.Context, branch *Branch) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return putBranch(tx, branch)
	})
}

func putBranch(tx *bbolt.Tx, branch *Branch) error {
	encoded, err := json.Marshal(branch)
	if err != nil {
		return fmt.Errorf("registry: writing %s: %w", branch.Name, err)
	}
	if err := tx.Bucket(bucket).Put([]byte(branch.EndpointID), encoded); err != nil {
		return fmt.Errorf("registry: writing %s: %w", branch.Name, err)
	}
	return tx.Bucket(endpointBucket).Put(endpointKey(branch.ProjectID, branch.Name), []byte(branch.EndpointID))
}

func (s *Store) Delete(ctx context.Context, projectID, name string) error {
	if err := ValidateName(projectID); err != nil {
		return err
	}
	if err := ValidateName(name); err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		index := tx.Bucket(endpointBucket)
		key := endpointKey(projectID, name)
		endpoint := index.Get(key)
		if endpoint == nil {
			return fmt.Errorf("%w: %s/%s", ErrNotFound, projectID, name)
		}
		if err := tx.Bucket(bucket).Delete(endpoint); err != nil {
			return err
		}
		return index.Delete(key)
	})
}
