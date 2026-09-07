package registry

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	petname "github.com/dustinkirkland/golang-petname"
	"go.etcd.io/bbolt"

	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
)

var ErrProjectNotFound = errors.New("registry: project not found")

// A Project owns exactly one Neon tenant. The storage layer can only branch a timeline inside its
// own tenant, so the tenant is the real isolation boundary and a project is the name and the
// ownership put on it.
type Project struct {
	// Opaque and permanent, for the same reason a branch has one: a name is only unique to its
	// owner, so the name cannot be what addresses it.
	ID string `json:"id"`

	Name     string        `json:"name"`
	TenantID neon.TenantID `json:"tenant_id"`

	// Owner is a stable directory identifier; Groups are group names. Either grants full access to
	// the project — there is no per-branch grant, because a fork can read its ancestor regardless.
	Owner  string   `json:"owner"`
	Groups []string `json:"groups,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func NewProject(name, owner string, groups []string, tenant neon.TenantID) (*Project, error) {
	if err := ValidateName(name); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("registry: a project needs an owner")
	}
	if tenant.IsZero() {
		return nil, errors.New("registry: a project needs a tenant")
	}
	id, err := NewProjectID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	return &Project{
		ID:        id,
		Name:      name,
		TenantID:  tenant,
		Owner:     owner,
		Groups:    groups,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// Permits answers whether an identity may use the project at all. Membership is not hierarchical:
// an admin override belongs to the caller, not here.
func (p *Project) Permits(user string, groups []string) bool {
	if user != "" && user == p.Owner {
		return true
	}
	return slices.ContainsFunc(p.Groups, func(granted string) bool {
		return slices.Contains(groups, granted)
	})
}

// Names are only unique to the thing that contains them, so neither a project nor a branch can be
// addressed by its name. Both get an opaque id instead: a phrase to be recognised and repeated out
// loud, and a random suffix that does the actual work of being unique.
//
// An endpoint id has the tighter constraint of the two — the proxy reads it out of an SNI name
// under a wildcard matching exactly one label — so both are built to satisfy that.
const (
	projectPrefix  = "pr-"
	endpointPrefix = "ep-"
)

// Five bytes encode to exactly eight characters, so the id never carries base32 padding, which is
// legal in neither a DNS label nor a Kubernetes object name.
var idAlphabet = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

func NewProjectID() (string, error) { return newID(projectPrefix) }

func NewEndpointID() (string, error) { return newID(endpointPrefix) }

func newID(prefix string) (string, error) {
	raw := make([]byte, 5)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("registry: generating an id: %w", err)
	}
	return fmt.Sprintf("%s%s-%s-%s", prefix,
		petname.Adjective(), petname.Name(), idAlphabet.EncodeToString(raw)), nil
}

var (
	projectBucket = []byte("projects")
	// Enforces that a name is unique to its owner, and nothing wider.
	projectNameBucket = []byte("project-names")
	endpointBucket    = []byte("endpoints")
)

// Index keys join two values that cannot themselves contain the separator, so they never have to
// be escaped or parsed back apart.
func endpointKey(projectID, branch string) []byte {
	return []byte(projectID + "\x00" + branch)
}

func projectNameKey(owner, name string) []byte {
	return []byte(owner + "\x00" + name)
}

func (s *Store) Project(ctx context.Context, id string) (*Project, error) {
	if err := ValidateName(id); err != nil {
		return nil, err
	}
	var project *Project
	err := s.db.View(func(tx *bbolt.Tx) error {
		decoded, err := readProject(tx, id)
		project = decoded
		return err
	})
	return project, err
}

func readProject(tx *bbolt.Tx, id string) (*Project, error) {
	stored := tx.Bucket(projectBucket).Get([]byte(id))
	if stored == nil {
		return nil, fmt.Errorf("%w: %s", ErrProjectNotFound, id)
	}
	var project Project
	if err := json.Unmarshal(stored, &project); err != nil {
		return nil, fmt.Errorf("registry: project %s: %w", id, err)
	}
	return &project, nil
}

func (s *Store) Projects(ctx context.Context) ([]Project, error) {
	var projects []Project
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(projectBucket).ForEach(func(_, stored []byte) error {
			var project Project
			if err := json.Unmarshal(stored, &project); err != nil {
				return fmt.Errorf("registry: %w", err)
			}
			projects = append(projects, project)
			return nil
		})
	})
	return projects, err
}

// CreateProject refuses a name its owner already uses. Two owners may each have one.
func (s *Store) CreateProject(ctx context.Context, project *Project) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(projectNameBucket).Get(projectNameKey(project.Owner, project.Name)) != nil {
			return fmt.Errorf("%w: %s", ErrNameTaken, project.Name)
		}
		return putProject(tx, project)
	})
}

// PutProject overwrites, and is for a project that already exists. A rename or a change of owner
// moves the name index with it, so the old key never lingers to block a reuse.
func (s *Store) PutProject(ctx context.Context, project *Project) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		previous, err := readProject(tx, project.ID)
		if err != nil {
			return err
		}
		if previous.Owner != project.Owner || previous.Name != project.Name {
			if tx.Bucket(projectNameBucket).Get(projectNameKey(project.Owner, project.Name)) != nil {
				return fmt.Errorf("%w: %s", ErrNameTaken, project.Name)
			}
			if err := tx.Bucket(projectNameBucket).Delete(projectNameKey(previous.Owner, previous.Name)); err != nil {
				return err
			}
		}
		return putProject(tx, project)
	})
}

func putProject(tx *bbolt.Tx, project *Project) error {
	encoded, err := json.Marshal(project)
	if err != nil {
		return fmt.Errorf("registry: writing project %s: %w", project.Name, err)
	}
	if err := tx.Bucket(projectBucket).Put([]byte(project.ID), encoded); err != nil {
		return err
	}
	return tx.Bucket(projectNameBucket).Put(projectNameKey(project.Owner, project.Name), []byte(project.ID))
}

// Deleting a project with branches still in it would orphan their timelines, which only the
// storage controller can then account for.
func (s *Store) DeleteProject(ctx context.Context, id string) error {
	if err := ValidateName(id); err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		project, err := readProject(tx, id)
		if err != nil {
			return err
		}
		prefix := append([]byte(id), 0)
		cursor := tx.Bucket(endpointBucket).Cursor()
		if key, _ := cursor.Seek(prefix); key != nil && strings.HasPrefix(string(key), string(prefix)) {
			return fmt.Errorf("registry: project %s still has branches", project.Name)
		}
		if err := tx.Bucket(projectNameBucket).Delete(projectNameKey(project.Owner, project.Name)); err != nil {
			return err
		}
		return tx.Bucket(projectBucket).Delete([]byte(id))
	})
}
