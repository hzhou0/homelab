package controlplane

import (
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
)

var errUnauthenticated = errors.New("the request carries no identity")

// Identity is who the caller is, as decided before the request arrived here. This service never
// authenticates anyone: it is told, by a proxy that did.
type Identity struct {
	// User is the directory login name, which the directory has no way to rename. An account
	// destroyed and remade under the same name inherits what the old one owned.
	User string

	// Display is the human-readable name, held only to be shown. Nothing is ever decided from it.
	Display string

	Groups []string
	Admin  bool
}

// IdentityOptions names the headers the proxy in front sets. Nothing here can compensate for this
// service being reachable without that proxy: a header is worth what the path in front of it is.
type IdentityOptions struct {
	UserHeader    string
	DisplayHeader string
	GroupsHeader  string

	// Admin is one identifier, not a group: it exists so the cluster's owner can find and repair
	// things, not to model an organisation. Empty means nobody holds it.
	Admin string
}

func (s *Server) identify(r *http.Request) (Identity, error) {
	user := strings.TrimSpace(r.Header.Get(s.identity.UserHeader))
	if user == "" {
		return Identity{}, errUnauthenticated
	}
	identity := Identity{
		User:    user,
		Display: strings.TrimSpace(r.Header.Get(s.identity.DisplayHeader)),
		Groups:  splitGroups(r.Header.Get(s.identity.GroupsHeader)),
		Admin:   s.identity.Admin != "" && user == s.identity.Admin,
	}
	if identity.Display == "" {
		identity.Display = user
	}
	return identity, nil
}

func splitGroups(header string) []string {
	if header == "" {
		return nil
	}
	parts := strings.Split(header, ",")
	groups := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			groups = append(groups, trimmed)
		}
	}
	return groups
}

// caller resolves the identity or answers the request. A caller with no identity is refused
// rather than treated as anonymous, because every route below this needs an owner.
func (s *Server) caller(w http.ResponseWriter, r *http.Request) *Identity {
	return s.identityOr(r, apiRefusal(w))
}

// scope resolves both halves of a project-scoped request. Every route below /api/projects/{project}
// needs the pair, and neither is usable without the other.
func (s *Server) scope(w http.ResponseWriter, r *http.Request) (*Identity, *registry.Project) {
	refuse := apiRefusal(w)
	identity := s.identityOr(r, refuse)
	if identity == nil {
		return nil, nil
	}
	project := s.projectOr(r, identity, refuse)
	if project == nil {
		return nil, nil
	}
	return identity, project
}

// How a refusal is written is the caller's to decide: the API answers in JSON and the page cannot.
func apiRefusal(w http.ResponseWriter) func(int, string) {
	return func(status int, message string) { writeError(w, status, message) }
}

func (s *Server) identityOr(r *http.Request, refuse func(int, string)) *Identity {
	identity, err := s.identify(r)
	if err != nil {
		refuse(http.StatusUnauthorized, "unauthenticated")
		return nil
	}
	return &identity
}

// A project the caller may not see answers 404 rather than 403: whether it exists is itself
// something they are not entitled to know.
func (s *Server) projectOr(r *http.Request, identity *Identity, refuse func(int, string)) *registry.Project {
	id := r.PathValue("project")
	project, err := s.registry.Project(r.Context(), id)
	switch {
	case err == nil:
	case errors.Is(err, registry.ErrProjectNotFound), errors.Is(err, registry.ErrInvalidName):
		refuse(http.StatusNotFound, "project not found")
		return nil
	default:
		s.log.Error("reading project", "project", id, "error", err)
		refuse(http.StatusServiceUnavailable, "registry unavailable")
		return nil
	}
	if !identity.permits(project) {
		refuse(http.StatusNotFound, "project not found")
		return nil
	}
	return project
}

func (i *Identity) permits(project *registry.Project) bool {
	return i.Admin || project.Permits(i.User, i.Groups)
}

// Access may only be given away by someone who has it. Granting a group you are not in pushes a
// project into other people's listings without their consent, which is the same act as naming
// somebody else the owner and is refused for the same reason.
func (i *Identity) mayGrant(groups []string) bool {
	return i.Admin || !slices.ContainsFunc(groups, func(group string) bool {
		return !slices.Contains(i.Groups, group)
	})
}
