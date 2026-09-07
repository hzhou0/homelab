# neon-ctl — tenancy and authorization

Open-source Neon ships the storage layer and `compute_ctl` but deliberately not a control plane.
This is the part that decides who may create a database, what it is called, and who may reach it.

## What is Neon's and what is ours

The storage layer knows about **tenants** and **timelines**, and nothing else. A tenant is the unit
of isolation; a timeline is a write-ahead log lineage. The one rule that matters here is that a
timeline can only be branched inside its own tenant — the storage controller enforces it, and it is
what forces the shape of everything below.

**Projects, branches and endpoints are ours.** The words are Neon's, and their hosted product maps
them the same way, but nothing in the open-source code knows any of them. Likewise the JWT scopes
the storage layer checks are machine credentials for its own API; they say nothing about people and
are not reused here.

| Ours | Is | Because |
|---|---|---|
| project | exactly one tenant | forks cannot cross tenants, so a project is the largest thing a branch can move within |
| branch | one timeline | a name and a catalog put on something the storage layer already has |
| endpoint | one opaque label | see below |

Nothing is addressed by its name. A project's name is unique to its owner and a branch's to its
project, so in both cases the name identifies the thing only once you already know what contains it
— which is the one thing a URL or an SNI label cannot assume.

## Why a project is exactly one tenant

A tenant is not a quota or a billing unit here; it is the boundary a fork cannot cross. If a project
held more than one, some branches could not be forked from others inside the same project, which
would make the project a lie. If a tenant were shared between projects, one project's branches could
be forked into another's.

So the mapping is forced rather than chosen, and it is what makes the tenant-scoped credentials the
storage layer already issues line up with the thing people actually own.

## Why identifiers are opaque

The tighter of the two cases is the endpoint. A client connects to `<endpoint>.<suffix>` and the
proxy takes the endpoint out of the SNI name, where a TLS wildcard certificate matches **exactly one
label** — so the identifier cannot be split into a project part and a branch part, and the branch
name alone is not unique enough to be one.

Projects have the same problem one level up, for a duller reason: two owners may each have a project
called `app`, so a name in a path would not say which.

Hence an opaque id for both, generated once and never changed: a memorable phrase for people to
repeat, and a random suffix that does the actual work of being unique. The endpoint id is also what
every Kubernetes object for a branch is named after, since it is the only identifier that is both
unique and a legal label.

Two consequences worth stating. Renaming does not change a connection string or a URL, because
neither was ever derived from a name. And two branches called `main` in different projects are
different endpoints — which is the point of the whole arrangement.

## Identity

This service authenticates nobody. It is told who the caller is by the authenticating proxy in front
of it, in request headers, and that is the only way it ever learns.

**The identifier is the directory login name, which is also its stable id.** The directory has no
rename operation, so a name cannot move between people while an account lives. An account destroyed
and remade under the same name inherits what the old one owned; nothing here detects that, and the
directory not deleting accounts is what makes it safe.

Nothing about a person is stored. There is no user table, no session, nothing to keep in sync with
the directory. What is stored is which identifier owns a project, and the display name is only ever
shown — never compared, never persisted, and absent for anybody but the caller. A listing therefore
shows identifiers for other people's projects, which is accepted rather than solved: the alternative
is either a cache that goes stale or putting the directory in the request path.

**The deployment invariant this rests on:** nothing may reach this service except through the proxy.
A header is only worth what the path in front of it is worth, and there is no configuration here
that can compensate for a service exposed directly.

## Authorization

A project is reachable by its **owner** or by anyone in one of its **granted groups**. There is no
distinction between reading and writing, and no per-branch grant: a fork can read its ancestor
whatever the rules say, so pretending otherwise would be theatre.

**Access can only be given away by someone who has it.** A grant naming a group the caller is not in
is refused, because it would push a project into other people's listings without their consent —
the same act as naming somebody else the owner, and refused for the same reason. Granting is an
ownership decision, so a group member may use a project but not decide who else may.

Handing a project to a different person is reserved to the administrator, for a duller reason than
it looks: nothing here can check that an identifier belongs to anybody, and a mistyped one leaves
the project reachable by no one else.

One **administrator** is named by configuration and can see every project. It is a single identifier
rather than a group, because it exists to make the cluster's owner able to find and repair things,
not to model an organisation. Nothing is administered by committee here.

A project the caller cannot reach answers as though it does not exist. Whether a name is taken is
itself something they are not entitled to know, and a distinct refusal would leak exactly that.

## What is deliberately absent

- **Roles.** Owner, group member and administrator is the whole model. A read-only grant would not
  survive contact with forks.
- **Nested organisations.** A project is the top of the tree.
- **Quotas.** Nothing counts branches or storage per project. Whether that becomes necessary depends
  on whether anybody but the cluster's owner ever uses this.
- **A global namespace.** Names are unique to their owner and their project, never to the cluster.
- **Group membership.** Groups are the directory's to define. Nothing here can enumerate them or
  check that one exists — only that the caller is in it — so a grant naming a group nobody holds is
  accepted and simply never matches.
