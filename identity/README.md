# identity

The cluster's front door. One login for everything published on a shared Gateway, with the Gateway
— not each application — deciding whether a request may proceed. Applications receive an
already-authenticated identity in request headers and never see a credential.

## Shape

Three workloads. **lldap** holds people and groups and nothing else. **Authelia** is the policy
decision point and the login portal: it answers the Gateway's authorization subrequest, and
publishes the portal a browser is redirected to. **Redis** holds sessions, so restarting or
rescheduling Authelia does not log the cluster out.

Authorization attaches through the Gateway API `ExternalAuth` filter, which Cilium translates into
an Envoy `ext_authz` filter. There is no listener-wide or Gateway-wide attachment in the Gateway
API, so every route carries its own copy — written once, in the chart that owns every route. This
chart publishes nothing and decides nothing about who reaches what; it answers the question.

## Install / upgrade

Install cilium first. Namespace is managed there along with grants to make it visible to all namespaces.

```sh
kubectl -n identity create secret generic identity-credentials \
  --from-literal=lldapJwtSecret=...     --from-literal=lldapKeySeed=... \
  --from-literal=lldapAdminPassword=... --from-literal=autheliaSessionSecret=... \
  --from-literal=autheliaStorageEncryptionKey=...

helm install identity identity -n identity
```

Nothing sensitive goes on the command line beyond the Secret; the rest is `values.yaml`.

## Bootstrap

lldap creates its own administrator at first start, so there is no window where the directory has
no one in it. That account is what the portal is first logged into, and what creates everyone else.

There is no signup: the administrator creates every account in the directory, and a person's
password is the whole of what the directory holds about them.

**A second factor is not available, and the default policy says so.** A factor is not directory
data — TOTP secrets and passkeys live in Authelia's own storage — and Authelia will not let one be
registered from a merely password-authenticated session, on the reasoning that whoever steals a
password could otherwise enrol their own and hold the account forever. Its way out is a one-time
code sent to the person, which needs something that can send mail. Without that, nobody can obtain
a factor, so requiring one would make every route unreachable rather than more secure.

Configuring a sender is what lifts this, and the default policy is then the only line that changes.
Until then the password and the network fence in front of the gateway are the boundary, and a
single route can still be raised on its own.

## The parts that are load-bearing

**Strategy order.** Only one authentication strategy answers a request that authenticated nowhere,
and it is the last one configured. The header strategy answers with a 401 challenge; the cookie
strategy declines to answer at all, which is what lets the redirect to the portal happen. Header
first, cookie last is therefore the only order that gives a browser a login page while still
letting a script present `Authorization` and get a challenge rather than a redirect.

**One cookie domain, wider than the gateway.** A session is a person, not a route into the cluster,
so the cookie is scoped to the whole zone rather than to the names one gateway serves. That leaves
room to publish a second gateway later without reissuing anybody's session, and it is only safe
because every name in the zone is this cluster's — a browser sends that cookie to all of them. A
domain per gateway is not the alternative it looks like: Authelia refuses a pair where one contains
the other, and the internal zone sits inside the public one.

The portal may sit under a narrower zone than the cookie, since the cookie reaches down into it.
What it cannot do is answer a client that can only reach the wider one, so publishing a route on a
gateway the portal does not answer for is one decision with where the portal lives, not two.

**The response-header allow-list is a fence, not a convenience.** Left empty, the filter copies
every header the portal returned into the proxied request. Naming the identity headers explicitly
is what stops a header a client supplied from surviving a response that did not overwrite it. The
same list is why the client's scheme has to be forwarded deliberately: the subrequest arrives over
plain HTTP, and without it every post-login redirect is downgraded.

**Break-glass is an account, not a bypass.** The filter fails closed, so a broken policy takes the
dashboards with it. The answer is one directory account holding administrative access everywhere,
not a route left unprotected for an outage that may never come — and beneath that, the Kubernetes
API, which is not in this path at all.

**Redis is deliberately shallow.** No volume, no persistence, no Sentinel: its worst case is that
everyone logs in again, and machinery costing more to run than what it protects is worth is the
wrong trade. The other two datasets are node-local for the same reason — logging in must not depend
on the object-storage stack an operator would be trying to diagnose.

**Authelia is a single replica**, and not configurably so. Its own storage is SQLite on a
ReadWriteOnce volume, which a second replica would either fail to mount or corrupt; running more is
a decision to move storage to a real database, which is a change to this chart rather than a value.
