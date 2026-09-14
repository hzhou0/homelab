# headlamp

Headlamp is a window onto the API server, not a principal in it. Its ServiceAccount holds no role;
every request carries the viewer's own ID token, so what a person sees is exactly what RBAC grants
their directory groups. Those grants are cluster-scoped and one of them is `cluster-admin`, which is
why this chart is foundational rather than something the operator agent deploys.

The API server must accept the issuer's tokens for every group bound here. That is a k3s server
change, described with the identity chart; without it signing in succeeds and every call is refused.

## The read role

A reader must not reach a credential, so the role names kinds rather than wildcarding them. A
wildcard carries Secrets and every exec, attach and proxy subresource — and a GET on the kubelet's
proxy is enough to exec. Pod logs are withheld too, since nothing stops a process printing a secret.

An API group is wildcarded only when none of its kinds can hold a credential inline; the rest are
enumerated without the kind that can. A group installed later stays invisible until someone checks
it. ConfigMaps are readable: a credential in one is a misconfiguration, not something this fences.

## Install

The client secret is the plaintext whose digest the identity chart holds.

```sh
kubectl create namespace headlamp
kubectl -n headlamp create secret generic headlamp-oidc --from-literal=clientSecret=...

helm install headlamp headlamp -n headlamp
helm upgrade headlamp headlamp -n headlamp
```

The route's namespace must exist before the cilium chart publishes it, since its ReferenceGrant is
created there.
