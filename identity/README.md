# identity

One login for everything published on a shared Gateway. Every route carries an `ExternalAuth` filter
that asks Authelia whether the request may proceed, and the application behind it receives the
decided identity in request headers. The routes, the filter and the namespace belong to the `cilium`
chart; install it first.

## Install

Generate the issuer signing key and wrap it as a configuration file:

```sh
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out issuer.pem
{ printf 'identity_providers:\n  oidc:\n    jwks:\n      - key: |\n'
  sed 's/^/          /' issuer.pem; } > oidc.yml
```

Create the Secret and install. The last two entries belong to OpenID Connect; the three `smtp*` ones
to the notifier, where the sender and the username are the same account. Delete `issuer.pem` and
`oidc.yml` afterwards — the Secret is the copy that matters.

```sh
kubectl -n identity create secret generic identity-credentials \
  --from-literal=lldapJwtSecret=...     --from-literal=lldapKeySeed=... \
  --from-literal=lldapAdminPassword=... --from-literal=autheliaSessionSecret=... \
  --from-literal=autheliaStorageEncryptionKey=... \
  --from-literal=smtpUsername='you@gmail.com' \
  --from-literal=smtpSender='Authelia <you@gmail.com>' \
  --from-literal=smtpPassword='<16-character app password, no spaces>' \
  --from-literal=autheliaOidcHmacSecret=... \
  --from-file=oidc.yml

helm install identity identity -n identity
helm upgrade identity identity -n identity
```

lldap creates its administrator at first start. Sign the portal into that account and create
everyone else from it; there is no signup, and a password is the whole of what the directory holds
about a person. Groups are directory data, created by hand.

## Publishing a route

Add the hostname to an access control rule in the same change that publishes the route in the
`cilium` chart. A route named nowhere reaches nobody.

## Adding an OpenID Connect client

Generate a secret and its digest with the image itself:

```sh
docker run --rm --network none ghcr.io/authelia/authelia:4.39.22 \
  authelia crypto hash generate pbkdf2 --variant sha512 --random
```

The digest goes in `values.yaml`, the plaintext wherever the client runs. The route still needs its
own rule: the rules gate the route, the client governs the token exchange.

## The Kubernetes API as a client

The API server holds no session and validates a token the caller already presents, so it accepts
this issuer's tokens once told to. That is a k3s server change and no chart makes it.

```yaml
# /etc/rancher/k3s/authn.yaml
apiVersion: apiserver.config.k8s.io/v1
kind: AuthenticationConfiguration
jwt:
  - issuer:
      url: https://<the portal>
      audiences: ['<the client>']
    claimValidationRules:
      - expression: 'has(claims.groups) && ("admins" in dyn(claims.groups))'
    claimMappings:
      username:
        claim: preferred_username
        prefix: 'oidc:'
      groups:
        claim: groups
        prefix: 'oidc:'
```

```yaml
# /etc/rancher/k3s/config.yaml.d/oidc.yaml
kube-apiserver-arg:
  - "authentication-config=/etc/rancher/k3s/authn.yaml"
```

`rc-service k3s restart` picks up the drop-in. The API server validates `authn.yaml` at startup and
exits on a file it cannot parse, taking the control plane with it; every claim reaches CEL typed
`any`, so an expression over one needs `dyn()`. After a successful start it watches the file and
reloads it on write, and a bad write leaves the running configuration in place.

The prefixes keep directory names clear of the ones Kubernetes already knows, and RBAC binds to the
prefixed group. The validation rule decides authentication, so an account outside the group holds no
token the API server accepts.

Two things here decide whether that file works. The portal's route is unauthenticated, which is what
lets the API server fetch discovery and keys directly. And a client whose groups decide access needs
a claims policy placing them in the ID token — Authelia serves the rest from userinfo, which the API
server never calls.

`authn.yaml` is node-local and a rebuilt server comes back without it. Signing in still succeeds
then, and every call the application makes is refused.

## Operating

Both volumes are backed up from outside the cluster, so recovering them never depends on the object
stack an operator has to sign in to reach.

The filter fails closed: a broken policy denies every published route at once. One directory account
holds administrative access everywhere, and the Kubernetes API sits outside this path.
