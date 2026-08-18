# JAD Authentication Architecture

## Overview

The JAD platform uses **jwtlet** (a lightweight Rust-based RFC 8693 token exchange service) instead of Keycloak. Authentication is built on **Kubernetes ServiceAccount tokens** as the identity source, with **clearglass** enforcing scope-based access control at the gateway.

---

## Components

| Component | Role | Port |
|-----------|------|------|
| **jwtlet** | Token issuer — exchanges K8s SA tokens for participant-scoped JWTs | 8080 (exchange), 8081 (management) |
| **clearglass** | Gateway enforcer — validates tokens and checks route-level scopes | 8080 (forward-auth) |
| **Traefik** | API gateway — routes requests and delegates auth to clearglass | 80/443 |

---

## How Authentication Works

Instead of a username/password flow through an IdP, the platform uses Kubernetes ServiceAccount tokens as identity proof:

```
┌──────────────┐     ┌────────────┐     ┌──────────────────┐     ┌─────────────┐
│ K8s SA Token │────▶│   jwtlet   │────▶│ Participant JWT  │────▶│  EDC APIs   │
│ (identity)   │     │ (exchange) │     │ (with scopes)    │     │ (validates) │
└──────────────┘     └────────────┘     └──────────────────┘     └─────────────┘
```

1. **Identity = Kubernetes ServiceAccount** — each actor (`seed-jobs`, `cfm-agents`, `redline`, `controlplane`) has a K8s SA
2. **jwtlet has mappings** — maps `SA identity` → `participant context` + `allowed scopes`
3. **Exchange produces a JWT** — signed by jwtlet, trusted by all EDC services via JWKS

---

## jwtlet Configuration

jwtlet's configuration (`jwtlet-config` ConfigMap):

- `issuer = "http://jwtlet.edc-v.svc.cluster.local:8080"` — the `iss` claim in issued tokens
- `[token].audience = "edcv"` — required `audience` parameter for exchange requests
- `[token].client_audience = "https://kubernetes.default.svc.cluster.local"` — expected audience on incoming SA tokens
- `[service_accounts]` — static grants for management API access:
  - `system:serviceaccount:edc-v:cfm-agents` → `jwtlet:management:mappings:write`, `jwtlet:management:scope:write`, `jwtlet:management:read`
  - `system:serviceaccount:edc-v:seed-jobs` → same set

**SA Token Validation**: jwtlet validates incoming subject tokens by calling the Kubernetes TokenReview API.

**Token Signing**: Uses a Vault-supplied signing key. The corresponding public key is served at `/.well-known/jwks.json`.

**EDC Trust Configuration** (all EDC services point to jwtlet):
```yaml
edc.iam.oauth2.issuer: "http://jwtlet.edc-v.svc.cluster.local:8080"
edc.iam.oauth2.jwks.url: "http://jwtlet.edc-v.svc.cluster.local:8080/.well-known/jwks.json"
```

---

## Token Exchange API

### Endpoint

```
POST http://jad.localhost/api/auth/token
Content-Type: application/x-www-form-urlencoded
```

(In-cluster: `http://jwtlet.edc-v.svc.cluster.local:8080/token`)

### Parameters

| Parameter | Value |
|-----------|-------|
| `grant_type` | `urn:ietf:params:oauth:grant-type:token-exchange` |
| `subject_token` | A Kubernetes SA token |
| `subject_token_type` | `urn:ietf:params:oauth:token-type:jwt` |
| `resource` | Participant context ID (e.g. `issuer`, `619e63dc...`) |
| `scope` | Scope tier to request (e.g. `write`, `cfm-read`) |
| `audience` | `edcv` |

### Exchange Flow

1. Pod reads projected SA token from `/var/run/secrets/jwtlet/token`
2. Pod sends exchange request to jwtlet `:8080/token`
3. jwtlet calls Kubernetes TokenReview to validate the SA token
4. jwtlet looks up the mapping for `clientIdentifier` + requested `resource`
5. jwtlet verifies requested scopes are allowed for this mapping
6. jwtlet expands scope tiers to concrete scope claims
7. jwtlet issues a signed JWT with `sub`, `aud`, `scope`, `act` claims
8. Caller uses the JWT as `Authorization: Bearer <token>` for API calls

---

## Scope Model

Scopes are requested as tier names and expanded by jwtlet:

| Requested `scope` | Expands to |
|---|---|
| `write` | `identity-api:write`, `management-api:write`, `issuer-admin-api:write` |
| `read` | `identity-api:read`, `management-api:read`, `issuer-admin-api:read`, `siglet-api:read` |
| `cfm-write` | `provision-manager-api:write`, `tenant-manager-api:write` |
| `cfm-read` | `provision-manager-api:read`, `tenant-manager-api:read` |

**Scope implication**: `admin ⊇ write ⊇ read` — having `write` satisfies a `read` check.

---

## Auth Flow: Issuer Participant (seed-jobs SA)

The `seed-jobs` ServiceAccount has a pre-existing jwtlet mapping (created by the platform's `jwtlet-seed` hook) binding it to the `issuer` participant context.

```
seed-jobs SA ─── kubectl create token ───▶ K8s SA JWT
                                                │
                                                ▼
                              POST jwtlet:8080/token
                              resource=issuer
                              scope=write
                              audience=edcv
                                                │
                                                ▼
                           ┌──────────────────────────────────────┐
                           │ JWT: sub=issuer                       │
                           │ scope=identity-api:write              │
                           │       management-api:write            │
                           │       issuer-admin-api:write          │
                           └──────────────────────────────────────┘
                                                │
                                                ▼
                        Call IssuerService / ControlPlane / IdentityHub
```

### Example (CLI)

```bash
SA_TOKEN=$(kubectl create token seed-jobs -n edc-v \
  --audience='https://kubernetes.default.svc.cluster.local' --duration=60m)

curl -s -X POST "http://jad.localhost/api/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:token-exchange" \
  --data-urlencode "subject_token=${SA_TOKEN}" \
  --data-urlencode "subject_token_type=urn:ietf:params:oauth:token-type:jwt" \
  --data-urlencode "resource=issuer" \
  --data-urlencode "scope=write" \
  --data-urlencode "audience=edcv"
```

---

## Auth Flow: New Participant (After CFM Provisioning)

### Step 1 — Mapping Creation (During Provisioning)

When CFM provisions a new participant, the **jwtlet-agent** creates mappings via jwtlet's management API:

```
POST jwtlet:8081/api/v1/mappings
Authorization: Bearer <cfm-agents SA token>
{
  "clientIdentifier": "system:serviceaccount:edc-v:controlplane",
  "participantContext": "<new-participant-context-id>",
  "scopes": ["read", "write", ...],
  "audiences": ["edcv"]
}
```

Mappings are created for multiple SAs (`controlplane`, `identityhub`, `siglet-sa`, `cfm-agents`) → new participant context.

### Step 2 — Token Exchange

```bash
# Use cfm-agents SA to get a token for the new participant
SA_TOKEN=$(kubectl create token cfm-agents -n edc-v \
  --audience='https://kubernetes.default.svc.cluster.local' --duration=60m)

curl -s -X POST "http://jad.localhost/api/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:token-exchange" \
  --data-urlencode "subject_token=${SA_TOKEN}" \
  --data-urlencode "subject_token_type=urn:ietf:params:oauth:token-type:jwt" \
  --data-urlencode "resource=<participant-context-id>" \
  --data-urlencode "scope=read" \
  --data-urlencode "audience=edcv"
```

### Resulting JWT

```json
{
  "sub": "<participant-context-id>",
  "iss": "http://jwtlet.edc-v.svc.cluster.local:8080",
  "aud": "edcv",
  "act": {
    "iss": "https://kubernetes.default.svc.cluster.local",
    "sub": "system:serviceaccount:edc-v:cfm-agents"
  },
  "scope": "identity-api:read management-api:read issuer-admin-api:read siglet-api:read"
}
```

---

## Gateway (External) Access — clearglass Enforcement

When calling through the Traefik gateway (`http://jad.localhost/api/...`):

```
Client ──Bearer JWT──▶ Traefik ──ForwardAuth──▶ clearglass ──▶ Backend
                                                     │
                                    1. Validates JWT signature (jwtlet JWKS)
                                    2. Checks scope vs route-map rules
                                    3. Returns 200 (pass) or 401/403 (deny)
```

### clearglass Route Map

clearglass enforces scope requirements per route pattern:

| Route Pattern | Required Scope |
|-------|---------------|
| `GET /api/v1alpha1/cells/**` | `tenant-manager-api:read` |
| `POST/DELETE /api/v1alpha1/cells/**` | `tenant-manager-api:write` |
| `GET /api/v1alpha1/tenants/**` | `tenant-manager-api:read` |
| `POST/DELETE /api/v1alpha1/tenants/**` | `tenant-manager-api:write` |
| `GET /api/mgmt/**/assets/**` | `management-api:assets:read` |
| `POST/PUT/DELETE /api/mgmt/**/assets/**` | `management-api:assets:write` |
| `GET /api/admin/**/credentialdefinitions/**` | `issuer-admin-api:credentialdefinitions:read` |
| `POST/DELETE /api/admin/**/credentialdefinitions/**` | `issuer-admin-api:credentialdefinitions:write` |
| `GET /api/identity/**` | `identity-api:read` |
| `POST/PUT/DELETE /api/identity/**` | `identity-api:write` |

Default policy: **deny** (no matching rule = rejected).

---

## SA-to-Participant Mapping

| ServiceAccount | `resource` param | Role |
|---|---|---|
| `seed-jobs` | `issuer` | Platform seeding / admin |
| `redline` | `redline` | Dataspace management UI |
| `cfm-agents` | `<any provisioned participant>` | CFM internal operations |
| `controlplane` | `<any provisioned participant>` | Runtime token exchange (Vault access) |

---

## API Route Table

| Service | External Path | Rewritten Backend Path | Middleware |
|---------|--------------|------------------------|------------|
| Control Plane | `/api/management` | `/api/mgmt` | `jwt-auth` |
| Identity Hub | `/api/identity` | `/api/identity/v1beta` | `jwt-auth` |
| Issuer Service (admin) | `/api/issuer/admin` | `/api/admin/v1beta` | `jwt-auth` |
| Issuer Service (issuance) | `/api/issuance` | `/api/issuance` | `jwt-auth` |
| Tenant Manager | `/api/tm` | `/api/v1alpha1` | `jwt-auth` |
| Provision Manager | `/api/pm` | `/api/v1alpha1` | `jwt-auth` |
| jwtlet (token exchange) | `/api/auth` | `/` | none |
| Dataplane (public) | `/api/dp/public` | `/` | none |

---

## Complete Auth Chain — External Request

For a client calling `GET http://jad.localhost/api/tm/cells`:

1. **Traefik** receives the request on the `edcv-gateway`
2. **HTTPRoute** matches path prefix `/api/tm` → attaches `jwt-auth` middleware
3. **ForwardAuth** sends the request headers to **clearglass** `/validate`
4. **clearglass** validates the JWT signature against jwtlet's JWKS
5. **clearglass** evaluates the rewritten path (`/api/v1alpha1/cells`) + method (GET) against the route map → requires `tenant-manager-api:read`
6. **clearglass** checks if the token's `scope` claim satisfies the requirement
7. If passed → request forwarded to the **Tenant Manager** backend
8. **Tenant Manager** independently verifies the token (`iss`, `aud`, `sub`)
9. Response returned to client

## Complete Auth Chain — Internal/Machine Request

For an in-cluster workload (e.g., a seed job calling IssuerService):

1. **Pod** reads projected SA token from `/var/run/secrets/jwtlet/token`
2. **Pod** exchanges it at **jwtlet** `:8080/token` → gets participant-scoped JWT
3. **Pod** calls EDC service **directly** (in-cluster, bypassing gateway/clearglass)
4. **EDC service** verifies the token against jwtlet's JWKS, checks `iss`, `aud`, `sub`, `scope`
5. Response returned

---

## Why No Keycloak?

| Aspect | Keycloak | jwtlet |
|--------|----------|--------|
| Identity source | Username/password, OIDC federation | Kubernetes ServiceAccount tokens |
| Secret management | Client secrets, refresh tokens | SA tokens are projected and auto-refreshed by K8s |
| Deployment | Separate stateful service + DB | Lightweight container, uses platform's PostgreSQL |
| Latency | External IdP round-trip | In-cluster, sub-millisecond |
| Participant scoping | Realm roles, client scopes | Mapping table (SA → participant + scopes) |
| Human access | Browser login flow | `kubectl create token` + exchange via `/api/auth/token` |

The platform uses K8s SA tokens as the identity provider directly. Identity is built into the cluster, no external IdP to manage, and per-participant scoping is handled by jwtlet's mapping table (PostgreSQL-backed).

---

## Quick Reference — Getting Tokens

```bash
# Issuer (admin) — read+write
SA_TOKEN=$(kubectl create token seed-jobs -n edc-v \
  --audience='https://kubernetes.default.svc.cluster.local' --duration=60m)
TOKEN=$(curl -s -X POST "http://jad.localhost/api/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:token-exchange" \
  --data-urlencode "subject_token=${SA_TOKEN}" \
  --data-urlencode "subject_token_type=urn:ietf:params:oauth:token-type:jwt" \
  --data-urlencode "resource=issuer" \
  --data-urlencode "scope=write" \
  --data-urlencode "audience=edcv" | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

# Issuer — CFM read (tenant-manager + provision-manager)
# Same as above but with scope=cfm-read

# New participant — read
SA_TOKEN=$(kubectl create token cfm-agents -n edc-v \
  --audience='https://kubernetes.default.svc.cluster.local' --duration=60m)
TOKEN=$(curl -s -X POST "http://jad.localhost/api/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:token-exchange" \
  --data-urlencode "subject_token=${SA_TOKEN}" \
  --data-urlencode "subject_token_type=urn:ietf:params:oauth:token-type:jwt" \
  --data-urlencode "resource=<participant-context-id>" \
  --data-urlencode "scope=read" \
  --data-urlencode "audience=edcv" | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
```

Tokens are valid for **1 hour**.
