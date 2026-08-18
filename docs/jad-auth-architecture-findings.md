# JAD Project (eclipse-dataspace-hub/jad) — Technical Knowledge Base

## Session Summary
This document captures the key technical findings from a development session focused on deploying and understanding the JAD (Joint Autonomous Dataspace) platform's authentication architecture, participant onboarding, and integration patterns with `cr-bff`.

---

## 1. Authentication Architecture

### Overview
The JAD platform uses **jwtlet** — a lightweight Rust-based RFC 8693 token exchange service (`ghcr.io/eclipse-cfm/jwtlet`) — instead of Keycloak. Authentication is built on **Kubernetes ServiceAccount tokens** as the identity source, with **clearglass** enforcing scope-based access control at the gateway.

### Components

| Component | Role | Ports |
|-----------|------|-------|
| **jwtlet** | Token issuer — exchanges K8s SA tokens for participant-scoped JWTs | 8080 (token exchange), 8081 (management API) |
| **clearglass** | Gateway enforcer — validates tokens and checks route-level scopes | 8080 (forward-auth) |
| **Traefik** | API gateway — routes requests and delegates auth to clearglass via ForwardAuth | 80/443 |

### jwtlet Configuration (`jwtlet-config` ConfigMap)

```toml
issuer = "http://jwtlet.edc-v.svc.cluster.local:8080"

[storage_backend]
type = "postgres"
url = "postgresql://jwtlet:jwtlet@core-platform-postgresql.edc-v.svc.cluster.local:5432/jwtlet"

[k8s]
api_server_url = "https://kubernetes.default.svc"
cluster_issuer = "https://kubernetes.default.svc.cluster.local"

[token]
client_audience = "https://kubernetes.default.svc.cluster.local"
audience = "edcv"

[service_accounts]
"system:serviceaccount:edc-v:cfm-agents" = ["jwtlet:management:mappings:write", "jwtlet:management:scope:write", "jwtlet:management:read"]
"system:serviceaccount:edc-v:seed-jobs" = ["jwtlet:management:mappings:write", "jwtlet:management:scope:write", "jwtlet:management:read"]
```

### Token Exchange Flow (RFC 8693)

```
K8s SA Token → jwtlet /token endpoint → Participant-scoped JWT
```

1. Pod reads projected SA token from `/var/run/secrets/jwtlet/token` (audience: `https://kubernetes.default.svc.cluster.local`, expiry: 3600s)
2. Sends exchange request to `POST http://jwtlet.edc-v.svc.cluster.local:8080/token`
3. jwtlet validates via Kubernetes TokenReview API → gets `system:serviceaccount:<ns>:<sa>`
4. Looks up mapping for that `clientIdentifier` + requested `resource` (participantContext)
5. Expands requested scope tiers to concrete claims
6. Issues signed JWT (Vault-supplied key) with `iss`, `sub`, `aud`, `scope`, `act` claims
7. EDC services verify via jwtlet's JWKS at `/.well-known/jwks.json`

### Exchange Request Parameters

| Parameter | Value |
|-----------|-------|
| `grant_type` | `urn:ietf:params:oauth:grant-type:token-exchange` |
| `subject_token` | Kubernetes SA token (projected with cluster-issuer audience) |
| `subject_token_type` | `urn:ietf:params:oauth:token-type:jwt` |
| `resource` | Participant context ID (becomes `sub` in issued token) |
| `scope` | Scope tier or narrow scope identifier |
| `audience` | `edcv` (must match jwtlet's `[token].audience`) |

### Scope Model

| Requested `scope` tier | Expands to |
|---|---|
| `write` | `identity-api:write`, `management-api:write`, `issuer-admin-api:write` |
| `read` | `identity-api:read`, `management-api:read`, `issuer-admin-api:read`, `siglet-api:read` |
| `cfm-write` | `provision-manager-api:write`, `tenant-manager-api:write` |
| `cfm-read` | `provision-manager-api:read`, `tenant-manager-api:read` |

Scope hierarchy: `admin ⊇ write ⊇ read`

### SA-to-Participant Mapping Table

| ServiceAccount | `resource` param | Role |
|---|---|---|
| `seed-jobs` | `issuer` | Platform seeding/admin |
| `redline` | `redline` | Dataspace management UI |
| `cfm-agents` | `<any provisioned participant>` | CFM internal operations |
| `controlplane` | `<any provisioned participant>` | Runtime Vault access per participant |

### Gateway Route Topology

| External Path | Backend Service | Rewrite To | Auth |
|---|---|---|---|
| `/api/management` | controlplane:8081 | `/api/mgmt` | `jwt-auth` |
| `/api/identity` | identityhub:7081 | `/api/identity/v1beta` | `jwt-auth` |
| `/api/issuer/admin` | issuerservice:10013 | `/api/admin/v1beta` | `jwt-auth` |
| `/api/tm` | tenant-manager:8080 | `/api/v1alpha1` | `jwt-auth` |
| `/api/pm` | provision-manager:8080 | `/api/v1alpha1` | `jwt-auth` |
| `/api/auth` | jwtlet:8080 | `/` | **none** |
| `/api/dp/public` | dataplane:11002 | `/` | none |

### Key Finding: No Keycloak
The platform has **no Keycloak deployment**. clearglass validates JWTs directly against jwtlet's JWKS. Identity is built into the Kubernetes cluster via ServiceAccount tokens.

---

## 2. Participant Onboarding (CFM Flow)

### CFM Agents (Orchestration Steps)

When a new participant is provisioned via CFM, these agents execute in order:

1. **jwtlet-agent** — Creates jwtlet mappings for `controlplane`, `identityhub`, `cfm-agents`, `siglet-sa` → new participant context
2. **edcv-agent** — Creates participant context in Control Plane (`POST /api/mgmt/v5beta/participants`) with vault config (no Keycloak credentials)
3. **ih-agent** — Creates participant in IdentityHub (`POST /api/identity/v1beta/participants`) with DID, EdDSA key, service endpoints
4. **registration-agent** — Creates Holder in IssuerService (`POST /api/admin/v1beta/participants/issuer/holders`) using vpaProperties
5. **siglet-agent** — Configures siglet key mapping
6. **onboarding-agent** — Polls for credential issuance until ISSUED

### Replacing CFM with GitHub Workflows

The session produced a complete guide (`cr-bff/docs/participant-onboarding-without-cfm.md`) and example GitHub Actions workflows that replicate the CFM orchestration by calling platform APIs directly. The key simplification: bypass the Tenant Manager/Provision Manager entirely and call EDC service APIs sequentially.

---

## 3. cr-bff Integration

### Architecture Decision
`cr-bff` uses jwtlet's workload-identity token exchange path. It is deployed in the same namespace (`edc-v`) and exchanges its own SA token for participant-scoped EDC tokens. This eliminates per-participant client secrets entirely.

### Token Flow
```
User → IdP token → cr-bff → validates IdP token → resolves participant → exchanges SA token at jwtlet → calls EDC → returns result
```

### Multi-Tenant Mapping
A single `cr-bff` ServiceAccount can have multiple jwtlet mappings (one per participant context). The `resource` parameter selects which participant the token is for.

### Provisioning Contract for cr-bff
1. Create `cr-bff` ServiceAccount in namespace `edc-v`
2. Project SA token volume (audience: `https://kubernetes.default.svc.cluster.local`, expiry: 3600s)
3. Register jwtlet mapping per participant (`clientIdentifier: system:serviceaccount:edc-v:cr-bff`)
4. Scope mappings already seeded by `jwtlet-seed-job.yaml`

### Token-Issuing API for External Participants
A `POST /v1/tokens` endpoint was added to cr-bff that:
1. Validates the caller's corporate IdP token (Amadeus LSS: `https://1a.uat.accounts.amadeus.com`)
2. Checks authorization for the requested participant context
3. Exchanges cr-bff's own SA token at jwtlet
4. Returns the EDC token to the external caller

The external participant then uses that token directly against EDC APIs.

---

## 4. Corporate IdP (Amadeus) Token — Key Finding

**jwtlet CANNOT accept the Amadeus IdP token directly** because it only validates Kubernetes ServiceAccount tokens via TokenReview API. The corporate IdP token has wrong issuer, wrong `sub`, wrong audience.

**Solution:** cr-bff acts as the bridge:
- Inbound: validates IdP token (authenticates the human)
- Outbound: uses its own SA token exchange at jwtlet (gets EDC authorization)

---

## 5. Deployment Details (KinD Cluster)

- **KinD cluster name:** `jad` (Podman-backed on macOS/ARM64)
- **Namespace:** `edc-v`
- **Helm releases:** `core-platform` (Core Platform Distribution v0.0.17) + `jad-dataspace` (dataspace profile chart)
- **Port mapping:** Host ports 80/443 mapped to Traefik gateway in-cluster
- **DNS rewrite:** CoreDNS configured to resolve `*.jad.localhost` to Traefik service from inside the cluster

### Debugging Notes
- CFM agents get stuck at `Init:1/2` when `clearglass` and `jwtlet` images fail to pull (need GHCR auth or pre-loaded images)
- `issuer-credentials-seed` job fails with `403 unauthorized_client` when `jwtlet-seed` hook hasn't created the `seed-jobs → issuer` mapping (happens when `core-platform` Helm release is in `failed` state)
- Fix: delete the failed core-platform release, reinstall, then install the dataspace profile chart
