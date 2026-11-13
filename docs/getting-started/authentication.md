# Authentication setup

OriginTracer uses Keycloak for people signing in and OriginTracer API keys for
instrumented processes uploading data. These credentials have separate jobs:
UI users receive short-lived access tokens, while each worker uses a dedicated
API key.

## Configure the realm

Create a Keycloak realm named `origintracer`. For each web client, open its
**Client scopes** tab, select its dedicated client scope, choose **Add mapper**
→ **By configuration** → **Audience**, and set **Included Client Audience** to
`origintracer-api`. Enable **Add to access token**. The FastAPI backend checks
the token signature, issuer, audience, and expiry; use the access token rather
than the ID token.

Enable user registration in the realm only if users should create their own
accounts. Configure email verification and password policy to match the
service's account policy.

## Configure the chat client

Create a confidential OpenID Connect client named `origintracer-chat`. Enable
the standard authorization-code flow and set its client secret in the chat UI's
server environment. Register these redirect URLs:

| Use | Redirect URL |
| --- | --- |
| Local development | `http://localhost:3000/api/auth/callback/keycloak` |
| Production at `/chat-agent` | `https://your-domain/chat-agent/api/auth/callback/keycloak` |

The Next.js server uses `AUTH_KEYCLOAK_ISSUER`, `AUTH_KEYCLOAK_ID`, and
`AUTH_KEYCLOAK_SECRET`. Set `AUTH_SECRET` to a random, private value. The issuer
must use the same host name users use to reach Keycloak.

## Configure the React client

Create a public OpenID Connect client named `origintracer-dashboard`. Enable
standard authorization-code flow with PKCE (S256). Register these redirect URLs
and post-logout destinations:

| Use | Redirect URL | Post-logout URL |
| --- | --- | --- |
| Local development | `http://localhost:5173/auth/callback` | `http://localhost:5173/` |
| Production at `/chat-react` | `https://your-domain/chat-react/auth/callback` | `https://your-domain/chat-react/` |

Register the silent callback URLs as well:
`http://localhost:5173/auth/silent-callback` and
`https://your-domain/chat-react/auth/silent-callback`.

Set `VITE_OIDC_AUTHORITY` to the realm URL and `VITE_OIDC_CLIENT_ID` to
`origintracer-dashboard` in the environment used to run or build the dashboard.
These values are public browser configuration; do not put secrets or worker API
keys in `VITE_*` variables.

## Configure FastAPI

Set these variables in the backend process environment:

```dotenv
ORIGINTRACER_OIDC_ISSUER=https://identity.example.com/realms/origintracer
ORIGINTRACER_OIDC_AUDIENCE=origintracer-api
# Optional; defaults to the issuer's Keycloak certs endpoint.
ORIGINTRACER_OIDC_JWKS_URL=https://identity.example.com/realms/origintracer/protocol/openid-connect/certs
```

The issuer must exactly match the token's `iss` claim. For local development,
use `http://localhost:8080/realms/origintracer` consistently in Keycloak,
FastAPI, and both UIs. `backend/main.py` does not load dotenv files; set these
values in the shell, Supervisor, or the service manager that starts Uvicorn.

## Create worker API keys

After signing in, create a worker key in the UI and copy it when it is shown.
The backend stores only a hash, so the secret cannot be displayed again. Set
the key as `ORIGINTRACER_API_KEY` for the instrumented process and restart that
process. Revoke keys that are no longer used.

For local development, the backend can also accept `test-key-123` when it is
configured as a development key. It uploads to the `local-dev` namespace and
is not associated with the signed-in account. Production must use generated
worker keys and must not configure or accept the development key.
