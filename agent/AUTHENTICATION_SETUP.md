# Local authentication setup

This guide covers the local Keycloak login flow shared by the React dashboard
and chat-ui, plus the separate API keys used by instrumented workers. It uses
the local URLs and client IDs currently configured for this repository.

## How the credentials fit together

There are two kinds of credentials:

- **OIDC access token:** Keycloak issues one when a person signs in. React sends
  it to FastAPI. Chat-ui sends it through the Agent Server, which asks FastAPI
  to validate it. FastAPI checks its signature, issuer, audience, and expiry.
- **Worker API key:** A signed-in person creates one in chat-ui. Instrumented
  processes use it to upload their observations to FastAPI. Use a separate key
  for each process so a key can be revoked independently.

Do not put worker keys or client secrets in browser `VITE_*` variables. Those
values are included in the built React files.

## 1. Configure Keycloak

Create or select the `origintracer` realm. Enable **Realm settings → Login →
User registration** if users should be able to register themselves.

Create these two OpenID Connect clients:

| Setting | `origintracer-dashboard` | `origintracer-chat` |
|---|---|---|
| Client authentication | Off (public client) | On (confidential client) |
| Standard flow | On | On |
| PKCE | S256 | Optional for this server-side client |
| Valid redirect URI | `http://localhost:5173/origintracer/react/auth/callback` and `http://localhost:5173/origintracer/react/auth/silent-callback` | `http://localhost:3000/api/auth/callback/keycloak` |
| Web origin | `http://localhost:5173` | `http://localhost:3000` |
| Post-logout redirect | `http://localhost:5173/origintracer/react/` | `http://localhost:3000/` |

For the chat client, copy its client secret from **Credentials** into the
chat-ui's ignored `.env.local` file. React is a browser client and must not use
a client secret.

For **each** client, add an **Audience** mapper to its dedicated scope:

1. Open **Clients → client → Client scopes → dedicated scope and mappers**.
2. Choose **Configure a new mapper → Audience**.
3. Set **Included Custom Audience** to `origintracer-api`.
4. Enable **Add to access token**, then save.

The access token should have `origintracer-api` in its `aud` claim. `account`
may also appear there. Keycloak's **Client scopes → Evaluate** view can check
the effective mapper for a client before you sign in.

## 2. Keep the issuer identical everywhere

Use one hostname consistently. For this local setup, use:

```text
http://localhost:8080/realms/origintracer
```

`localhost` and `127.0.0.1` are different issuer strings. FastAPI compares its
configured issuer with the token's `iss` claim exactly. Check Keycloak's
discovery document before startup if unsure:

```bash
curl -s http://localhost:8080/realms/origintracer/.well-known/openid-configuration \
  | python -c 'import json, sys; print(json.load(sys.stdin)["issuer"])'
```

Use the returned `issuer` value in all three settings below.

## 3. Start FastAPI with OIDC enabled

FastAPI does not load its OIDC settings from a dotenv file. Set them in the
environment of the process that runs Uvicorn. From the repository's
`backend/` directory:

```bash
ORIGINTRACER_API_KEYS=test-key-123:local-dev \
ORIGINTRACER_OIDC_ISSUER=http://localhost:8080/realms/origintracer \
ORIGINTRACER_OIDC_AUDIENCE=origintracer-api \
uvicorn main:app --host 0.0.0.0 --port 8001 --log-level info
```

The `test-key-123` key is for existing development uploaders and maps them to
`local-dev`. It does not make their data belong to the signed-in Keycloak user.
Restart Uvicorn after changing these values.

## 4. Configure the local UIs

### React dashboard

Create `frontend/.env.local` (do not commit it) with:

```dotenv
VITE_BASE_PATH=/origintracer/react/
VITE_API_URL=http://localhost:8001
VITE_OIDC_AUTHORITY=http://localhost:8080/realms/origintracer
VITE_OIDC_CLIENT_ID=origintracer-dashboard
```

Start or restart Vite from `frontend/`:

```bash
npm run dev
```

Open `http://localhost:5173/origintracer/react/`. Vite embeds `VITE_*` values
when it starts/builds, so restart the dev server after changing them and
rebuild for production changes.

### Agent Server and chat-ui

Copy `agent/chat-ui/.env.local.example` to `agent/chat-ui/.env.local`. Set the
server-side values; keep the client secret private:

```dotenv
AUTH_SECRET=<random-long-value>
AUTH_KEYCLOAK_ISSUER=http://localhost:8080/realms/origintracer
AUTH_KEYCLOAK_ID=origintracer-chat
AUTH_KEYCLOAK_SECRET=<origintracer-chat-client-secret>
```

Generate a local NextAuth secret with `openssl rand -hex 32`. Keep
`NEXT_BASE_PATH` empty for local development. Start the Agent Server on the
port configured by chat-ui, then run `corepack pnpm dev` from
`agent/chat-ui/`. Open `http://localhost:3000`.

`agent/langgraph.json` loads `agent/.env.example` into the Agent Server. That
file is ignored by Git and is the local runtime environment in this setup;
keep real provider keys out of tracked templates and never commit the local
file. From `agent/`, start the server with:

```bash
langgraph dev --no-reload --no-browser --port 8123
```

## 5. Create and use worker keys

After signing in to chat-ui, open **Account → Create worker key**. Name the key
for one process, copy the plaintext value immediately, and store it in that
process's environment:

```dotenv
ORIGINTRACER_API_URL=http://localhost:8001
ORIGINTRACER_API_KEY=<generated-worker-key>
```

Restart the instrumented process after setting the key. For example, set it in
the Django or Flask process environment or its Supervisor program. To observe
the Agent Server itself, create a separate key and set it in the environment
loaded by `langgraph dev`. Do not put these keys in React `.env.local`,
chat-ui's browser bundle, or `VITE_*` settings.

The local in-memory backend loses generated keys and uploaded data when it
stops. Use the PostgreSQL backend for data and keys that must survive restarts.

## 6. Verify the flow

1. Confirm the Keycloak discovery `issuer` matches FastAPI and both UI settings.
2. Sign in separately to React and chat-ui. Each uses its own OIDC client.
3. If FastAPI returns 401, inspect its log. `InvalidIssuerError` means the
   actual token `iss` differs; `InvalidAudienceError` means its `aud` lacks
   `origintracer-api`. Check the **access token**, not the ID token.
4. Sign out and sign back in after mapper changes so the app gets a fresh token.
5. Create a worker key in chat-ui, set it in one instrumented process, restart
   that process, and confirm it appears under the signed-in account.

Do not paste a live access token or generated API key into logs, tickets, or
chat. When sharing token diagnostics, share only the `iss`, `aud`, and expiry
claims.
