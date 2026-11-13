# Run OriginTracer locally

This guide starts the services used by the signed-in dashboard and chat UI.
The backend, Agent Server, and UI are separate processes. Keycloak supplies
user sign-in; the OriginTracer backend stores uploaded snapshots and events.

## 1. Start Keycloak

Start a local Keycloak instance in development mode. For example, with Docker:

```bash
docker run --name origintracer-keycloak \
  -p 8080:8080 \
  -e KC_BOOTSTRAP_ADMIN_USERNAME=admin \
  -e KC_BOOTSTRAP_ADMIN_PASSWORD=change-me \
  quay.io/keycloak/keycloak:latest start-dev
```

Open <http://localhost:8080> and create the `origintracer` realm. Use
`http://localhost:8080/realms/origintracer` as its issuer everywhere.

Create these clients:

| Client | Type | Local redirect URLs |
| --- | --- | --- |
| `origintracer-chat` | Confidential web client | `http://localhost:3000/api/auth/callback/keycloak` |
| `origintracer-dashboard` | Public web client with authorization-code flow and PKCE S256 | `http://localhost:5173/auth/callback`, `http://localhost:5173/auth/silent-callback` |

For the dashboard client, set the post-logout redirect to
`http://localhost:5173/`. Add an **Audience** mapper to each client's dedicated
client scope: in **Client scopes**, open the dedicated scope, choose **Add
mapper** → **By configuration** → **Audience**, set **Included Client Audience**
to `origintracer-api`, and enable **Add to access token**. The backend validates
the signed access token against this audience. Enable realm registration only
if users should create accounts themselves.

## 2. Start FastAPI

From the repository root, activate the Python environment that has OriginTracer
installed. Install the backend runtime packages if needed:

```bash
python -m pip install fastapi uvicorn httpx msgpack
```

Start the in-memory backend with local OIDC validation enabled:

```bash
ORIGINTRACER_API_KEYS=test-key-123:local-dev \
ORIGINTRACER_OIDC_ISSUER=http://localhost:8080/realms/origintracer \
ORIGINTRACER_OIDC_AUDIENCE=origintracer-api \
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8001
```

The backend reads these settings from its process environment. The in-memory store is intended for local use and is cleared when FastAPI
stops. The `test-key-123` worker key stores uploads in the `local-dev`
namespace; use a generated worker key to attach uploads to a signed-in account.

## 3. Start an instrumented worker

Configure the application with its OriginTracer probes and a worker API key.
For a first local run, `test-key-123` uploads data to the `local-dev` namespace.
To associate uploads with a signed-in account, create a worker key from the
React dashboard, set it as `ORIGINTRACER_API_KEY` for the process, and restart
the worker. For Django, put the middleware first in `MIDDLEWARE`, enable the
`django` and `asyncio` probes in `origintracer.yaml`, then call
`origintracer.init(config=BASE_DIR / "origintracer.yaml")` from the app's
`AppConfig.ready()` method.

## 4. Start the Agent Server

From `agent/`, install the Agent Server requirements in the active Python
environment. Create the ignored runtime environment file named in
`langgraph.json` and set the local backend URL and worker key:

```bash
python -m pip install -r requirements.txt
```

```dotenv
ORIGINTRACER_API_URL=http://localhost:8001
ORIGINTRACER_API_KEY=test-key-123
```

Choose a model provider and set its API key or local Ollama address in the same
environment file. Then start the development server:

```bash
langgraph dev --no-reload --no-browser --port 8123
```

The registered graph is `origintracer_investigator`. Agent Server requests and
worker discovery use the backend at port 8001.

## 5. Start a UI

### Chat UI

From `agent/chat-ui/`, install dependencies, create the local environment file,
and start Next.js:

```bash
corepack pnpm install --frozen-lockfile
cp .env.local.example .env.local
corepack pnpm dev
```

Open <http://localhost:3000>. Set the Keycloak values in `.env.local` to enable
sign-in. The browser talks to the same-origin Next.js API routes; the server
proxies requests to the Agent Server and FastAPI.

### React dashboard

From `frontend/`, install dependencies and start Vite:

```bash
npm ci
npm run dev
```

Open <http://localhost:5173>. Set these values in `frontend/.env.local` to
enable Keycloak sign-in and use the local FastAPI service:

```dotenv
VITE_API_URL=http://localhost:8001
VITE_OIDC_AUTHORITY=http://localhost:8080/realms/origintracer
VITE_OIDC_CLIENT_ID=origintracer-dashboard
```

For the chat UI, set `AUTH_KEYCLOAK_ISSUER` to the same realm URL,
`AUTH_KEYCLOAK_ID=origintracer-chat`, and the confidential client secret in
`agent/chat-ui/.env.local`. Set a private `AUTH_SECRET` there as well.

## 6. Create a worker key and confirm data

Sign in to either UI and create a worker API key. Store it in the instrumented
process environment as `ORIGINTRACER_API_KEY`, then restart that process. New
snapshots and events will be associated with the account that created the key.
The local development key remains useful for unauthenticated uploads to the
`local-dev` namespace.

The dashboard and chat worker lists populate after a process uploads its first
runtime heartbeat. A LangGraph worker can initially have an empty graph; a run
through the graph adds its operations.
