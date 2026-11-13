# Production deployment

This guide covers the production layout for OriginTracer's backend, Agent
Server, chat UI, and React dashboard. It assumes DNS, TLS, Docker, PostgreSQL,
and a Keycloak realm are available. The examples use one Ubuntu host; keep the
same service boundaries if you deploy across multiple hosts.

## Service layout

| Service | Runs as | Public access |
| --- | --- | --- |
| Keycloak | Identity service | HTTPS on its own host name |
| FastAPI | Uvicorn under a service manager | Nginx proxy only |
| PostgreSQL | Database service | Private network only |
| Agent Server | LangGraph Docker Compose stack | Nginx proxy only |
| Chat UI | Next.js under PM2 or systemd | Nginx at `/chat-agent/` |
| React dashboard | Static Vite files | Nginx at `/chat-react/` |

Use PostgreSQL for durable backend data and keep internal service ports private.
The settings below assume the Keycloak realm issuer is
`https://identity.example.com/realms/origintracer` and the API audience is
`origintracer-api`.

## 1. Configure Keycloak

Configure `origintracer-chat` as a confidential client and
`origintracer-dashboard` as a public client with authorization-code flow and
PKCE S256. Register these URLs on the clients:

| Client | Valid redirect URLs | Post-logout URL |
| --- | --- | --- |
| `origintracer-chat` | `https://your-domain/chat-agent/api/auth/callback/keycloak` | — |
| `origintracer-dashboard` | `https://your-domain/chat-react/auth/callback`, `https://your-domain/chat-react/auth/silent-callback` | `https://your-domain/chat-react/` |

Add an **Audience** mapper to each client's dedicated client scope: in **Client
scopes**, open the dedicated scope, choose **Add mapper** → **By configuration**
→ **Audience**, set **Included Client Audience** to `origintracer-api`, and
enable **Add to access token**. The FastAPI audience setting and access tokens
must use the same value.

Store the chat client secret only in the server environment. Disable public
registration unless open account creation is intended, and configure the realm's
email verification, password policy, and brute-force protection.

## 2. Configure and start FastAPI

Set these values in the FastAPI service environment:

```dotenv
ORIGINTRACER_ENV=production
ORIGINTRACER_DB_DSN=postgresql://USER:PASSWORD@HOST/origintracer
ORIGINTRACER_OIDC_ISSUER=https://identity.example.com/realms/origintracer
ORIGINTRACER_OIDC_AUDIENCE=origintracer-api
# Optional; defaults to the issuer's Keycloak certs endpoint.
ORIGINTRACER_OIDC_JWKS_URL=https://identity.example.com/realms/origintracer/protocol/openid-connect/certs
```

Run Uvicorn on a private interface under Supervisor, systemd, or an equivalent
service manager. The command is:

```bash
cd backend
uvicorn main:app --host 127.0.0.1 --port 8001
```

Do not configure the local `test-key-123` key in production.

## 3. Deploy the React dashboard and create a worker key

Create `frontend/.env.production` from the example and set the public build
values:

```bash
cp .env.production.example .env.production
```

```dotenv
VITE_BASE_PATH=/chat-react/
VITE_API_URL=/origintracer-api
VITE_OIDC_AUTHORITY=https://identity.example.com/realms/origintracer
VITE_OIDC_CLIENT_ID=origintracer-dashboard
```

From `frontend/`, install locked dependencies and build the static app:

```bash
npm ci
npm run build
sudo mkdir -p /var/www/origintracer/chat-react
sudo cp -a dist/. /var/www/origintracer/chat-react/
```

Add the React and FastAPI locations to the TLS Nginx server block. The API
proxy strips `/origintracer-api/` and forwards the signed-in user's bearer
token:

```nginx
location = /chat-react {
    return 308 /chat-react/;
}

location ^~ /chat-react/ {
    root /var/www/origintracer;
    try_files $uri $uri/ /chat-react/index.html;
}

location ^~ /origintracer-api/ {
    limit_except GET HEAD POST DELETE { deny all; }
    proxy_pass http://127.0.0.1:8001/;
    proxy_set_header Authorization $http_authorization;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

Sign in to the React dashboard and create a worker API key. It is shown once;
store it in the service manager's protected environment for each instrumented
process. `VITE_*` values are public and included in browser files, so never put
an OIDC secret or worker API key there.

## 4. Build and start the Agent Server

In the Agent Server environment file, set the Docker-reachable FastAPI address,
a generated worker key, and the model provider credentials:

```dotenv
ORIGINTRACER_API_URL=http://host.docker.internal:8001
ORIGINTRACER_API_KEY=GENERATED_WORKER_KEY
```

From `agent/`, build and start the LangGraph Compose stack:

```bash
langgraph build -t origintracer-langgraph:agent
langgraph up --docker-compose docker-compose.yml \
  --image origintracer-langgraph:agent --wait
```

The Compose configuration exposes the Agent Server on port 8123 and supplies
its Redis and PostgreSQL services. Keep those ports private. The registered
assistant ID is `origintracer_investigator`.

## 5. Build and run the Chat UI

Create `agent/chat-ui/.env.local` from `.env.local.example`, then set the
production values before building:

```dotenv
NEXT_BASE_PATH=/chat-agent
NEXT_PUBLIC_BASE_PATH=/chat-agent
NEXT_PUBLIC_API_URL=https://your-domain/chat-agent/api
NEXT_PUBLIC_ASSISTANT_ID=origintracer_investigator
LANGGRAPH_API_URL=http://127.0.0.1:8123
ORIGINTRACER_API_URL=http://127.0.0.1:8001
ORIGINTRACER_API_KEY=GENERATED_WORKER_KEY
AUTH_SECRET=GENERATED_RANDOM_SECRET
AUTH_KEYCLOAK_ISSUER=https://identity.example.com/realms/origintracer
AUTH_KEYCLOAK_ID=origintracer-chat
AUTH_KEYCLOAK_SECRET=KEYCLOAK_CLIENT_SECRET
```

The Next.js process uses the host's `127.0.0.1:8001` address. From
`agent/chat-ui/`, install the locked packages and build:

```bash
corepack pnpm install --frozen-lockfile
corepack pnpm exec next build --webpack
pm2 start corepack --name origintracer-chat-ui -- \
  pnpm exec next start --hostname 127.0.0.1 --port 3000
pm2 save
```

The production build uses Webpack on the tested Linux ARM64 host. Add the Chat
UI locations to the TLS Nginx server block and keep port 3000 private:

```nginx
location = /chat-agent {
    proxy_pass http://127.0.0.1:3000;
    include proxy_params;
    proxy_buffering off;
    proxy_read_timeout 3600s;
}

location ^~ /chat-agent/ {
    proxy_pass http://127.0.0.1:3000;
    include proxy_params;
    proxy_buffering off;
    proxy_request_buffering off;
    proxy_read_timeout 3600s;
    proxy_send_timeout 3600s;
}
```

Validate and reload Nginx after adding the locations. Keep FastAPI, PostgreSQL,
Redis, and Agent Server ports closed to public traffic.

## Production checklist

- HTTPS is active for the application and identity-provider hosts.
- Both Keycloak clients issue access tokens with `origintracer-api` in `aud`.
- FastAPI uses the exact Keycloak issuer and a persistent PostgreSQL DSN.
- Instrumented processes use generated worker keys, not the development key.
- Secrets remain in service environments or a secret manager, never in a
  frontend build or committed configuration file.
- PostgreSQL and internal service ports are private.
- Nginx serves `/chat-agent/` and `/chat-react/` and proxies `/origintracer-api/`.

