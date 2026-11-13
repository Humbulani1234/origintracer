# Installation

Install OriginTracer from the repository root:

```bash
git clone https://github.com/Humbulani1234/origintracer.git
cd origintracer
python -m pip install .
```

For development, install the editable package with its development dependencies
as described in [Development tests](../development/tests.md).

## Choose a setup

- [Run the local applications](run-locally.md) starts Keycloak, FastAPI, the
  Agent Server, and either UI for local use.
- [Authentication setup](authentication.md) explains the Keycloak realm,
  clients, redirect URLs, audience, and worker API keys.
- [Production deployment](../deployment/production.md) describes the service
  layout and the shortest deployment path.

## Instrument a Django application

Add OriginTracer middleware as the first entry in Django's `MIDDLEWARE` list.
This lets the probes attach request context to events.

```python
MIDDLEWARE = [
    "origintracer.probes.django_probe.TracerMiddleware",
    "django.middleware.security.SecurityMiddleware",
    # ...
]
```

Create `origintracer.yaml` in the project root:

```yaml
probes:
  - django
  - asyncio
```

Initialize OriginTracer from the application's `AppConfig`:

```python
from django.apps import AppConfig

class MyAppConfig(AppConfig):
    name = "myapp"

    def ready(self):
        import origintracer

        origintracer.init(config=BASE_DIR / "origintracer.yaml")
```

Start the application, send it a few requests, and open the REPL in a second
terminal:

```bash
python -m origintracer.repl.repl
```

```text
SHOW nodes
SHOW edges
CAUSAL
```

The graph is built from the events emitted by the configured probes. See the
[Django probe guide](../probes/django.md) for its event types and configuration.
