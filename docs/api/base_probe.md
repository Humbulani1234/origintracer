# SDK Base Probe

`BaseProbe` defines the lifecycle contract for runtime instrumentation. `ProbeRegistry` registers probe classes and creates configured instances.

::: origintracer.sdk.base_probe.BaseProbe
    options:
      members:
        - start
        - stop

::: origintracer.sdk.base_probe.ProbeRegistry
    options:
      members:
        - register
        - get
        - instantiate
        - available
        - load_from_config
