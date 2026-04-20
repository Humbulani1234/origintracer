# asyncio Probe — BCC Installation

BCC is a system-level tool and cannot be installed inside a virtualenv with
`pip`. It must be installed at the OS level and imported from there.

## Requirements

- Linux kernel 4.9 or higher (5.x / 6.x are fine)
- `CAP_BPF` or root
- `apt`-based distro (Ubuntu / Debian)

Check your kernel version:

```bash
uname -r
```

## Installation

**1. Install kernel headers**

BCC compiles BPF programs at runtime against your running kernel and needs
headers for that exact version:

```bash
sudo apt update
sudo apt install linux-headers-$(uname -r)
```

Verify:

```bash
ls /lib/modules/$(uname -r)/build
```

**2. Install BCC at OS level**

```bash
sudo apt install -y bpfcc-tools libbpfcc-dev python3-bpfcc
```

| Package | Provides |
|---|---|
| `bpfcc-tools` | Command-line BCC tools |
| `python3-bpfcc` | Python bindings — `from bcc import BPF` |
| `libbpfcc-dev` | C headers needed to compile BPF programs |

**3. Verify outside your virtualenv**

```bash
deactivate
python3 -c "from bcc import BPF; print('bcc ok')"
```

**4. Make BCC visible inside your virtualenv**

BCC's Python bindings live in the system Python, not in your venv.
A `.pth` file bridges the gap.

Find where `python3-bpfcc` installed to:

```bash
python3 -c "import bcc; print(bcc.__file__)"
# Typically: /usr/lib/python3/dist-packages/bcc/__init__.py
```

Activate your venv and add the system packages path:

```bash
source /path/to/your/venv/bin/activate

echo "/usr/lib/python3/dist-packages" > \
    $(python -c "import site; print(site.getsitepackages()[0])")/system_bcc.pth
```

Verify inside the venv:

```bash
python -c "from bcc import BPF; print('bcc visible in venv')"
```

Once this passes, `get_bridge()` will work and `bridge.available` will be `True`.

**5. Verify BPF permissions**

```bash
sudo $(which python) -c "
    from bcc import BPF
    b = BPF(text='int kprobe__sys_clone(void *ctx) { return 0; }')
    print('BPF compile ok')
"
```

**6. Run gunicorn as root** *(dev only)*

kprobes require root to attach:

```bash
sudo /path/to/your/venv/bin/gunicorn \
    -c gunicorn.conf.py \
    config.asgi:application \
    --worker-class uvicorn.workers.UvicornWorker
```

## Checklist

- [ ] `uname -r` → kernel >= 4.9
- [ ] `apt install linux-headers-$(uname -r)` → headers present
- [ ] `apt install python3-bpfcc` → BCC installed at OS level
- [ ] `.pth` file written into venv → BCC visible inside venv
- [ ] `sudo python -c "from bcc import BPF"` → compile test passes
- [ ] gunicorn running as sudo → kprobes can attach