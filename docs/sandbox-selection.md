# Sandbox Technology Selection

## Decision: sandbox-exec (macOS) + unshare (Linux) + python-resource fallback

| Platform | Backend | Isolation Level | Notes |
|----------|---------|----------------|-------|
| macOS | `/usr/bin/sandbox-exec` | OS-level (SBPL profile) | Default on macOS 10.7+ |
| Linux | `unshare --pid --fork --mount-proc` | Namespace-level | Requires CAP_SYS_ADMIN or user namespaces |
| Fallback | `resource.setrlimit` | Process-level (soft) | CPU/memory/disk/process limits only |

## Why not other options?

| Option | Rejected Reason |
|--------|----------------|
| Docker/Podman | Too heavy for per-task sandboxing; requires daemon |
| gVisor | Linux-only, complex setup |
| Firejail | SUID binary, security concerns |
| AppArmor/SELinux | System-wide policy, not per-task |
| chroot | Requires root, trivial escape |

## Architecture

```
SandboxExecutor
  ├─ _detect_backend()       → sandbox-exec | unshare | python-resource
  ├─ _build_sbpl_profile()   → SBPL string (macOS)
  ├─ execute_in_sandbox()    → asyncio subprocess with sandbox wrapper
  ├─ cleanup_profile()       → remove temp SBPL file
  └─ backend (property)      → current backend name
```

## SBPL Profile (macOS)

- Deny all by default
- Allow read from /System, /Library, /usr
- Allow read/write from configured `allowed_paths`
- Allow network to configured `allowed_network_hosts` (or all if empty)
- Allow process-fork, signal, IPC

## Security Guarantees

- **sandbox-exec**: Kernel-enforced, cannot be bypassed from userspace
- **unshare**: PID/mount namespace isolation, files still accessible without mount namespace
- **python-resource**: Soft limits only, cooperative enforcement
