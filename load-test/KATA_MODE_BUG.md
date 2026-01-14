# Kata Mode Exec Bug (FIXED)

**Status:** Fixed on 2026-01-13

## Original Issue

When running in kata mode, the sandbox-agent failed to execute commands inside containers with error:
```
the file echo was not found
```

## Root Cause

In `src/sandbox-agent/pkg/service/service.go`, the `resolveContainerID` function returns the **sandbox ID** (VM ID) for kata mode:

```go
// In kata mode, use sandbox ID
sandboxID, err := s.resolveSandboxID(sessionID)
if err != nil {
    return "", err
}
return sandboxID, nil
```

However, the kata-agent's `ExecProcessRequest` expects a **container ID** - the ID of the container running inside the VM, not the VM itself.

### IDs Involved

For a kata sandbox pod:
- **Sandbox ID** (VM ID): `8ff83935aadefc0f2403b8ca5cc4e0902e6a6ac88117a716898f6e27b37e6204`
  - From annotation: `io.kubernetes.cri.sandbox-id`
  - This is the ID of the Kata VM

- **Container ID**: `44ccca91edafbf38f13e40ff6f877065d349a2c2db796232022dc6f88067f08a`
  - From: `pod.status.containerStatuses[0].containerID`
  - This is the ID of the container running inside the VM

The current code passes the sandbox ID to kata-agent, but kata-agent looks for the executable in the context of the sandbox (VM), not the container. The container is where the Python/shell executables actually exist.

## Fix Required

Modify `resolveContainerID` in kata mode to:

1. Get the sandbox ID (to connect to the right shim/kata-agent)
2. Get the container ID from pod status (to exec into the right container)
3. Return both, or modify the backend to accept both

### Proposed Fix

In `src/sandbox-agent/pkg/service/service.go`:

```go
func (s *service) resolveContainerID(sessionID string) (string, error) {
    if sessionID == "" {
        return "", fmt.Errorf("%w: session id is required", apierrors.ErrInvalidArgument)
    }

    // In both modes, use container ID from pod status
    containerID, ok := s.store.ContainerIDForSession(sessionID)
    if !ok {
        return "", fmt.Errorf("%w: container not found for session", apierrors.ErrNotFound)
    }
    return containerID, nil
}
```

And in the kata backend, the `Exec` method needs to:
1. Use the sandbox ID to connect to the kata-agent (for shim lookup)
2. Use the container ID in the ExecProcessRequest

```go
func (b *Backend) Exec(ctx context.Context, containerID string, cmd []string, env []string, cwd string, timeout time.Duration) (*backend.ExecResult, error) {
    // containerID is the actual container ID
    // sandboxID is needed to find the shim socket - need to look it up separately
    sandboxID := b.getSandboxIDForContainer(containerID)

    result, err := b.agentClient.Exec(ctx, sandboxID, containerID, cmd, env, cwd, timeout)
    // ...
}
```

## Workaround

Until fixed, use **pod mode** for load testing. Pod mode uses CRI exec which works correctly.

## Impact

- Kata mode exec completely broken
- Kata mode shell/Python REPL broken
- Kata mode interactive processes broken

## Files Modified

1. `src/sandbox-agent/pkg/service/service.go` - `resolveContainerID()`
2. `src/sandbox-agent/pkg/backend/kata/kata.go` - `Exec()` and related methods
3. `src/sandbox-agent/pkg/k8s/store.go` - Added `SandboxIDForContainerID()` method

## Fix Applied

The fix was applied on 2026-01-13 with these changes:

1. **`src/sandbox-agent/pkg/k8s/store.go`**: Added `SandboxIDForContainerID(containerID string)` method that looks up the sandbox ID (VM ID) from a container ID.

2. **`src/sandbox-agent/pkg/backend/kata/kata.go`**:
   - Added `SandboxIDResolver` function type
   - Added `SetSandboxIDResolver()` method
   - Added `resolveSandboxID()` helper that uses the resolver
   - Updated all methods to use `sandboxID := b.resolveSandboxID(containerID)` instead of `sandboxID := containerID`

3. **`src/sandbox-agent/pkg/service/service.go`**:
   - Modified `resolveContainerID()` to always return the container ID from pod status, regardless of mode

4. **`src/sandbox-agent/main.go`**:
   - Wired up the resolver: `kataBackend.SetSandboxIDResolver(watcher.Store().SandboxIDForContainerID)`

## Test Results

After the fix, kata mode works correctly:
- Smoke test: 100% success rate, 40ms median latency
- Realistic test (45 VUs): 95% success rate, 1.35s median latency

Note: Commands must use absolute paths (e.g., `/bin/echo` instead of `echo`) as kata-agent doesn't search PATH.
