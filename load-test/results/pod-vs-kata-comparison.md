# Pod Mode vs Kata Mode Load Test Comparison

**Date:** 2026-01-14
**Scenario:** realistic (45 VUs, 15 minutes)
**Cluster:** GKE aaron-poc (us-central1)

## Summary

| Metric | Pod Mode | Kata Mode |
|--------|----------|-----------|
| **Success Rate** | 100% | 95.2% |
| **Total Iterations** | 2,006 | 2,147 |
| **Commands Executed** | 3,860 | 4,022 |
| **Requests/sec** | 4.26 | 4.45 |
| **Failed Requests** | 0 | 195 (4.79%) |

## Latency Comparison

### Command Latency (shell)
| Percentile | Pod Mode | Kata Mode |
|------------|----------|-----------|
| min | 28ms | 29ms |
| median | 3.01s | 1.35s |
| avg | 2.76s | 2.37s |
| p90 | 3.98s | 2.31s |
| p95 | 4.24s | 2.97s |
| max | 13.4s | 30s |

### Python Latency
| Percentile | Pod Mode | Kata Mode |
|------------|----------|-----------|
| min | 30ms | 29ms |
| median | 3.06s | 1.44s |
| avg | 2.87s | 2.81s |
| p90 | 3.96s | 2.46s |
| p95 | 4.25s | 15s |
| max | 9.07s | 30s |

### Sandbox Creation Latency
| Percentile | Pod Mode | Kata Mode |
|------------|----------|-----------|
| min | 877ms | 1.68s |
| median | 1.68s | 2.7s |
| avg | 3.25s | 9.03s |
| p90 | 7.08s | 30s (timeout) |
| p95 | 8.08s | 30s (timeout) |
| max | 8.88s | 30s |

## Success Rate by Operation

| Operation | Pod Mode | Kata Mode |
|-----------|----------|-----------|
| Sandbox Created | 100% | 77% (35/45) |
| Shell Commands | 100% | 96% (2363/2454) |
| Python Execution | 100% | 94% (1474/1568) |

## Analysis

### Pod Mode Strengths
- **Perfect reliability**: 100% success rate with no failures
- **Consistent latency**: Lower variance, predictable performance
- **Faster sandbox creation**: Avg 3.25s vs 9.03s

### Kata Mode Strengths
- **Lower median latency**: Commands complete faster at median (1.35s vs 3.01s)
- **Better p90 latency**: 2.31s vs 3.98s for commands
- **Higher throughput potential**: More iterations completed (2147 vs 2006)

### Kata Mode Weaknesses
- **Sandbox creation failures**: 23% of sandboxes failed to create under load
- **Timeout spikes**: Some operations hit 30s timeout
- **Higher variance**: p95 latency much worse than median

### Root Causes

**Pod Mode High Latency:**
- CRI exec contention on containerd
- Multiple pods calling crictl exec simultaneously
- Small nodes (940m CPU) with 15 pods each

**Kata Mode Failures:**
- VM startup overhead under concurrent load
- Limited node resources for running multiple VMs
- Possible vsock connection timeouts

## Recommendations

1. **For reliability-critical workloads**: Use pod mode
2. **For low-latency interactive workloads**: Use kata mode with larger nodes
3. **For kata mode at scale**:
   - Increase node resources
   - Reduce concurrent sandbox creations
   - Consider sandbox pooling

## Configuration

Both tests used:
- 3 Ubuntu nodes (kata2-pool)
- ~940m CPU allocatable per node
- ~2.8GB memory per node
- Storage disabled (no snapshot auto-save)

### Profile Distribution
| Profile | VUs | Behavior |
|---------|-----|----------|
| occasional | 10 | 1 cmd every 15-30s |
| steady | 15 | 2-4 cmds every 3-6s |
| light_python | 10 | Python 80% every 5-10s |
| heavy_python | 5 | Heavy Python 90% every 8-15s |
| bursty | 5 | 5-10 rapid cmds, then pause |
