# Load Test Results: Realistic Scenario

**Date:** 2026-01-14 02:41 UTC
**Scenario:** `realistic`
**Cluster:** GKE `aaron-poc` (us-central1)

## Configuration

### Profile Distribution (45 VUs total)
| Profile | Count | Behavior |
|---------|-------|----------|
| occasional | 10 | 1 cmd every 15-30s |
| steady | 15 | Batch of 2-4 cmds every 3-6s |
| light_python | 10 | Python (80%) every 5-10s |
| heavy_python | 5 | Heavy Python (90%) every 8-15s |
| bursty | 5 | 5-10 rapid cmds, then pause |

### Test Phases
- Ramp-up: 3 minutes (0 → 45 VUs)
- Hold: 10 minutes (45 VUs)
- Ramp-down: 2 minutes (45 → 0 VUs)

### Infrastructure
- **Nodes:** 3 Ubuntu nodes (kata2-pool)
  - `gke-aaron-poc-kata2-pool-9561edec-54ph`: 15 pods
  - `gke-aaron-poc-kata2-pool-a859cce2-h6xx`: 15 pods
  - `gke-aaron-poc-kata2-pool-e012c9de-eyvk`: 14 pods
- **Node specs:** ~940m CPU allocatable, ~2.8GB memory
- **Sandbox mode:** Pod (CRI-based, not Kata VMs)
- **Storage:** Disabled (no snapshot auto-save)

## Results Summary

| Metric | Value |
|--------|-------|
| **Duration** | 15m 16s |
| **Total Iterations** | 2,006 |
| **Commands Executed** | 3,860 |
| **Requests/sec** | 4.26 |
| **Success Rate** | **100%** |
| **Errors** | 0 |

## Latency Metrics

### Sandbox Creation
| Percentile | Latency |
|------------|---------|
| min | 877ms |
| p50 (median) | 1.68s |
| p90 | 7.08s |
| p95 | 8.08s |
| max | 8.88s |

### Shell Command Execution
| Percentile | Latency |
|------------|---------|
| min | 28ms |
| p50 (median) | 3.01s |
| p90 | 3.98s |
| p95 | 4.24s |
| max | 13.4s |

### Python Execution
| Percentile | Latency |
|------------|---------|
| min | 30ms |
| p50 (median) | 3.06s |
| p90 | 3.96s |
| p95 | 4.25s |
| max | 9.07s |

## Threshold Results

| Threshold | Target | Actual | Status |
|-----------|--------|--------|--------|
| http_req_duration p95 | < 5000ms | 4260ms | ✅ PASS |
| http_req_failed | < 5% | 0% | ✅ PASS |
| success_rate | > 95% | 100% | ✅ PASS |

## Observations

### Positive
1. **100% success rate** - No failed requests across 3,906 HTTP calls
2. **Even pod distribution** - Load balanced evenly across 3 nodes
3. **Stable under load** - System maintained 45 concurrent sandboxes for 10+ minutes
4. **All profiles worked** - occasional, steady, light_python, heavy_python, bursty all executed correctly

### Areas of Concern
1. **High command latency** - Median 3s for shell commands (expected ~50ms based on smoke test)
2. **Sandbox creation variance** - p95 at 8s vs p50 at 1.68s indicates contention
3. **Max latency spikes** - 13.4s max for shell commands suggests occasional resource starvation

### Likely Bottlenecks
1. **CRI exec contention** - Multiple pods calling `crictl exec` simultaneously
2. **Node CPU saturation** - Small nodes (940m CPU) with 15 pods each
3. **sandbox-agent capacity** - Single agent per node handling all exec requests

## Raw k6 Output

```
checks_total.......: 3905    4.261247/s
checks_succeeded...: 100.00% 3905 out of 3905
checks_failed......: 0.00%   0 out of 3905

command_latency................: avg=2.76s  min=28ms   med=3.01s  max=13.4s p(90)=3.98s  p(95)=4.24s
commands_executed..............: 3860    4.212142/s
python_latency.................: avg=2.87s  min=30ms   med=3.06s  max=9.07s p(90)=3.96s  p(95)=4.25s
sandbox_create_latency.........: avg=3.25s  min=877ms  med=1.68s  max=8.88s p(90)=7.08s  p(95)=8.08s
success_rate...................: 100.00% 3905 out of 3905

http_req_duration..............: avg=2.8s   min=1.63ms med=3.02s  max=13.4s p(90)=3.98s  p(95)=4.26s
http_req_failed................: 0.00%   0 out of 3906
http_reqs......................: 3906    4.262338/s

iteration_duration.............: avg=16.95s min=5.54s  med=14.45s max=1m9s  p(90)=26.71s p(95)=31.24s
iterations.....................: 2006    2.189004/s
vus............................: 2       min=0            max=45
vus_max........................: 45      min=45           max=45

data_received..................: 1.1 MB  1.2 kB/s
data_sent......................: 890 kB  971 B/s
```

## Next Steps

1. **Profile resource usage** - Check sandbox-agent CPU/memory during load
2. **Test with larger nodes** - Would larger nodes reduce latency?
3. **Run worst_case scenario** - Test with all heavy_python users
4. **Compare with Kata mode** - How does VM-based isolation compare?
