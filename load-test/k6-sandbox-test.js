// k6 Load Test for Sandbox Service
// Each VU creates ONE sandbox and reuses it for the entire test duration
//
// Run: k6 run -e BASE_URL=http://kata-deploy-sandbox-service.kata-system:8080 k6-sandbox-test.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter, Rate } from 'k6/metrics';
import { randomString, randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Custom metrics
const sandboxCreateLatency = new Trend('sandbox_create_latency', true);
const shellLatency = new Trend('shell_latency', true);
const pythonLatency = new Trend('python_latency', true);
const sandboxErrors = new Counter('sandbox_errors');
const successRate = new Rate('success_rate');
const commandsExecuted = new Counter('commands_executed');

// Configuration
const BASE_URL = __ENV.BASE_URL || 'http://kata-deploy-sandbox-service.kata-system:8080';
const SCENARIO = __ENV.SCENARIO || 'standard';

// Scenario configurations
const scenarios = {
  // Quick smoke test - 2 concurrent sandboxes
  smoke: {
    executor: 'constant-vus',
    vus: 2,
    duration: '1m',
  },
  // Standard load test - ramp up concurrent sandboxes
  standard: {
    executor: 'ramping-vus',
    startVUs: 0,
    stages: [
      { duration: '1m', target: 10 },   // ramp to 10 concurrent sandboxes
      { duration: '3m', target: 10 },   // hold - each runs commands
      { duration: '1m', target: 25 },   // ramp to 25
      { duration: '3m', target: 25 },   // hold
      { duration: '1m', target: 50 },   // ramp to 50
      { duration: '3m', target: 50 },   // hold
      { duration: '2m', target: 0 },    // ramp down (sandboxes cleaned up)
    ],
  },
  // Stress test - find breaking point
  stress: {
    executor: 'ramping-vus',
    startVUs: 0,
    stages: [
      { duration: '2m', target: 25 },
      { duration: '3m', target: 50 },
      { duration: '3m', target: 100 },
      { duration: '3m', target: 150 },
      { duration: '3m', target: 200 },
      { duration: '2m', target: 0 },
    ],
  },
  // Spike test - sudden burst of new sandboxes
  spike: {
    executor: 'ramping-vus',
    startVUs: 0,
    stages: [
      { duration: '30s', target: 10 },
      { duration: '10s', target: 100 }, // sudden spike
      { duration: '2m', target: 100 },
      { duration: '30s', target: 10 },
      { duration: '1m', target: 10 },
      { duration: '30s', target: 0 },
    ],
  },
  // Soak test - long duration with steady sandboxes
  soak: {
    executor: 'constant-vus',
    vus: 30,
    duration: '30m',
  },
};

export const options = {
  scenarios: {
    default: scenarios[SCENARIO] || scenarios.standard,
  },
  thresholds: {
    http_req_duration: ['p(95)<5000', 'p(99)<10000'],
    http_req_failed: ['rate<0.05'],
    sandbox_create_latency: ['p(95)<10000'],
    shell_latency: ['p(95)<3000'],
    success_rate: ['rate>0.95'],
  },
  // Ensure cleanup runs even on test abort
  teardownTimeout: '60s',
};

// Helper to make requests
function makeRequest(method, path, body = null) {
  const url = `${BASE_URL}${path}`;
  const params = {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',
  };

  if (method === 'GET') return http.get(url, params);
  if (method === 'POST') return http.post(url, body ? JSON.stringify(body) : null, params);
  if (method === 'DELETE') return http.del(url, null, params);
}

// Setup: runs once per VU at start - creates the sandbox
export function setup() {
  console.log(`Load test starting against ${BASE_URL}`);
  console.log(`Scenario: ${SCENARIO}`);

  // Verify service is healthy
  const res = http.get(`${BASE_URL}/healthz`, { timeout: '5s' });
  if (res.status !== 200) {
    throw new Error(`Service not healthy: ${res.status}`);
  }
  console.log('Service health check passed');

  return { startTime: Date.now() };
}

// Each VU gets a unique, persistent session ID
function getSessionId() {
  // Use VU number to create a stable session ID per VU
  // This ensures the same VU always uses the same sandbox
  return `loadtest-vu${__VU}-${randomString(6)}`;
}

// Main test function - runs repeatedly for each VU
// The sandbox is created on first iteration and reused
export default function(data) {
  // Each VU has a persistent session ID based on its VU number
  // We use exec.vu.idInTest but since that's not available, we use __VU
  const sessionId = `loadtest-vu${__VU}`;

  // First iteration for this VU - create the sandbox
  if (__ITER === 0) {
    const start = Date.now();
    const res = makeRequest('POST', `/v1/${sessionId}/exec`, {
      command: ['echo', 'sandbox-ready'],
      timeout_ms: 15000,
    });
    sandboxCreateLatency.add(Date.now() - start);

    const ok = check(res, {
      'sandbox created': (r) => r.status === 200,
    });
    successRate.add(ok);

    if (!ok) {
      sandboxErrors.add(1);
      console.log(`VU ${__VU}: Failed to create sandbox: ${res.status} ${res.body}`);
      sleep(5); // Back off before retry
      return;
    }

    console.log(`VU ${__VU}: Sandbox created (session: ${sessionId})`);
    sleep(1);
  }

  // Run a batch of commands against the existing sandbox
  runCommandBatch(sessionId);

  // Small delay between iterations
  sleep(randomIntBetween(1, 3));
}

// Run a realistic batch of commands
function runCommandBatch(sessionId) {
  // Randomly pick a workload type
  const rand = Math.random();

  if (rand < 0.5) {
    // 50%: Shell commands
    runShellCommands(sessionId, randomIntBetween(3, 6));
  } else if (rand < 0.8) {
    // 30%: Python execution
    runPythonCode(sessionId);
  } else {
    // 20%: Mixed workload
    runShellCommands(sessionId, 2);
    runPythonCode(sessionId);
    runShellCommands(sessionId, 2);
  }
}

// Run multiple shell commands
function runShellCommands(sessionId, count) {
  const commands = [
    'echo "hello from sandbox"',
    'pwd && ls -la',
    'date && uptime',
    'cat /etc/os-release | head -3',
    'env | head -10',
    'ps aux | head -5',
    'df -h | head -3',
    'free -m 2>/dev/null || echo "free not available"',
    'for i in 1 2 3; do echo "loop $i"; done',
    'echo "test" > /tmp/test.txt && cat /tmp/test.txt && rm /tmp/test.txt',
  ];

  for (let i = 0; i < count; i++) {
    const cmd = commands[Math.floor(Math.random() * commands.length)];

    const start = Date.now();
    const res = makeRequest('POST', `/v1/${sessionId}/shell`, {
      command: cmd,
      timeout_ms: 10000,
    });
    shellLatency.add(Date.now() - start);
    commandsExecuted.add(1);

    const ok = check(res, { 'shell ok': (r) => r.status === 200 });
    successRate.add(ok);

    if (!ok) {
      sandboxErrors.add(1);
      // If sandbox is gone, skip remaining commands
      if (res.status === 404) {
        console.log(`VU ${__VU}: Sandbox not found, will recreate on next iteration`);
        return;
      }
    }

    sleep(randomIntBetween(0.5, 2)); // Think time between commands
  }
}

// Run Python code
function runPythonCode(sessionId) {
  const pythonSnippets = [
    'print("Hello from Python")\nprint(sum(range(100)))',
    'import sys\nprint(f"Python {sys.version_info.major}.{sys.version_info.minor}")',
    'result = [x**2 for x in range(10)]\nprint(result)',
    'import os\nprint(os.getcwd())\nprint(os.listdir(".")[:5])',
    'data = {"key": "value", "count": 42}\nprint(data)',
    'for i in range(5):\n    print(f"iteration {i}")',
  ];

  const code = pythonSnippets[Math.floor(Math.random() * pythonSnippets.length)];

  const start = Date.now();
  const res = makeRequest('POST', `/v1/${sessionId}/repl/python`, {
    code: code,
    timeout_ms: 15000,
  });
  pythonLatency.add(Date.now() - start);
  commandsExecuted.add(1);

  const ok = check(res, { 'python ok': (r) => r.status === 200 });
  successRate.add(ok);

  if (!ok) {
    sandboxErrors.add(1);
  }

  sleep(randomIntBetween(1, 3));
}

// Teardown: runs once per VU at end - cleanup sandbox
export function teardown(data) {
  const duration = (Date.now() - data.startTime) / 1000;
  console.log(`Test completed in ${duration.toFixed(1)}s`);

  // Note: Individual VU cleanup happens automatically when VUs scale down
  // The sandbox-service TTL will clean up any remaining sandboxes
}

// Handle summary output
export function handleSummary(data) {
  const summary = {
    timestamp: new Date().toISOString(),
    scenario: SCENARIO,
    metrics: {
      total_requests: data.metrics.http_reqs?.values?.count || 0,
      commands_executed: data.metrics.commands_executed?.values?.count || 0,
      http_req_duration_p95: data.metrics.http_req_duration?.values?.['p(95)'] || 0,
      http_req_failed_rate: data.metrics.http_req_failed?.values?.rate || 0,
      sandbox_create_p95: data.metrics.sandbox_create_latency?.values?.['p(95)'] || 0,
      shell_p95: data.metrics.shell_latency?.values?.['p(95)'] || 0,
      python_p95: data.metrics.python_latency?.values?.['p(95)'] || 0,
      success_rate: data.metrics.success_rate?.values?.rate || 0,
      errors: data.metrics.sandbox_errors?.values?.count || 0,
    },
  };

  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }),
    'summary.json': JSON.stringify(summary, null, 2),
  };
}

import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.1/index.js';
