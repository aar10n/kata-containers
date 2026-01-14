// k6 Load Test with User Profiles
//
// Profiles define distinct usage patterns that can be mixed to simulate
// different load scenarios (light, realistic, worst-case, etc.)
//
// Usage:
//   k6 run -e SCENARIO=realistic k6-profiles.js
//   k6 run -e SCENARIO=worst_case k6-profiles.js
//   k6 run -e SCENARIO=light_load k6-profiles.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter, Rate } from 'k6/metrics';
import { SharedArray } from 'k6/data';

// ============================================================================
// METRICS
// ============================================================================
const sandboxCreateLatency = new Trend('sandbox_create_latency', true);
const commandLatency = new Trend('command_latency', true);
const pythonLatency = new Trend('python_latency', true);
const errors = new Counter('errors');
const successRate = new Rate('success_rate');
const commandsExecuted = new Counter('commands_executed');

// ============================================================================
// CONFIGURATION
// ============================================================================
const BASE_URL = __ENV.BASE_URL || 'http://kata-deploy-sandbox-service.kata-system:8080';
const SCENARIO = __ENV.SCENARIO || 'realistic';

// ============================================================================
// USER PROFILES
// ============================================================================
// Each profile defines a distinct usage pattern

const PROFILES = {
  // Occasional user: runs one command every 15-30 seconds
  // Simulates: monitoring scripts, cron jobs, light interactive use
  occasional: {
    weight: 1,
    commandInterval: [15, 30],  // seconds between commands
    commandsPerBatch: 1,
    pythonProbability: 0.1,    // 10% chance of Python vs shell
    pythonComplexity: 'light',
  },

  // Steady user: runs commands consistently every 3-6 seconds
  // Simulates: active development, debugging sessions
  steady: {
    weight: 1,
    commandInterval: [3, 6],
    commandsPerBatch: [2, 4],  // can be range
    pythonProbability: 0.3,
    pythonComplexity: 'light',
  },

  // Light Python user: occasional light Python execution
  // Simulates: data exploration, simple scripting
  light_python: {
    weight: 1,
    commandInterval: [5, 10],
    commandsPerBatch: 1,
    pythonProbability: 0.8,
    pythonComplexity: 'light',
  },

  // Heavy Python user: frequent intensive Python execution
  // Simulates: ML training, data processing, compute-heavy tasks
  heavy_python: {
    weight: 1,
    commandInterval: [8, 15],
    commandsPerBatch: 1,
    pythonProbability: 0.9,
    pythonComplexity: 'heavy',
  },

  // Bursty user: rapid commands in bursts, then pauses
  // Simulates: copy-paste workflows, batch operations
  bursty: {
    weight: 1,
    commandInterval: [0.5, 1],  // fast during burst
    commandsPerBatch: [5, 10],
    burstPause: [10, 20],       // pause between bursts
    pythonProbability: 0.2,
    pythonComplexity: 'light',
  },
};

// ============================================================================
// SCENARIO CONFIGURATIONS
// ============================================================================
// Define different mixes of profiles for various test scenarios

const SCENARIOS = {
  // Quick smoke test
  smoke: {
    profiles: { steady: 2 },
    duration: '1m',
  },

  // Quick iteration test: 3 min total, 15 VUs
  // Good balance of load and speed for testing improvements
  quick: {
    profiles: {
      occasional: 3,
      steady: 5,
      light_python: 4,
      heavy_python: 2,
      bursty: 1,
    },
    rampUp: '30s',
    hold: '2m',
    rampDown: '30s',
  },

  // Quick stress test: 3 min total, 45 VUs
  // Same VU count as realistic but compressed - shows contention quickly
  quick_stress: {
    profiles: {
      occasional: 10,
      steady: 15,
      light_python: 10,
      heavy_python: 5,
      bursty: 5,
    },
    rampUp: '30s',
    hold: '2m',
    rampDown: '30s',
  },

  // Light load: mostly occasional users
  light_load: {
    profiles: {
      occasional: 15,
      steady: 5,
      light_python: 3,
    },
    rampUp: '2m',
    hold: '5m',
    rampDown: '1m',
  },

  // Realistic mix: typical production distribution
  realistic: {
    profiles: {
      occasional: 10,
      steady: 15,
      light_python: 10,
      heavy_python: 5,
      bursty: 5,
    },
    rampUp: '3m',
    hold: '10m',
    rampDown: '2m',
  },

  // Worst case: all heavy users
  worst_case: {
    profiles: {
      heavy_python: 20,
      bursty: 10,
      steady: 10,
    },
    rampUp: '2m',
    hold: '5m',
    rampDown: '1m',
  },

  // Stress test: ramp up to find breaking point
  stress: {
    profiles: {
      steady: 50,
      heavy_python: 25,
      bursty: 25,
    },
    stages: [
      { duration: '2m', target: 0.25 },   // 25% of profile counts
      { duration: '3m', target: 0.5 },    // 50%
      { duration: '3m', target: 0.75 },   // 75%
      { duration: '3m', target: 1.0 },    // 100%
      { duration: '3m', target: 1.25 },   // 125% (overshoot)
      { duration: '2m', target: 0 },
    ],
  },

  // Spike test: sudden burst of users
  spike: {
    profiles: {
      steady: 20,
      light_python: 10,
      bursty: 20,
    },
    stages: [
      { duration: '1m', target: 0.2 },
      { duration: '30s', target: 1.0 },   // sudden spike
      { duration: '3m', target: 1.0 },
      { duration: '30s', target: 0.2 },
      { duration: '1m', target: 0.2 },
      { duration: '30s', target: 0 },
    ],
  },

  // Soak test: sustained load for long duration
  soak: {
    profiles: {
      occasional: 20,
      steady: 15,
      light_python: 10,
      heavy_python: 5,
    },
    rampUp: '2m',
    hold: '30m',
    rampDown: '2m',
  },
};

// ============================================================================
// BUILD K6 OPTIONS FROM SCENARIO
// ============================================================================

function buildOptions() {
  const scenario = SCENARIOS[SCENARIO];
  if (!scenario) {
    throw new Error(`Unknown scenario: ${SCENARIO}. Available: ${Object.keys(SCENARIOS).join(', ')}`);
  }

  const totalVUs = Object.values(scenario.profiles).reduce((a, b) => a + b, 0);

  let stages;
  if (scenario.stages) {
    // Use explicit stages with scaling
    stages = scenario.stages.map(s => ({
      duration: s.duration,
      target: Math.round(totalVUs * s.target),
    }));
  } else {
    // Build stages from rampUp/hold/rampDown
    stages = [
      { duration: scenario.rampUp || '1m', target: totalVUs },
      { duration: scenario.hold || '5m', target: totalVUs },
      { duration: scenario.rampDown || '1m', target: 0 },
    ];
  }

  // For smoke test, use simpler config
  if (scenario.duration) {
    return {
      scenarios: {
        default: {
          executor: 'constant-vus',
          vus: totalVUs,
          duration: scenario.duration,
        },
      },
      thresholds: {
        http_req_failed: ['rate<0.1'],
        success_rate: ['rate>0.9'],
      },
    };
  }

  return {
    scenarios: {
      default: {
        executor: 'ramping-vus',
        startVUs: 0,
        stages: stages,
        gracefulRampDown: '30s',
      },
    },
    thresholds: {
      http_req_duration: ['p(95)<5000', 'p(99)<10000'],
      http_req_failed: ['rate<0.05'],
      sandbox_create_latency: ['p(95)<15000'],
      command_latency: ['p(95)<3000'],
      python_latency: ['p(95)<5000'],
      success_rate: ['rate>0.95'],
    },
  };
}

export const options = buildOptions();

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

function randomBetween(min, max) {
  if (Array.isArray(min)) {
    [min, max] = min;
  }
  return Math.random() * (max - min) + min;
}

function randomInt(min, max) {
  return Math.floor(randomBetween(min, max + 1));
}

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

// Assign each VU to a profile based on scenario distribution
function getProfileForVU(vuId) {
  const scenario = SCENARIOS[SCENARIO];
  const profiles = scenario.profiles;

  // Build cumulative distribution
  let cumulative = 0;
  const distribution = [];
  for (const [name, count] of Object.entries(profiles)) {
    cumulative += count;
    distribution.push({ name, threshold: cumulative });
  }

  // Map VU to profile (deterministic based on VU ID)
  const total = cumulative;
  const position = ((vuId - 1) % total) + 1;

  for (const { name, threshold } of distribution) {
    if (position <= threshold) {
      return name;
    }
  }

  return Object.keys(profiles)[0]; // fallback
}

// ============================================================================
// COMMAND GENERATORS
// ============================================================================

const SHELL_COMMANDS = [
  'echo "hello"',
  'pwd',
  'ls -la',
  'date',
  'uptime',
  'cat /etc/os-release | head -3',
  'env | head -5',
  'ps aux | head -3',
  'df -h | head -2',
  'whoami && id',
];

const LIGHT_PYTHON = [
  'print("Hello, World!")',
  'print(sum(range(100)))',
  'import sys; print(sys.version)',
  'result = [x**2 for x in range(10)]; print(result)',
  'import os; print(os.getcwd())',
  'data = {"key": "value"}; print(data)',
  'for i in range(5): print(f"i={i}")',
  'print("\\n".join(f"Line {i}" for i in range(3)))',
];

const HEAVY_PYTHON = [
  // CPU-bound computation
  `
import time
start = time.time()
result = sum(i**2 for i in range(100000))
elapsed = time.time() - start
print(f"Computed {result} in {elapsed:.3f}s")
`,
  // Memory allocation
  `
data = [list(range(1000)) for _ in range(100)]
print(f"Created {len(data)} lists with {len(data[0])} items each")
total = sum(sum(row) for row in data)
print(f"Total sum: {total}")
`,
  // String processing
  `
import string
text = string.ascii_letters * 1000
words = [text[i:i+10] for i in range(0, len(text), 10)]
print(f"Processed {len(words)} words")
unique = len(set(words))
print(f"Unique words: {unique}")
`,
  // List comprehensions and sorting
  `
import random
data = [random.randint(1, 10000) for _ in range(10000)]
sorted_data = sorted(data)
print(f"Sorted {len(data)} numbers")
print(f"Min: {sorted_data[0]}, Max: {sorted_data[-1]}")
`,
  // Nested loops
  `
result = 0
for i in range(100):
    for j in range(100):
        result += i * j
print(f"Nested loop result: {result}")
`,
];

function runShellCommand(sessionId) {
  const cmd = SHELL_COMMANDS[randomInt(0, SHELL_COMMANDS.length - 1)];
  const start = Date.now();
  const res = makeRequest('POST', `/v1/${sessionId}/shell`, {
    command: cmd,
    timeout_ms: 10000,
  });
  commandLatency.add(Date.now() - start);
  commandsExecuted.add(1);

  const ok = check(res, { 'shell ok': (r) => r.status === 200 });
  successRate.add(ok);
  if (!ok) errors.add(1);

  return ok;
}

function runPythonCode(sessionId, complexity) {
  const snippets = complexity === 'heavy' ? HEAVY_PYTHON : LIGHT_PYTHON;
  const code = snippets[randomInt(0, snippets.length - 1)];

  const start = Date.now();
  const res = makeRequest('POST', `/v1/${sessionId}/repl/python`, {
    code: code,
    timeout_ms: complexity === 'heavy' ? 30000 : 15000,
  });
  pythonLatency.add(Date.now() - start);
  commandsExecuted.add(1);

  const ok = check(res, { 'python ok': (r) => r.status === 200 });
  successRate.add(ok);
  if (!ok) errors.add(1);

  return ok;
}

// ============================================================================
// PROFILE EXECUTORS
// ============================================================================

function executeOccasional(sessionId, profile) {
  // Run one command, then long pause
  if (Math.random() < profile.pythonProbability) {
    runPythonCode(sessionId, profile.pythonComplexity);
  } else {
    runShellCommand(sessionId);
  }
  sleep(randomBetween(profile.commandInterval));
}

function executeSteady(sessionId, profile) {
  // Run a batch of commands with short pauses
  const batchSize = Array.isArray(profile.commandsPerBatch)
    ? randomInt(...profile.commandsPerBatch)
    : profile.commandsPerBatch;

  for (let i = 0; i < batchSize; i++) {
    if (Math.random() < profile.pythonProbability) {
      runPythonCode(sessionId, profile.pythonComplexity);
    } else {
      runShellCommand(sessionId);
    }
    sleep(randomBetween(1, 2)); // short pause between commands in batch
  }
  sleep(randomBetween(profile.commandInterval));
}

function executeLightPython(sessionId, profile) {
  if (Math.random() < profile.pythonProbability) {
    runPythonCode(sessionId, 'light');
  } else {
    runShellCommand(sessionId);
  }
  sleep(randomBetween(profile.commandInterval));
}

function executeHeavyPython(sessionId, profile) {
  if (Math.random() < profile.pythonProbability) {
    runPythonCode(sessionId, 'heavy');
  } else {
    runShellCommand(sessionId);
  }
  sleep(randomBetween(profile.commandInterval));
}

function executeBursty(sessionId, profile) {
  // Run a burst of fast commands
  const burstSize = Array.isArray(profile.commandsPerBatch)
    ? randomInt(...profile.commandsPerBatch)
    : profile.commandsPerBatch;

  for (let i = 0; i < burstSize; i++) {
    if (Math.random() < profile.pythonProbability) {
      runPythonCode(sessionId, profile.pythonComplexity);
    } else {
      runShellCommand(sessionId);
    }
    sleep(randomBetween(profile.commandInterval)); // fast
  }

  // Long pause between bursts
  sleep(randomBetween(profile.burstPause));
}

const PROFILE_EXECUTORS = {
  occasional: executeOccasional,
  steady: executeSteady,
  light_python: executeLightPython,
  heavy_python: executeHeavyPython,
  bursty: executeBursty,
};

// ============================================================================
// MAIN TEST FUNCTION
// ============================================================================

export function setup() {
  const scenario = SCENARIOS[SCENARIO];
  const totalVUs = Object.values(scenario.profiles).reduce((a, b) => a + b, 0);

  console.log(`\n========================================`);
  console.log(`Load Test: ${SCENARIO}`);
  console.log(`Target: ${BASE_URL}`);
  console.log(`Total VUs: ${totalVUs}`);
  console.log(`Profile distribution:`);
  for (const [name, count] of Object.entries(scenario.profiles)) {
    console.log(`  - ${name}: ${count} VUs`);
  }
  console.log(`========================================\n`);

  // Health check
  const res = http.get(`${BASE_URL}/healthz`, { timeout: '5s' });
  if (res.status !== 200) {
    throw new Error(`Service not healthy: ${res.status}`);
  }
  console.log('Service health check passed');

  return { startTime: Date.now() };
}

export default function(data) {
  const sessionId = `loadtest-vu${__VU}`;
  const profileName = getProfileForVU(__VU);
  const profile = PROFILES[profileName];

  // First iteration: create sandbox
  if (__ITER === 0) {
    console.log(`VU ${__VU}: Profile=${profileName}`);

    const start = Date.now();
    // Use absolute path for kata mode compatibility
    const res = makeRequest('POST', `/v1/${sessionId}/exec`, {
      command: ['/bin/echo', 'sandbox-ready'],
      timeout_ms: 20000,
    });
    sandboxCreateLatency.add(Date.now() - start);

    const ok = check(res, { 'sandbox created': (r) => r.status === 200 });
    successRate.add(ok);

    if (!ok) {
      errors.add(1);
      console.log(`VU ${__VU}: Failed to create sandbox: ${res.status}`);
      sleep(5);
      return;
    }
    sleep(1);
  }

  // Execute profile behavior
  const executor = PROFILE_EXECUTORS[profileName];
  if (executor) {
    executor(sessionId, profile);
  } else {
    console.log(`Unknown profile: ${profileName}`);
    sleep(5);
  }
}

export function teardown(data) {
  const duration = (Date.now() - data.startTime) / 1000;
  console.log(`\nTest completed in ${duration.toFixed(1)}s`);
}

// ============================================================================
// SUMMARY
// ============================================================================

export function handleSummary(data) {
  const scenario = SCENARIOS[SCENARIO];
  const summary = {
    timestamp: new Date().toISOString(),
    scenario: SCENARIO,
    profiles: scenario.profiles,
    metrics: {
      total_requests: data.metrics.http_reqs?.values?.count || 0,
      commands_executed: data.metrics.commands_executed?.values?.count || 0,
      errors: data.metrics.errors?.values?.count || 0,
      success_rate: data.metrics.success_rate?.values?.rate || 0,
      sandbox_create_p95: data.metrics.sandbox_create_latency?.values?.['p(95)'] || 0,
      command_p95: data.metrics.command_latency?.values?.['p(95)'] || 0,
      python_p95: data.metrics.python_latency?.values?.['p(95)'] || 0,
    },
  };

  // Build text summary
  let text = `
================================================================================
LOAD TEST RESULTS: ${SCENARIO}
================================================================================

Profile Distribution:
${Object.entries(scenario.profiles).map(([n, c]) => `  ${n}: ${c} VUs`).join('\n')}

Key Metrics:
  Commands Executed:     ${summary.metrics.commands_executed}
  Success Rate:          ${(summary.metrics.success_rate * 100).toFixed(1)}%
  Errors:                ${summary.metrics.errors}

Latencies (p95):
  Sandbox Creation:      ${summary.metrics.sandbox_create_p95.toFixed(0)}ms
  Shell Commands:        ${summary.metrics.command_p95.toFixed(0)}ms
  Python Execution:      ${summary.metrics.python_p95.toFixed(0)}ms

================================================================================
`;

  return {
    stdout: text,
    'summary.json': JSON.stringify(summary, null, 2),
  };
}
