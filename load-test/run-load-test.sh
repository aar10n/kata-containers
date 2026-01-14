#!/bin/bash
# Load Test Runner for Sandbox Service
# Usage: ./run-load-test.sh [scenario] [options]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${NAMESPACE:-kata-system}"
SERVICE_URL="${SERVICE_URL:-http://kata-deploy-sandbox-service.kata-system:8080}"

# Default scenario
SCENARIO="${1:-smoke}"
shift 2>/dev/null || true

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

usage() {
    cat <<EOF
Usage: $0 <scenario> [options]

Scenarios:
  smoke      Quick 1-minute test with 2 VUs (default)
  standard   15-minute test ramping from 10 to 50 VUs
  stress     16-minute test ramping up to 200 VUs
  spike      5-minute test with sudden spike to 100 VUs
  soak       30-minute test with constant 30 VUs

Options:
  --local         Run k6 locally (requires k6 installed)
  --port-forward  Run locally with port-forward to cluster
  --watch         Stream logs while test runs
  --cleanup       Delete previous test jobs before starting

Examples:
  $0 smoke                    # Quick smoke test in cluster
  $0 standard --watch         # Standard test with live logs
  $0 stress --local           # Stress test running k6 locally
  $0 spike --port-forward     # Spike test via port-forward
EOF
}

# Parse options
LOCAL=false
PORT_FORWARD=false
WATCH=false
CLEANUP=false

for arg in "$@"; do
    case $arg in
        --local) LOCAL=true ;;
        --port-forward) PORT_FORWARD=true; LOCAL=true ;;
        --watch) WATCH=true ;;
        --cleanup) CLEANUP=true ;;
        --help|-h) usage; exit 0 ;;
    esac
done

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  Sandbox Service Load Test${NC}"
echo -e "${BLUE}============================================${NC}"
echo -e "Scenario:  ${GREEN}${SCENARIO}${NC}"
echo -e "Namespace: ${NAMESPACE}"
echo ""

# Cleanup previous runs if requested
if [ "$CLEANUP" = true ]; then
    echo -e "${YELLOW}Cleaning up previous test runs...${NC}"
    kubectl delete job k6-load-test -n "$NAMESPACE" 2>/dev/null || true
    kubectl delete configmap k6-load-test -n "$NAMESPACE" 2>/dev/null || true
fi

# Check if service is healthy
echo -e "${BLUE}Checking sandbox-service health...${NC}"
if kubectl exec -n "$NAMESPACE" deploy/kata-deploy-sandbox-service -- wget -q -O- http://localhost:8080/healthz >/dev/null 2>&1; then
    echo -e "${GREEN}✓ Service is healthy${NC}"
else
    echo -e "${YELLOW}⚠ Could not verify service health (may still work)${NC}"
fi

# Count existing sandbox pods
SANDBOX_COUNT=$(kubectl get pods -n "$NAMESPACE" -l sandbox.kata.io/session-id --no-headers 2>/dev/null | wc -l | tr -d ' ')
echo -e "Current sandbox pods: ${SANDBOX_COUNT}"
echo ""

if [ "$LOCAL" = true ]; then
    # Run k6 locally
    if ! command -v k6 &>/dev/null; then
        echo -e "${RED}Error: k6 is not installed${NC}"
        echo "Install with: brew install k6"
        exit 1
    fi

    if [ "$PORT_FORWARD" = true ]; then
        echo -e "${BLUE}Starting port-forward...${NC}"
        kubectl port-forward -n "$NAMESPACE" svc/kata-deploy-sandbox-service 8080:8080 &
        PF_PID=$!
        trap "kill $PF_PID 2>/dev/null" EXIT
        sleep 2
        SERVICE_URL="http://localhost:8080"
    fi

    echo -e "${BLUE}Running k6 locally...${NC}"
    echo -e "Target: ${SERVICE_URL}"
    echo ""

    k6 run \
        -e BASE_URL="$SERVICE_URL" \
        -e SCENARIO="$SCENARIO" \
        "$SCRIPT_DIR/k6-sandbox-test.js"
else
    # Run k6 as Kubernetes Job
    echo -e "${BLUE}Deploying k6 load test job...${NC}"

    # Delete existing job if present
    kubectl delete job k6-load-test -n "$NAMESPACE" 2>/dev/null || true

    # Create ConfigMap with test script
    kubectl delete configmap k6-load-test -n "$NAMESPACE" 2>/dev/null || true

    # Apply the job manifest with scenario override
    cat "$SCRIPT_DIR/k6-job.yaml" | \
        sed "s/value: \"smoke\"/value: \"$SCENARIO\"/" | \
        kubectl apply -f -

    echo -e "${GREEN}✓ Job created${NC}"
    echo ""

    # Get job pod name
    echo -e "${BLUE}Waiting for pod to start...${NC}"
    sleep 3

    POD_NAME=""
    for i in {1..30}; do
        POD_NAME=$(kubectl get pods -n "$NAMESPACE" -l app=k6-load-test --no-headers -o custom-columns=":metadata.name" 2>/dev/null | head -1)
        if [ -n "$POD_NAME" ]; then
            break
        fi
        sleep 1
    done

    if [ -z "$POD_NAME" ]; then
        echo -e "${RED}Error: Could not find k6 pod${NC}"
        exit 1
    fi

    echo -e "Pod: ${GREEN}${POD_NAME}${NC}"

    # Wait for pod to be running
    kubectl wait --for=condition=Ready pod/"$POD_NAME" -n "$NAMESPACE" --timeout=60s 2>/dev/null || true

    if [ "$WATCH" = true ]; then
        echo ""
        echo -e "${BLUE}Streaming logs (Ctrl+C to stop watching)...${NC}"
        echo -e "${YELLOW}----------------------------------------${NC}"
        kubectl logs -f "$POD_NAME" -n "$NAMESPACE"
    else
        echo ""
        echo -e "${GREEN}Test running in background.${NC}"
        echo ""
        echo "Monitor with:"
        echo -e "  ${YELLOW}kubectl logs -f $POD_NAME -n $NAMESPACE${NC}"
        echo ""
        echo "Check status:"
        echo -e "  ${YELLOW}kubectl get job k6-load-test -n $NAMESPACE${NC}"
        echo ""
        echo "Watch sandbox pods:"
        echo -e "  ${YELLOW}watch kubectl get pods -n $NAMESPACE -l sandbox.kata.io/session-id${NC}"
    fi
fi
