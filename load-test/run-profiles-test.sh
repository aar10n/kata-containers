#!/bin/bash
# Profile-based Load Test Runner
# Can be run from any directory
#
# Usage: load-test/run-profiles-test.sh <scenario> [--mode pod|kata] [--watch]
#    or: ./run-profiles-test.sh <scenario> [--mode pod|kata] [--watch]
#
# Examples:
#   load-test/run-profiles-test.sh quick --watch
#   load-test/run-profiles-test.sh realistic --mode kata --watch

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
NAMESPACE="${NAMESPACE:-kata-system}"

# Defaults
SCENARIO="${1:-smoke}"
MODE=""
WATCH=false

# Parse arguments
shift 2>/dev/null || true
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            MODE="$2"
            shift 2
            ;;
        --watch)
            WATCH=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

cat <<EOF
${BLUE}============================================${NC}
  Profile-Based Sandbox Load Test
${BLUE}============================================${NC}

${GREEN}Available Scenarios:${NC}
  smoke        2 VUs, 1 minute
  quick        15 VUs, 3 minutes  <- fast iteration
  quick_stress 45 VUs, 3 minutes  <- shows contention
  light_load   23 VUs, 8 minutes
  realistic    45 VUs, 15 minutes
  worst_case   40 VUs, 8 minutes
  stress       25→125 VUs, 16 minutes

${GREEN}Modes:${NC}
  pod   - Regular K8s pods (CRI exec)
  kata  - Kata VMs (kata-agent via vsock)

EOF

# Check current sandbox-agent mode
echo -e "${BLUE}Checking sandbox-agent configuration...${NC}"
CURRENT_MODE=$(kubectl get configmap -n "$NAMESPACE" kata-deploy-sandbox-agent-config -o jsonpath='{.data.config\.yaml}' 2>/dev/null | grep -E "^mode:" | awk '{print $2}' | tr -d '"')

if [ -z "$CURRENT_MODE" ]; then
    echo -e "${RED}Error: Could not determine sandbox-agent mode${NC}"
    exit 1
fi

echo -e "Current sandbox-agent mode: ${GREEN}${CURRENT_MODE}${NC}"

# If mode specified, check it matches
if [ -n "$MODE" ]; then
    if [ "$MODE" != "$CURRENT_MODE" ]; then
        echo ""
        echo -e "${YELLOW}Mode mismatch!${NC}"
        echo -e "Requested mode: ${YELLOW}${MODE}${NC}"
        echo -e "Current mode:   ${YELLOW}${CURRENT_MODE}${NC}"
        echo ""
        echo "To switch modes, update kata-snapshot-poc-values.yaml:"
        echo -e "  ${YELLOW}sandboxAgent.mode: ${MODE}${NC}"
        echo ""
        echo "Then upgrade the helm release (from repo root):"
        echo -e "  ${YELLOW}helm upgrade kata-containers tools/packaging/kata-deploy/helm-chart/kata-deploy -n kata-system -f kata-snapshot-poc-values.yaml -f load-test/values-loadtest.yaml${NC}"
        echo ""
        read -p "Would you like me to do this now? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo ""
            echo -e "${BLUE}Updating values file...${NC}"

            # Update the mode in values file
            VALUES_FILE="$ROOT_DIR/kata-snapshot-poc-values.yaml"
            if [[ "$OSTYPE" == "darwin"* ]]; then
                sed -i.bak "s/mode: ${CURRENT_MODE}/mode: ${MODE}/" "$VALUES_FILE" && rm -f "${VALUES_FILE}.bak"
            else
                sed -i "s/mode: ${CURRENT_MODE}/mode: ${MODE}/" "$VALUES_FILE"
            fi

            echo -e "${BLUE}Upgrading helm release...${NC}"
            helm upgrade kata-containers "$ROOT_DIR/tools/packaging/kata-deploy/helm-chart/kata-deploy" \
                -n "$NAMESPACE" \
                -f "$ROOT_DIR/kata-snapshot-poc-values.yaml" \
                -f "$SCRIPT_DIR/values-loadtest.yaml"

            echo -e "${BLUE}Waiting for sandbox-agent rollout...${NC}"
            kubectl rollout status ds/kata-deploy-sandbox-agent -n "$NAMESPACE" --timeout=120s

            CURRENT_MODE="$MODE"
            echo -e "${GREEN}✓ Switched to ${MODE} mode${NC}"
        else
            echo "Aborting."
            exit 1
        fi
    else
        echo -e "${GREEN}✓ Mode matches: ${MODE}${NC}"
    fi
else
    MODE="$CURRENT_MODE"
fi

# Verify mode by checking healthz endpoint
echo ""
echo -e "${BLUE}Verifying sandbox-agent mode via healthz...${NC}"
HEALTHZ_MODE=$(kubectl exec -n "$NAMESPACE" ds/kata-deploy-sandbox-agent -- wget -q -O- http://localhost:8080/healthz 2>/dev/null | tr -d '"{} ' | grep -o 'mode:[a-z]*' | cut -d: -f2 || echo "unknown")

if [ "$HEALTHZ_MODE" != "$MODE" ] && [ "$HEALTHZ_MODE" != "unknown" ]; then
    echo -e "${YELLOW}Warning: healthz reports mode '${HEALTHZ_MODE}' but config says '${MODE}'${NC}"
    echo "The agent may need to restart. Waiting 10s..."
    sleep 10
fi

echo -e "${GREEN}✓ Running in ${MODE} mode${NC}"
echo ""

echo -e "${BLUE}--------------------------------------------${NC}"
echo -e "Scenario: ${GREEN}${SCENARIO}${NC}"
echo -e "Mode:     ${GREEN}${MODE}${NC}"
echo -e "${BLUE}--------------------------------------------${NC}"
echo ""

# Clean up any existing sandbox pods from previous tests
EXISTING_SANDBOXES=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | grep "sandbox-loadtest" | wc -l | tr -d ' ')
if [ "$EXISTING_SANDBOXES" -gt 0 ]; then
    echo -e "${YELLOW}Cleaning up $EXISTING_SANDBOXES existing test sandboxes...${NC}"
    kubectl delete pods -n "$NAMESPACE" -l sandbox.kata.io/session-id --force --grace-period=0 2>/dev/null || true
    sleep 5
fi

# Delete existing job
kubectl delete job k6-profiles-test -n "$NAMESPACE" 2>/dev/null || true
kubectl delete configmap k6-profiles-test -n "$NAMESPACE" 2>/dev/null || true

# Apply with scenario
cat "$SCRIPT_DIR/k6-profiles-job.yaml" | \
    sed "s/value: \"smoke\"/value: \"$SCENARIO\"/" | \
    kubectl apply -f -

echo -e "${GREEN}✓ Job created${NC}"
sleep 3

POD_NAME=$(kubectl get pods -n "$NAMESPACE" -l app=k6-profiles-test --no-headers -o custom-columns=":metadata.name" 2>/dev/null | head -1)
echo -e "Pod: ${GREEN}${POD_NAME}${NC}"

if [ "$WATCH" = true ]; then
    echo ""
    echo -e "${BLUE}Streaming logs (will save results when complete)...${NC}"
    kubectl logs -f "$POD_NAME" -n "$NAMESPACE" 2>/dev/null || kubectl logs -f job/k6-profiles-test -n "$NAMESPACE"

    # Save results after completion
    TIMESTAMP=$(date +%Y-%m-%d-%H%M)
    RESULTS_DIR="$SCRIPT_DIR/results"
    mkdir -p "$RESULTS_DIR"
    RESULTS_FILE="$RESULTS_DIR/${SCENARIO}-${MODE}-${TIMESTAMP}.log"
    kubectl logs job/k6-profiles-test -n "$NAMESPACE" > "$RESULTS_FILE" 2>/dev/null
    echo ""
    echo -e "${GREEN}Results saved to: ${RESULTS_FILE}${NC}"
else
    echo ""
    echo "Monitor with:"
    echo -e "  ${YELLOW}kubectl logs -f job/k6-profiles-test -n $NAMESPACE${NC}"
    echo ""
    echo "Watch sandboxes:"
    echo -e "  ${YELLOW}watch 'kubectl get pods -n $NAMESPACE | grep sandbox-loadtest'${NC}"
    echo ""
    echo "Save results when done (from repo root):"
    echo -e "  ${YELLOW}kubectl logs job/k6-profiles-test -n $NAMESPACE > load-test/results/${SCENARIO}-${MODE}-\$(date +%Y-%m-%d).log${NC}"
fi
