#!/usr/bin/env bash
# scale-for-onsale.sh — Manually adjust LMI execution-environment bounds
# on all three ticketing handlers.
#
# Use this before a scheduled on-sale to pre-warm capacity, and after to
# scale back down. It calls put-function-scaling-config on all three
# functions against the $LATEST.PUBLISHED qualifier — matching what the
# ticketing template configures at deploy time.
#
# Requires AWS CLI v2.22+ with the `put-function-scaling-config` command.
# Older CLI versions can call the same API via boto3 directly:
#   python3 -c "import boto3; boto3.client('lambda').put_function_scaling_config(...)"
#
# Usage:
#   ./scripts/scale-for-onsale.sh <region> <min-ees> <max-ees>
#
# Examples:
#   # Pre-warm to peak for a big on-sale
#   ./scripts/scale-for-onsale.sh us-east-1 500 2000
#
#   # Scale back down after the window closes
#   ./scripts/scale-for-onsale.sh us-east-1 25 200

set -euo pipefail

REGION="${1:-us-east-1}"
MIN_EES="${2:-}"
MAX_EES="${3:-}"
STACK_PREFIX="${STACK_PREFIX:-ticketing-lmi}"

if [[ -z "$MIN_EES" || -z "$MAX_EES" ]]; then
  echo "ERROR: min-ees and max-ees are required."
  echo "Usage: $0 <region> <min-ees> <max-ees>"
  exit 1
fi

FUNCTIONS=(
  "${STACK_PREFIX}-queue-position"
  "${STACK_PREFIX}-inventory-lock"
  "${STACK_PREFIX}-booking-confirm"
)

echo "Applying scaling config to \$LATEST.PUBLISHED on ${#FUNCTIONS[@]} functions..."
echo "  region: ${REGION}"
echo "  min:    ${MIN_EES}"
echo "  max:    ${MAX_EES}"
echo ""

for FUNC in "${FUNCTIONS[@]}"; do
  echo "  ${FUNC}"
  aws lambda put-function-scaling-config \
    --function-name "$FUNC" \
    --qualifier '$LATEST.PUBLISHED' \
    --function-scaling-config "MinExecutionEnvironments=${MIN_EES},MaxExecutionEnvironments=${MAX_EES}" \
    --region "$REGION" \
    --output text \
    --query 'FunctionScalingConfig.[MinExecutionEnvironments,MaxExecutionEnvironments]'
done

echo ""
echo "Done. Execution environments will resize over the next 1-3 minutes."
echo "Watch the Lambda console (each function's Monitor tab) or the LMI Capacity"
echo "Provider console to confirm the fleet has scaled to the new size:"
echo "  https://${REGION}.console.aws.amazon.com/lambda/home?region=${REGION}#/capacity-providers/${STACK_PREFIX}-ticketing-cp"
