#!/usr/bin/env bash
# scale-for-onsale.sh — Manually adjust LMI execution-environment bounds
# on the three ticketing handlers, per-handler.
#
# Use this before a scheduled on-sale to pre-warm capacity, and after to
# scale back down. It calls put-function-scaling-config on all three
# functions against the $LATEST.PUBLISHED qualifier — matching what the
# ticketing template configures at deploy time.
#
# Each handler takes its own min:max pair, so queue-position (lightest)
# can be sized differently from booking-confirm (heaviest). All three
# flags are required on every call, including scale-down.
#
# Requires AWS CLI v2.22+ with the `put-function-scaling-config` command.
# Older CLI versions can call the same API via boto3 directly:
#   python3 -c "import boto3; boto3.client('lambda').put_function_scaling_config(...)"
#
# Usage:
#   ./scripts/scale-for-onsale.sh \
#     --region <aws-region> \
#     --queue-position <min>:<max> \
#     --inventory-lock <min>:<max> \
#     --booking-confirm <min>:<max>
#
# Examples:
#   # Pre-warm for the reference 40K TPS × 120 s run (300 EEs total)
#   ./scripts/scale-for-onsale.sh \
#     --region us-east-1 \
#     --queue-position 50:100 \
#     --inventory-lock 100:200 \
#     --booking-confirm 150:300
#
#   # Scale back down to the deploy defaults after the window closes
#   ./scripts/scale-for-onsale.sh \
#     --region us-east-1 \
#     --queue-position 1:3 \
#     --inventory-lock 1:3 \
#     --booking-confirm 1:3

set -euo pipefail

STACK_PREFIX="${STACK_PREFIX:-ticketing-lmi}"

usage() {
  cat <<'EOF'
Usage:
  scale-for-onsale.sh \
    --region <aws-region> \
    --queue-position <min>:<max> \
    --inventory-lock <min>:<max> \
    --booking-confirm <min>:<max>

All four flags are required. <min>:<max> must be non-negative integers
with min <= max. Flag order does not matter.
EOF
}

die() {
  echo "ERROR: $*" >&2
  echo "" >&2
  usage >&2
  exit 1
}

# Parse a "<min>:<max>" value for a given flag name into globals MIN and MAX.
parse_range() {
  local flag="$1" value="$2"
  if [[ ! "$value" =~ ^[0-9]+:[0-9]+$ ]]; then
    die "${flag} value '${value}' is not in <min>:<max> form with non-negative integers."
  fi
  MIN="${value%:*}"
  MAX="${value#*:}"
  if (( MIN > MAX )); then
    die "${flag} min (${MIN}) must be <= max (${MAX})."
  fi
}

REGION=""
QP_RANGE=""
IL_RANGE=""
BC_RANGE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)
      [[ $# -ge 2 ]] || die "--region requires a value."
      [[ -z "$REGION" ]] || die "--region specified more than once."
      REGION="$2"
      shift 2
      ;;
    --queue-position)
      [[ $# -ge 2 ]] || die "--queue-position requires a <min>:<max> value."
      [[ -z "$QP_RANGE" ]] || die "--queue-position specified more than once."
      QP_RANGE="$2"
      shift 2
      ;;
    --inventory-lock)
      [[ $# -ge 2 ]] || die "--inventory-lock requires a <min>:<max> value."
      [[ -z "$IL_RANGE" ]] || die "--inventory-lock specified more than once."
      IL_RANGE="$2"
      shift 2
      ;;
    --booking-confirm)
      [[ $# -ge 2 ]] || die "--booking-confirm requires a <min>:<max> value."
      [[ -z "$BC_RANGE" ]] || die "--booking-confirm specified more than once."
      BC_RANGE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

[[ -n "$REGION"   ]] || die "--region is required."
[[ -n "$QP_RANGE" ]] || die "--queue-position is required."
[[ -n "$IL_RANGE" ]] || die "--inventory-lock is required."
[[ -n "$BC_RANGE" ]] || die "--booking-confirm is required."

parse_range "--queue-position"  "$QP_RANGE"; QP_MIN="$MIN"; QP_MAX="$MAX"
parse_range "--inventory-lock"  "$IL_RANGE"; IL_MIN="$MIN"; IL_MAX="$MAX"
parse_range "--booking-confirm" "$BC_RANGE"; BC_MIN="$MIN"; BC_MAX="$MAX"

# Three parallel arrays: (function name, min, max) per handler. Function
# names follow the same STACK_PREFIX-<handler> convention the template
# uses at deploy time.
HANDLERS=(
  "${STACK_PREFIX}-queue-position"
  "${STACK_PREFIX}-inventory-lock"
  "${STACK_PREFIX}-booking-confirm"
)
MINS=("$QP_MIN" "$IL_MIN" "$BC_MIN")
MAXS=("$QP_MAX" "$IL_MAX" "$BC_MAX")

echo "Applying per-handler scaling config to \$LATEST.PUBLISHED on ${#HANDLERS[@]} functions..."
echo "  region:          ${REGION}"
echo "  queue-position:  ${QP_MIN}:${QP_MAX}"
echo "  inventory-lock:  ${IL_MIN}:${IL_MAX}"
echo "  booking-confirm: ${BC_MIN}:${BC_MAX}"
echo ""

for i in "${!HANDLERS[@]}"; do
  FUNC="${HANDLERS[$i]}"
  MIN_EES="${MINS[$i]}"
  MAX_EES="${MAXS[$i]}"
  echo "  ${FUNC} (${MIN_EES}:${MAX_EES})"
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
