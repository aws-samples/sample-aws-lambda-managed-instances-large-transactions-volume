#!/usr/bin/env bash
# cleanup.sh — Delete both CloudFormation stacks AND the retained bookings
# DDB table so the account is left in a clean state for a fresh redeploy.
#
# The template keeps `DeletionPolicy: Retain` on the bookings table as a
# safety net against accidental `sam delete`. This script is the authorised
# full-clean path — it requires an explicit "yes" confirmation.
#
# Usage:
#   ./scripts/cleanup.sh [region]
#
# Set KEEP_BOOKINGS=1 to skip the bookings-table delete step (leaves the
# retained table in place; redeploy will fail with `AlreadyExists` until
# you drop it manually).

set -euo pipefail

REGION="${1:-us-east-1}"
TICKETING_STACK="ticketing-lmi"
SIMULATION_STACK="ticketing-simulation"
BOOKINGS_TABLE="${TICKETING_STACK}-bookings"

echo "WARNING: This will delete both AWS CloudFormation stacks AND their data."
echo "  - Stack:  ${SIMULATION_STACK}"
echo "  - Stack:  ${TICKETING_STACK}"
if [[ "${KEEP_BOOKINGS:-0}" != "1" ]]; then
  echo "  - Table:  ${BOOKINGS_TABLE}  (retained by CFN; this script drops it)"
fi
echo ""
read -rp "Type 'yes' to confirm: " CONFIRM

if [[ "$CONFIRM" != "yes" ]]; then
  echo "Aborted."
  exit 0
fi

echo "Deleting simulation stack..."
aws cloudformation delete-stack --stack-name "$SIMULATION_STACK" --region "$REGION"
aws cloudformation wait stack-delete-complete --stack-name "$SIMULATION_STACK" --region "$REGION"
echo "Simulation stack deleted."

# Before deleting the ticketing stack, deactivate all functions (set EEs to 0)
# so the capacity provider can be deleted.
echo ""
echo "Deactivating AWS Lambda functions (setting execution environments to 0)..."
python3 - "$TICKETING_STACK" "$REGION" <<'PYEOF'
import sys
import boto3
from botocore.exceptions import ClientError

stack, region = sys.argv[1:]
client = boto3.client("lambda", region_name=region)

for func in ["queue-position", "inventory-lock", "booking-confirm"]:
    func_name = f"{stack}-{func}"
    try:
        client.put_function_scaling_config(
            FunctionName=func_name,
            Qualifier="$LATEST.PUBLISHED",
            FunctionScalingConfig={
                "MinExecutionEnvironments": 0,
                "MaxExecutionEnvironments": 0,
            },
        )
        print(f"  Deactivated {func_name}")
    except ClientError:
        print(f"  Skipped {func_name} (may not exist or already deactivated)")
PYEOF

echo "Waiting 60 seconds for execution environments to drain..."
sleep 60

echo "Deleting ticketing stack..."
aws cloudformation delete-stack --stack-name "$TICKETING_STACK" --region "$REGION"
aws cloudformation wait stack-delete-complete --stack-name "$TICKETING_STACK" --region "$REGION"
echo "Ticketing stack deleted."

# ---------------------------------------------------------------------------
# Drop the retained bookings table so the next `sam deploy` starts fresh.
# The inventory-sharded table has DeletionPolicy: Delete and went with the
# stack; only the bookings table (Retain) survives on its own.
# ---------------------------------------------------------------------------
if [[ "${KEEP_BOOKINGS:-0}" == "1" ]]; then
  echo ""
  echo "KEEP_BOOKINGS=1 — leaving ${BOOKINGS_TABLE} in place."
  echo "Note: redeploy will fail with AlreadyExists until you drop it manually:"
  echo "  aws dynamodb delete-table --table-name ${BOOKINGS_TABLE} --region ${REGION}"
else
  echo ""
  echo "Deleting retained bookings table (${BOOKINGS_TABLE})..."
  if aws dynamodb describe-table --table-name "$BOOKINGS_TABLE" --region "$REGION" >/dev/null 2>&1; then
    aws dynamodb delete-table --table-name "$BOOKINGS_TABLE" --region "$REGION" \
      --query 'TableDescription.[TableName,TableStatus]' --output text
    aws dynamodb wait table-not-exists --table-name "$BOOKINGS_TABLE" --region "$REGION"
    echo "Bookings table deleted."
  else
    echo "Bookings table not found — nothing to delete."
  fi
fi

echo ""
echo "Cleanup complete. Account is clean for a fresh \`sam deploy\`."
