#!/usr/bin/env bash
# deploy.sh — Deploy both apps in order.
# Captures VpcId and SubnetIds, writes them into samconfig.toml for both
# apps, then runs sam build + sam deploy.  After this script runs you can
# also run `sam deploy` directly from either app directory.
#
# The ticketing app's front-end is API Gateway. AWS enforces an account-
# level rate limit on API Gateway (default 10K rps, quota code L-8A5B8E43);
# request an increase via Service Quotas before running above ~8K TPS.
#
# Usage:
#   ./scripts/deploy.sh <vpc-id> <subnet-ids> [region]
#
# Examples:
#   ./scripts/deploy.sh vpc-0abc123 "subnet-0aaa111,subnet-0bbb222"
#   ./scripts/deploy.sh vpc-0abc123 "subnet-0aaa,subnet-0bbb,subnet-0ccc" us-west-2
#
# Optional environment variables:
#   BUDGET_EMAIL=<address>   Enable the ticketing-app's AWS::Budgets::Budget
#                            alarm. When set, the deploy creates a Budget
#                            resource that emails this address at 80% / 100%
#                            of BudgetMonthlyUsd (default $100). Unset = no
#                            alarm (the stack still deploys fine).

set -euo pipefail

VPC_ID="${1:-}"
SUBNET_IDS="${2:-}"
REGION="${3:-us-east-1}"
BUDGET_EMAIL="${BUDGET_EMAIL:-}"

# Strip all whitespace from the subnet list. SAM's parameter_overrides parses on
# spaces, so "subnet-a, subnet-b" would silently drop subnet-b.
SUBNET_IDS="${SUBNET_IDS//[[:space:]]/}"

if [[ -z "$VPC_ID" || -z "$SUBNET_IDS" ]]; then
  echo "ERROR: vpc-id and subnet-ids are required."
  echo "Usage: $0 <vpc-id> <subnet-ids> [region]"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
TICKETING_DIR="$REPO_ROOT/ticketing-app"
SIMULATION_DIR="$REPO_ROOT/simulation-app"
TICKETING_STACK="ticketing-lmi"
SIMULATION_STACK="ticketing-simulation"

# ---------------------------------------------------------------------------
# Helper: update VpcId, SubnetIds, and region in samconfig.toml.
# Uses Python for reliable in-place TOML string replacement —
# avoids sed portability issues on macOS.
# ---------------------------------------------------------------------------
update_samconfig() {
  local config_file="$1"
  local vpc_id="$2"
  local subnet_ids="$3"
  local region="$4"
  local prefix_list_id="${5:-}"

  python3 - "$config_file" "$vpc_id" "$subnet_ids" "$region" "$prefix_list_id" <<'PYEOF'
import sys, re

config_file, vpc_id, subnet_ids, region, prefix_list_id = sys.argv[1:]

with open(config_file, "r") as f:
    content = f.read()

block_header = "[default.deploy.parameters]"
if block_header not in content:
    print(f"WARNING: could not find '{block_header}' in {config_file} — skipping", file=sys.stderr)
    sys.exit(0)

parts = content.split(block_header, 1)
before = parts[0] + block_header
after = parts[1]

after = re.sub(r'"VpcId=[^"]*"', f'"VpcId={vpc_id}"', after)
after = re.sub(r'"SubnetIds=[^"]*"', f'"SubnetIds={subnet_ids}"', after)
after = re.sub(r'^(region\s*=\s*).*$', f'\\g<1>"{region}"', after, flags=re.MULTILINE, count=1)

# Scope LMI egress to the AWS-services prefix list when one was discovered.
# Update the override in place if present; otherwise append it to the
# parameter_overrides list so the secure (scoped-egress) path is the default.
if prefix_list_id:
    if 'AwsServicesPrefixListId=' in after:
        after = re.sub(
            r'"AwsServicesPrefixListId=[^"]*"',
            f'"AwsServicesPrefixListId={prefix_list_id}"',
            after,
        )
    else:
        # Insert as the last entry of the parameter_overrides = [ ... ] list.
        after = re.sub(
            r'(parameter_overrides\s*=\s*\[)',
            f'\\g<1>\n  "AwsServicesPrefixListId={prefix_list_id}",',
            after,
            count=1,
        )

with open(config_file, "w") as f:
    f.write(before + after)

print(f"  Updated {config_file}")
PYEOF
}

# ---------------------------------------------------------------------------
# Step 0: discover the AWS-services prefix list for scoped LMI egress
# ---------------------------------------------------------------------------
# The LMI security group scopes its 443 egress to a managed prefix list (never
# 0.0.0.0/0 — see AwsServicesPrefixListId in ticketing-app/template.yaml). The
# DynamoDB gateway-endpoint prefix list is the standard, always-present one to
# anchor on. AwsServicesPrefixListId is REQUIRED by the template; if we can't
# resolve it here we stop rather than let the deploy fail later at the
# template's Rules assertion with less context.
#
# Override: set AWS_SERVICES_PREFIX_LIST_ID in the environment to skip
# discovery (e.g. when the deploying principal lacks ec2:DescribePrefixLists
# but you already know the prefix list ID).
echo "==== Discovering AWS-services prefix list (for scoped LMI egress) ===="
if [[ -n "${AWS_SERVICES_PREFIX_LIST_ID:-}" ]]; then
  echo "  Using prefix list from environment: ${AWS_SERVICES_PREFIX_LIST_ID}"
else
  AWS_SERVICES_PREFIX_LIST_ID=$(aws ec2 describe-prefix-lists \
    --region "$REGION" \
    --filters "Name=prefix-list-name,Values=com.amazonaws.${REGION}.dynamodb" \
    --query "PrefixLists[0].PrefixListId" \
    --output text 2>/dev/null || true)

  if [[ -z "$AWS_SERVICES_PREFIX_LIST_ID" || "$AWS_SERVICES_PREFIX_LIST_ID" == "None" ]]; then
    echo "  ERROR: could not resolve the DynamoDB prefix list for ${REGION}." >&2
    echo "  AwsServicesPrefixListId is required (LMI egress is scoped to it;" >&2
    echo "  there is no open-egress fallback). Fix one of:" >&2
    echo "    - grant the deploying principal ec2:DescribePrefixLists, or" >&2
    echo "    - re-run with the ID set explicitly:" >&2
    echo "        AWS_SERVICES_PREFIX_LIST_ID=pl-xxxxxxxx $0 $VPC_ID \"$SUBNET_IDS\" $REGION" >&2
    exit 1
  fi
  echo "  Using prefix list: ${AWS_SERVICES_PREFIX_LIST_ID}"
fi

# ---------------------------------------------------------------------------
# Step 1: patch ticketing-app/samconfig.toml
# ---------------------------------------------------------------------------
echo "==== Updating ticketing-app/samconfig.toml ===="
update_samconfig "$TICKETING_DIR/samconfig.toml" "$VPC_ID" "$SUBNET_IDS" "$REGION" "$AWS_SERVICES_PREFIX_LIST_ID"

# ---------------------------------------------------------------------------
# Step 2: build + deploy Ticketing App
# ---------------------------------------------------------------------------
echo ""
echo "==== Building Ticketing App ===="
cd "$TICKETING_DIR"
sam build --cached

echo ""
echo "==== Deploying Ticketing App (${TICKETING_STACK}) ===="
# BudgetNotificationEmail is passed as a CLI parameter-override (not via
# samconfig.toml) because it's an optional per-deploy knob, not a stable
# per-environment setting. SAM CLI merges --parameter-overrides with the
# samconfig.toml list — VpcId and SubnetIds patched above still apply.
TICKETING_DEPLOY_EXTRA_ARGS=()
if [[ -n "$BUDGET_EMAIL" ]]; then
  echo "  Budget notifications: ${BUDGET_EMAIL} (AWS::Budgets::Budget resource will be created)"
  TICKETING_DEPLOY_EXTRA_ARGS+=(--parameter-overrides "BudgetNotificationEmail=${BUDGET_EMAIL}")
fi
sam deploy \
  --stack-name "$TICKETING_STACK" \
  --no-confirm-changeset \
  ${TICKETING_DEPLOY_EXTRA_ARGS[@]+"${TICKETING_DEPLOY_EXTRA_ARGS[@]}"}

# ---------------------------------------------------------------------------
# Step 3: publish AWS Lambda versions (activates Managed Instances)
# ---------------------------------------------------------------------------
echo ""
echo "==== Publishing AWS Lambda versions (activates Managed Instances) ===="
for FUNC in queue-position inventory-lock booking-confirm; do
  FUNC_NAME="${TICKETING_STACK}-${FUNC}"
  echo "  Publishing version for $FUNC_NAME..."
  aws lambda publish-version \
    --function-name "$FUNC_NAME" \
    --region "$REGION" \
    --description "Initial version" \
    --query "Version" \
    --output text
done

# ---------------------------------------------------------------------------
# Step 3b: apply FunctionScalingConfig per handler
# ---------------------------------------------------------------------------
# AWS::Serverless::Function silently ignores the FunctionScalingConfig
# property, so the min/max EE values baked into template.yaml are not
# applied by CFN. Set them explicitly here against $LATEST.PUBLISHED.
# Values match the FunctionScalingConfig blocks in ticketing-app/template.yaml.
echo ""
echo "==== Applying FunctionScalingConfig (Min/Max Execution Environments) ===="
declare -a SCALING_CONFIG=(
  "queue-position:50:100"
  "inventory-lock:100:500"
  "booking-confirm:150:500"
)
for CFG in "${SCALING_CONFIG[@]}"; do
  IFS=':' read -r FUNC MIN_EE MAX_EE <<< "$CFG"
  FUNC_NAME="${TICKETING_STACK}-${FUNC}"
  echo "  ${FUNC_NAME}: Min=${MIN_EE}, Max=${MAX_EE}"
  aws lambda put-function-scaling-config \
    --function-name "$FUNC_NAME" \
    --qualifier '$LATEST.PUBLISHED' \
    --function-scaling-config "MinExecutionEnvironments=${MIN_EE},MaxExecutionEnvironments=${MAX_EE}" \
    --region "$REGION" \
    --output text \
    --query 'FunctionScalingConfig.[MinExecutionEnvironments,MaxExecutionEnvironments]'
done

# ---------------------------------------------------------------------------
# Step 4: retrieve the Ticketing API Gateway endpoint for the sim app
# ---------------------------------------------------------------------------
echo ""
echo "==== Retrieving Ticketing API Gateway endpoint ===="

TICKETING_API_ENDPOINT=$(aws cloudformation describe-stacks \
  --stack-name "$TICKETING_STACK" \
  --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='ApiEndpoint'].OutputValue" \
  --output text)

if [[ -z "$TICKETING_API_ENDPOINT" || "$TICKETING_API_ENDPOINT" == "None" ]]; then
  echo "ERROR: Could not find 'ApiEndpoint' in stack '${TICKETING_STACK}' outputs."
  exit 1
fi

echo "  Ticketing endpoint: $TICKETING_API_ENDPOINT"

# ---------------------------------------------------------------------------
# Step 5: patch simulation-app/samconfig.toml with the live endpoint
# ---------------------------------------------------------------------------
echo ""
echo "==== Updating simulation-app/samconfig.toml ===="
python3 - "$SIMULATION_DIR/samconfig.toml" "$TICKETING_API_ENDPOINT" "$REGION" <<'PYEOF'
import sys, re

config_file, api_endpoint, region = sys.argv[1:]
block_header = "[default.deploy.parameters]"

with open(config_file, "r") as f:
    content = f.read()

parts = content.split(block_header, 1)
before = parts[0] + block_header
after = parts[1]

after = re.sub(
    r'"TicketingApiEndpoint=[^"]*"',
    f'"TicketingApiEndpoint={api_endpoint}"',
    after,
)
after = re.sub(r'^(region\s*=\s*).*$', f'\\g<1>"{region}"', after, flags=re.MULTILINE, count=1)

with open(config_file, "w") as f:
    f.write(before + after)

print(f"  Updated {config_file}")
PYEOF

# ---------------------------------------------------------------------------
# Step 6: build + deploy Simulation App
# ---------------------------------------------------------------------------
echo ""
echo "==== Building Simulation App ===="
cd "$SIMULATION_DIR"
sam build --cached

echo ""
echo "==== Deploying Simulation App (${SIMULATION_STACK}) ===="
sam deploy \
  --stack-name "$SIMULATION_STACK" \
  --no-confirm-changeset

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
SIM_API_ENDPOINT=$(aws cloudformation describe-stacks \
  --stack-name "$SIMULATION_STACK" \
  --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='SimulationApiEndpoint'].OutputValue" \
  --output text)

echo ""
echo "========================================"
echo " Deployment complete"
echo "========================================"
echo ""
echo "  Simulation API   : $SIM_API_ENDPOINT"
echo "  Ticketing API GW : $TICKETING_API_ENDPOINT"
echo ""
echo "  LMI egress       : scoped to prefix list ${AWS_SERVICES_PREFIX_LIST_ID} (443, AWS services only)"
if [[ -n "$BUDGET_EMAIL" ]]; then
  echo "  Budget alarm     : enabled, notifications to ${BUDGET_EMAIL}"
else
  echo "  Budget alarm     : disabled (set BUDGET_EMAIL to enable; see script header)"
fi
echo ""
echo "  Required Service Quotas — request increases BEFORE running load tests:"
echo "    1. EC2 vCPU quota (L-1216C47A, 'Running On-Demand Standard instances')"
echo "         Recommended: 7,000 vCPU (template caps LMI fleet at 6,000)"
echo "         Default is 5-32 vCPU — insufficient for the baked-in Min EEs."
echo "    2. API Gateway account throttle (L-8A5B8E43)"
echo "         Recommended for 40K TPS burst: 50,000 rps (default is 10,000)."
echo ""
echo "Both samconfig.toml files have been updated — run 'sam deploy' directly"
echo "from either app directory to redeploy without passing parameters again."
echo ""
echo "To trigger a simulation run:"
echo "  # Smoke test (works with default AWS quotas):"
echo "  curl -X POST ${SIM_API_ENDPOINT}/simulation/start \\"
echo "       -H 'Content-Type: application/json' \\"
echo "       -d '{\"target_tps\": 1000, \"ramp_duration_seconds\": 30, \"sustain_duration_seconds\": 120}'"
echo ""
echo "  # Higher-TPS run (needs API Gateway quota increase; see README):"
echo "  #   -d '{\"target_tps\": 40000, \"ramp_duration_seconds\": 0, \"sustain_duration_seconds\": 120}'"
echo ""
echo "AWS console links:"
echo "  Lambda functions (per-function invocations / duration / errors / throttles):"
echo "    https://${REGION}.console.aws.amazon.com/lambda/home?region=${REGION}#/functions"
echo "  Capacity Provider console (LMI fleet health, EE CPU/memory, instance count):"
echo "    https://${REGION}.console.aws.amazon.com/lambda/home?region=${REGION}#/capacity-providers/${TICKETING_STACK}-ticketing-cp"
