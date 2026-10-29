# LMI Ticketing Demo UI

A single-page panel for firing a burst against the LMI ticketing sample and
inspecting the AWS-side results (API Gateway count, DynamoDB bookings,
per-handler EE and Lambda metrics) after the burst finishes. No build step —
plain HTML + Alpine.js + the Tailwind Play runtime, both self-hosted under
`vendor/` (no external CDN dependency, so the demo works offline and pins the
exact library versions).

Both the simulation API and the ticketing API are deployed with
`AuthorizationType: AWS_IAM`, so every call to either must be SigV4-signed.
The UI never signs requests in the browser — it sends same-origin requests
to `/proxy/*` and `serve.py` signs them using the operator's AWS credentials
before forwarding to the real API Gateway endpoint. No AWS credential is
ever exposed to the browser.

See the [main repository README](../README.md) for the ticketing/simulation
stacks this UI drives.

## Prerequisites

- Both `ticketing-lmi` and `ticketing-simulation` CloudFormation stacks
  deployed (see repo README).
- Python 3.8+ on your workstation.
- `boto3` installed in that Python (`pip install boto3`). If you already use
  AWS CLI v2, boto3 is usually available in the same environment.
- AWS credentials on your workstation (`~/.aws/credentials`, `AWS_PROFILE`,
  or SSO session) scoped to invoke the two API Gateway endpoints. The IAM
  identity you use is the same identity the ticketing handlers see as
  `user_id` on every booking they confirm.

## Configure

Point the UI at your deployed simulation API in two places:

1. **Proxy environment variable** — read by `serve.py` at launch; this is
   what actually makes signed requests:

   ```bash
   export SIMULATION_API_URL="$(aws cloudformation describe-stacks \
     --stack-name ticketing-simulation \
     --query 'Stacks[0].Outputs[?OutputKey==`SimulationApiEndpoint`].OutputValue' \
     --output text)"
   ```

2. **`config.js`** — display-only, used to render the "Ready-to-paste CLI
   queries" strings in the UI panel. Set it to the same endpoint for
   consistency:

   ```js
   window.CONFIG = {
     SIMULATION_API_URL: "https://xxxxxxxxxx.execute-api.us-east-1.amazonaws.com/v1",
     // ...
   };
   ```

## Run

```bash
python3 serve.py
# → Serving on http://127.0.0.1:8000/ (no-cache)
# → Proxying /proxy/* to <SIMULATION_API_URL> with SigV4 signatures
```

`serve.py` binds to `127.0.0.1` only — this is a developer-local tool
holding your AWS credentials in process memory and must not accept
connections from the network. Open `http://127.0.0.1:8000/` in a browser.

Pass a port argument to override the default `8000`:

```bash
python3 serve.py 9090
```

If you change the port, update `AllowedOrigin` on both CloudFormation
stacks (ticketing-app and simulation-app) to the matching
`http://127.0.0.1:<port>` or `http://localhost:<port>` value, or the
CORS preflight will reject the browser's requests.

## What the UI does

1. **Fire a burst.** Pick a target TPS (preset button or custom value),
   set the duration and ramp, then press **Start burst**. The UI POSTs
   `{target_tps, ramp_duration_seconds, sustain_duration_seconds}` to
   `/proxy/simulation/start` — `serve.py` SigV4-signs the forwarded
   request to the real `/simulation/start` endpoint and relays the
   returned `run_id`.

2. **Copy the burst identifiers.** After the burst starts, the panel
   shows the `run_id` (partition key in the `ticketing-simulation-metrics`
   table) and the derived `event_id` (used in `inventory-sharded` and
   `bookings`). Click either to copy.

3. **View results after the burst finishes.** Press **View results** to
   fetch AWS-side ground truth in a single call to
   `/proxy/dashboard?include_ground_truth=true&include_ee_stats=true`.
   The backend runs four queries in parallel:
   - API Gateway `Count` metric from CloudWatch
   - Segmented parallel `Scan` of the bookings table filtered by
     `event_id`
   - CloudWatch Logs Insights on each handler's log group to count
     distinct execution environments used
   - CloudWatch `AWS/Lambda` metrics (Throttles + Duration p50/p95/p99)
     per handler

4. **Copy-paste CLI queries.** The "Ready-to-paste CLI queries"
   expander at the bottom of the identifiers block gives you snippets
   that bypass the UI. Because the simulation API uses AWS_IAM auth,
   the `curl`-style snippet uses [`awscurl`](https://pypi.org/project/awscurl/)
   (`pip install awscurl`) which signs each request using your standard
   AWS credential chain.

The UI never calls the ticketing API directly — it only orchestrates
the simulation stack, which is where all the load-generation lives.

## File layout

```
demo-ui/
├── index.html    # Structure + Alpine.js markup
├── app.js        # Alpine component (state, methods)
├── config.js     # SIMULATION_API_URL + preset load buttons
├── styles.css    # Small overrides on top of Tailwind (spinner-hiding rule)
├── serve.py      # Loopback dev server + SigV4-signing proxy
├── vendor/       # Self-hosted third-party libs (no external CDN)
│   ├── tailwindcss-play-3.4.5.min.js   # Tailwind Play in-browser JIT runtime
│   └── alpinejs-3.13.5.min.js          # Alpine.js
└── README.md     # This file
```

### Updating the vendored libraries

The two libraries under `vendor/` are pinned to the versions in their
filenames. To bump a version, re-download from the upstream source and update
the `<script src>` in `index.html` to match:

```bash
# Alpine.js
curl -fsSL -o vendor/alpinejs-<version>.min.js \
  "https://cdn.jsdelivr.net/npm/alpinejs@<version>/dist/cdn.min.js"

# Tailwind Play runtime
curl -fsSL -o vendor/tailwindcss-play-<version>.min.js \
  "https://cdn.tailwindcss.com/<version>"
```

> The Tailwind Play runtime compiles utility classes in the browser and is
> meant for prototyping/demos. For a production UI, replace it with a proper
> Tailwind build step that emits a static, tree-shaken CSS file.
