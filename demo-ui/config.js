// Demo UI configuration.
//
// Both the simulation API and the ticketing API are deployed with
// AuthorizationType: AWS_IAM, so every request must be SigV4-signed.
// The UI sends same-origin requests to /proxy/*; demo-ui/serve.py does
// the signing and forwards to the real API Gateway endpoint. That means:
//
//   - SIMULATION_API_URL below is used ONLY to render the "Ready-to-paste
//     CLI queries" strings in the UI. Setting it has no effect on how the
//     UI talks to the API — the proxy reads its own SIMULATION_API_URL
//     from the environment of the python3 serve.py process.
//
//   - To configure the proxy, set the env var before launching serve.py:
//       export SIMULATION_API_URL="$(aws cloudformation describe-stacks \
//         --stack-name ticketing-simulation \
//         --query 'Stacks[0].Outputs[?OutputKey==`SimulationApiEndpoint`].OutputValue' \
//         --output text)"
//       python3 serve.py
//
// The CLI-display value below should match whatever endpoint serve.py was
// started with. If left as the placeholder, the UI shows a hint instead
// of a copy-paste snippet.

window.CONFIG = {
  // Display-only. Copy-paste the same endpoint you set in SIMULATION_API_URL
  // env var when you ran serve.py.
  SIMULATION_API_URL: "REPLACE_WITH_SIMULATION_API_ENDPOINT",

  // Preset load buttons rendered above the target-TPS input.
  TARGET_LOADS: [
    { label: "1K",  value: 1000,  desc: "Trial mode"  },
    { label: "5K",  value: 5000,  desc: "Small event" },
    { label: "10K", value: 10000, desc: "Mid-size"    },
    { label: "40K", value: 40000, desc: "Headline"    }
  ],

  // Default burst shape. Users can override in the UI before firing. These
  // defaults respect the server-side clamps (MaxSustainSeconds = 300,
  // MaxRampSeconds = 60) in simulation-app/template.yaml; exceeding them
  // in the UI will come back as 400 from the orchestrator.
  BURST_SUSTAIN_SECONDS: 60,
  BURST_RAMP_SECONDS: 5,
};
