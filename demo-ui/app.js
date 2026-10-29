// LMI Ticketing Demo — Alpine.js component
//
// Panel does two things:
//   1. POST /proxy/simulation/start to fire a burst against the ticketing API
//   2. On demand, GET /proxy/dashboard?run_id=... to fetch AWS-side results
//      (API Gateway count, DDB bookings scan, per-handler EE + Lambda metrics)
//
// All fetches go to same-origin /proxy/* paths; demo-ui/serve.py SigV4-signs
// each request with the operator's AWS credentials and forwards it to the
// real simulation API. The browser never holds an AWS credential — both the
// simulation API and the ticketing API are deployed with AWS_IAM auth.
//
// Depends on: config.js (window.CONFIG), Alpine.js (self-hosted under vendor/).

function demoApp() {
  const cfg = window.CONFIG || {};
  // The proxy base path on serve.py. Changing this requires matching
  // the path prefix in demo-ui/serve.py's ProxyingNoCacheHandler.
  const proxyBase = "/proxy";
  // SIMULATION_API_URL is used only for display in the "Ready-to-paste CLI
  // queries" block below. The actual proxy forwarding reads
  // SIMULATION_API_URL from the serve.py environment, not from this
  // config, so the browser never learns the real API endpoint.
  const simApi = cfg.SIMULATION_API_URL || "";

  return {
    // ===== Configuration mirrors =====
    targetLoads: cfg.TARGET_LOADS || [],

    // ===== Burst config (editable in UI) =====
    selectedTarget: 1000,
    customTarget: null,
    customTargetActive: false,
    burstDuration: cfg.BURST_SUSTAIN_SECONDS || 60,
    burstRamp: cfg.BURST_RAMP_SECONDS || 5,
    burstRunning: false,
    burstElapsed: 0,
    _burstStartAt: 0,
    _burstTimer: null,

    // ===== Identifiers for the current / last burst =====
    burstRunId: "",        // Full UUID returned by /simulation/start
    copiedField: "",       // Which identifier was just copied (flash feedback)
    _copyResetTimer: null,

    // Small text under the button — the only feedback that remains.
    statusLine: "",

    // ===== Unified post-burst results (on-demand) =====
    // Single call to /dashboard?include_ground_truth=true&include_ee_stats=true
    // fetches API Gateway CloudWatch count + bookings-table scan +
    // per-handler Log Insights + per-handler Lambda CloudWatch metrics
    // in parallel. Populated by fetchResults() when the user clicks
    // "View results" after the burst finishes.
    results: null,           // { apigw_count, bookings, scanned_rows, scan_elapsed_s, handlers: [...] }
    resultsLoading: false,
    resultsError: "",

    // ==================================================================
    // COMPUTED
    // ==================================================================

    get effectiveTarget() {
      if (this.customTargetActive && this.customTarget > 0) return this.customTarget;
      return this.selectedTarget;
    },

    // The orchestrator derives event_ids as `RUN-{run_id[:8]}-{i:02d}`.
    // With the stack's default SIMULATED_EVENTS="EVT-001" (single event),
    // that yields exactly one event_id per burst — mirrored here so users
    // can copy it without needing to fetch anything server-side.
    get burstEventId() {
      if (!this.burstRunId) return "";
      return `RUN-${this.burstRunId.slice(0, 8)}-00`;
    },

    // Ready-to-paste CLI queries that use the current burst's identifiers.
    // Set AWS_PROFILE and --region to match your deployment before running.
    get cliDashboard() {
      if (!this.burstRunId) return "";
      if (!simApi) return "# set SIMULATION_API_URL in config.js to generate this snippet";
      // Both APIs are deployed with AuthorizationType: AWS_IAM, so curl
      // must SigV4-sign the request. awscurl (pip install awscurl) does
      // this using the standard AWS credential chain.
      return `awscurl --service execute-api --region us-east-1 \\
  "${simApi}/dashboard?run_id=${this.burstRunId}&include_ground_truth=true&include_ee_stats=true&event_id=${this.burstEventId}" | jq .`;
    },

    get cliBookingsCount() {
      if (!this.burstEventId) return "";
      // The bookings table has no event_id GSI, so this is a Scan.
      // Cheap for demo tables but O(N) — for large tables prefer the
      // /dashboard API (cliDashboard) which returns pre-aggregated counts.
      return `aws dynamodb scan \\
  --table-name ticketing-lmi-bookings \\
  --filter-expression "event_id = :evt" \\
  --expression-attribute-values '{":evt":{"S":"${this.burstEventId}"}}' \\
  --select COUNT \\
  --region us-east-1`;
    },

    get cliSimMetrics() {
      if (!this.burstRunId) return "";
      return `aws dynamodb query \\
  --table-name ticketing-simulation-metrics \\
  --key-condition-expression "run_id = :rid" \\
  --expression-attribute-values '{":rid":{"S":"${this.burstRunId}"}}' \\
  --region us-east-1`;
    },

    // ==================================================================
    // LIFECYCLE
    // ==================================================================

    init() {
      if (!this.targetLoads.find(t => t.value === this.selectedTarget)) {
        this.selectedTarget = this.targetLoads[0]?.value || 1000;
      }
    },

    // ==================================================================
    // UNIFIED POST-BURST RESULTS (on-demand)
    // ==================================================================

    // Single fetch of /dashboard with both ground_truth and ee_stats flags.
    // Backend runs API Gateway CW query + bookings DDB scan + per-handler
    // Log Insights + per-handler Lambda CloudWatch metrics in parallel.
    // Returns a flat shape the UI consumes without further processing.
    async fetchResults() {
      if (!this.burstRunId) return;
      this.resultsLoading = true;
      this.resultsError = "";
      try {
        const qs = new URLSearchParams({
          run_id: this.burstRunId,
          event_id: this.burstEventId,
          include_ground_truth: "true",
          include_ee_stats: "true",
        });
        const url = `${proxyBase}/dashboard?${qs.toString()}`;
        const r = await fetch(url);
        if (!r.ok) throw new Error("HTTP " + r.status);
        const data = await r.json();
        if (data.error) throw new Error(data.error);

        const gt = data.ground_truth || {};
        const ee = data.ee_stats || {};
        const apigw = gt.api_gateway || {};
        const bk = gt.bookings || {};
        if (apigw.error) throw new Error(`API Gateway: ${apigw.error}`);
        if (bk.error) throw new Error(`Bookings scan: ${bk.error}`);
        if (ee.error) throw new Error(`EE stats: ${ee.error}`);

        // Flatten per-handler data: merge EE count with Lambda CW metrics.
        // p50 is available on the backend but the table only renders p95/p99.
        const handlers = (ee.handlers || []).map(h => ({
          handler: h.handler,
          distinct_ees_used: h.distinct_ees_used ?? 0,
          throttles: h.lambda_metrics?.throttles ?? 0,
          p95_ms: h.lambda_metrics?.duration_p95_ms ?? 0,
          p99_ms: h.lambda_metrics?.duration_p99_ms ?? 0,
        }));

        this.results = {
          apigw_count: apigw.count ?? 0,
          bookings: bk.count ?? 0,
          scanned_rows: bk.scanned_rows ?? 0,
          scan_elapsed_s: bk.elapsed_s ?? 0,
          handlers,
        };
      } catch (e) {
        console.error("Results fetch failed", e);
        this.resultsError = e.message || String(e);
      } finally {
        this.resultsLoading = false;
      }
    },

    // ==================================================================
    // TARGET SELECTION
    // ==================================================================

    selectPresetTarget(value) {
      this.selectedTarget = value;
      this.customTarget = null;
      this.customTargetActive = false;
    },

    onCustomTargetInput() {
      this.customTargetActive = !!(this.customTarget && this.customTarget > 0);
    },

    // Copy the given value to the clipboard and briefly flash the field's
    // "copy" affordance to "copied ✓". Falls back to a hidden <textarea>
    // and execCommand("copy") on browsers that don't expose the async
    // clipboard API over http:// (e.g. localhost).
    async copyToClipboard(value, fieldName) {
      if (!value) return;
      try {
        if (navigator.clipboard && window.isSecureContext) {
          await navigator.clipboard.writeText(value);
        } else {
          const ta = document.createElement("textarea");
          ta.value = value;
          ta.style.position = "fixed";
          ta.style.opacity = "0";
          document.body.appendChild(ta);
          ta.select();
          document.execCommand("copy");
          document.body.removeChild(ta);
        }
        this.copiedField = fieldName;
        if (this._copyResetTimer) clearTimeout(this._copyResetTimer);
        this._copyResetTimer = setTimeout(() => (this.copiedField = ""), 1500);
      } catch (e) {
        console.warn("Copy failed", e);
        alert(value);   // last-resort — user can select from the alert
      }
    },

    // ==================================================================
    // BURST SIMULATION
    // ==================================================================

    async startBurst() {
      if (this.burstRunning) return;
      const target = this.effectiveTarget;
      if (target < 1) return;

      this.burstRunning = true;
      this.burstElapsed = 0;
      this._burstStartAt = Date.now();
      this.burstRunId = "";       // clear the previous burst's IDs
      this.copiedField = "";
      // Reset results — button re-appears for the new run
      this.results = null;
      this.resultsError = "";
      this.resultsLoading = false;
      this.statusLine = "Sending burst request…";

      try {
        const r = await fetch(`${proxyBase}/simulation/start`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            target_tps: target,
            ramp_duration_seconds: this.burstRamp,
            sustain_duration_seconds: this.burstDuration
          })
        });
        if (!r.ok) throw new Error("HTTP " + r.status);
        const data = await r.json();
        this.burstRunId = data.run_id || "";
        this.statusLine = this.burstRunId
          ? `Burst started · duration ${this.burstDuration}s`
          : "Burst started (no run_id returned)";
        if (this.burstRunId) console.log("Burst started, run_id:", this.burstRunId);
      } catch (e) {
        console.error("Simulation-start call failed", e);
        this.burstRunning = false;
        this.statusLine = `Failed to start burst: ${e.message || e}`;
        return;
      }

      const totalMs = (this.burstDuration + this.burstRamp) * 1000;
      this._burstTimer = setInterval(() => {
        this.burstElapsed = Math.round((Date.now() - this._burstStartAt) / 1000);
        if (Date.now() - this._burstStartAt >= totalMs) {
          this.stopBurst();
        }
      }, 250);
    },

    stopBurst() {
      this.burstRunning = false;
      this.burstElapsed = 0;
      if (this._burstTimer) {
        clearInterval(this._burstTimer);
        this._burstTimer = null;
      }
      this.statusLine = "Burst complete";
      // burstRunId + burstEventId stay set so the identifiers block remains
      // visible for copy/paste after the burst finishes.
    }
  };
}

document.addEventListener("alpine:init", () => {
  Alpine.data("demoApp", demoApp);
});
