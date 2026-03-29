import { corsHeaders, json } from "../_shared/cors.ts";

function env(name: string): string {
  const value = Deno.env.get(name);
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function mapWorkflowStatus(run: Record<string, unknown> | null) {
  if (!run) {
    return { current_job: null, last_job: null, selected_job: null };
  }

  const status = String(run.status ?? "");
  const conclusion = String(run.conclusion ?? "");
  const createdAt = String(run.created_at ?? new Date().toISOString());
  const updatedAt = String(run.updated_at ?? createdAt);
  const displayTitle = String(run.display_title ?? "Actualizacion Contifico");
  const htmlUrl = String(run.html_url ?? "");
  const runId = String(run.id ?? "");

  const scopeKey = displayTitle.toLowerCase().includes("backfill") ? "backfill" : "refresh";
  const scopeLabel = scopeKey === "backfill" ? "Refresh completo" : "Refresh rapido";
  const baseJob = {
    job_id: runId,
    status: status === "completed" ? (conclusion === "success" ? "success" : "error") : "running",
    scope: `${scopeKey}_plus_snapshot`,
    scope_key: scopeKey,
    scope_label: scopeLabel,
    stage: status === "completed" ? (conclusion === "success" ? "finalizado" : "error") : "running",
    started_at: createdAt,
    finished_at: status === "completed" ? updatedAt : null,
    duration_seconds: null,
    message: displayTitle,
    error_text: status === "completed" && conclusion !== "success" ? `Workflow terminado con conclusion ${conclusion}.` : null,
    html_url: htmlUrl,
  };

  if (status === "completed") {
    return { current_job: null, last_job: baseJob, selected_job: baseJob };
  }
  return { current_job: baseJob, last_job: null, selected_job: baseJob };
}

function selectWorkflowRun(payload: Record<string, unknown>, requestedJobId: string | null) {
  const runs = Array.isArray(payload.workflow_runs) ? payload.workflow_runs as Array<Record<string, unknown>> : [];
  if (!requestedJobId || !requestedJobId.includes(":")) {
    return runs[0] ?? null;
  }

  const [, requestedAt] = requestedJobId.split(":", 2);
  const normalizedRequestedAt = requestedAt.trim();
  if (!normalizedRequestedAt) {
    return runs[0] ?? null;
  }

  const exact = runs.find((run) => String(run.display_title ?? "").includes(normalizedRequestedAt));
  if (exact) {
    return exact;
  }

  return runs.find((run) => String(run.created_at ?? "") >= normalizedRequestedAt) ?? runs[0] ?? null;
}

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  if (request.method !== "GET") {
    return json({ error: "Method not allowed" }, 405);
  }

  try {
    const githubToken = env("GITHUB_WORKFLOW_TOKEN");
    const githubOwner = env("GITHUB_OWNER");
    const githubRepo = env("GITHUB_REPO");
    const workflowFile = Deno.env.get("GITHUB_WORKFLOW_FILE") || "contifico-cloud-refresh.yml";
    const workflowRef = Deno.env.get("GITHUB_REF") || "main";
    const url = new URL(request.url);
    const runId = url.searchParams.get("run_id");

    const endpoint = runId && /^\d+$/.test(runId)
      ? `https://api.github.com/repos/${githubOwner}/${githubRepo}/actions/runs/${runId}`
      : `https://api.github.com/repos/${githubOwner}/${githubRepo}/actions/workflows/${workflowFile}/runs?branch=${workflowRef}&per_page=5`;

    const response = await fetch(endpoint, {
      headers: {
        "Accept": "application/vnd.github+json",
        "Authorization": `Bearer ${githubToken}`,
        "User-Agent": "contifico-refresh-status",
      },
    });

    if (!response.ok) {
      const details = await response.text();
      return json({ error: "No fue posible consultar el workflow cloud", details }, 502);
    }

    const payload = await response.json();
    const run = Array.isArray(payload.workflow_runs) ? selectWorkflowRun(payload as Record<string, unknown>, runId) : payload;
    const mapped = mapWorkflowStatus(run);
    return json({
      api_available: true,
      capabilities: {
        refresh_scopes: [
          { key: "refresh", label: "Refresh rapido" },
          { key: "backfill", label: "Refresh completo" },
        ],
      },
      runtime: {
        current_job: mapped.current_job,
        last_job: mapped.last_job,
      },
      job: mapped.selected_job,
    });
  } catch (error) {
    return json({ error: String(error instanceof Error ? error.message : error) }, 500);
  }
});
