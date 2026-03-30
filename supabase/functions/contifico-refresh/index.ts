import { corsHeaders, json } from "../_shared/cors.ts";
import { AuthError, requireUser } from "../_shared/auth.ts";

type RefreshScope = "refresh" | "backfill";

function env(name: string): string {
  const value = Deno.env.get(name);
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function normalizeScope(value: unknown): RefreshScope {
  const text = String(value ?? "refresh").trim().toLowerCase();
  if (["refresh", "rapido", "rápido", "quick", "fast"].includes(text)) return "refresh";
  if (["backfill", "completo", "full", "complete"].includes(text)) return "backfill";
  throw new Error("Invalid scope. Use refresh or backfill.");
}

function scopeLabel(scope: RefreshScope): string {
  return scope === "backfill" ? "Refresh completo" : "Refresh rapido";
}

function jobId(scope: RefreshScope, requestedAt: string): string {
  return `${scope}:${requestedAt}`;
}

function scopeFromTitle(displayTitle: string): RefreshScope {
  return displayTitle.toLowerCase().includes("backfill") ? "backfill" : "refresh";
}

function runningJobFromRun(run: Record<string, unknown>) {
  const displayTitle = String(run.display_title ?? "Actualizacion Contifico");
  const scope = scopeFromTitle(displayTitle);
  return {
    job_id: String(run.id ?? ""),
    status: "running",
    scope: `${scope}_plus_snapshot`,
    scope_key: scope,
    scope_label: scopeLabel(scope),
    stage: String(run.status ?? "queued"),
    started_at: String(run.created_at ?? new Date().toISOString()),
    finished_at: null,
    duration_seconds: null,
    message: "Ya existe una actualizacion cloud en curso. Se reutiliza esa corrida para evitar conflictos al publicar el snapshot.",
    html_url: String(run.html_url ?? ""),
  };
}

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  if (request.method !== "POST") {
    return json({ error: "Method not allowed" }, 405);
  }

  try {
    await requireUser(request);
    const body = await request.json().catch(() => ({}));
    const scope = normalizeScope(body?.scope);

    const githubToken = env("GITHUB_WORKFLOW_TOKEN");
    const githubOwner = env("GITHUB_OWNER");
    const githubRepo = env("GITHUB_REPO");
    const workflowFile = Deno.env.get("GITHUB_WORKFLOW_FILE") || "contifico-cloud-refresh.yml";
    const workflowRef = Deno.env.get("GITHUB_REF") || "main";

    const runsResponse = await fetch(
      `https://api.github.com/repos/${githubOwner}/${githubRepo}/actions/workflows/${workflowFile}/runs?branch=${workflowRef}&per_page=5`,
      {
        headers: {
          "Accept": "application/vnd.github+json",
          "Authorization": `Bearer ${githubToken}`,
          "User-Agent": "contifico-refresh-dispatch",
        },
      },
    );

    if (!runsResponse.ok) {
      const details = await runsResponse.text();
      return json(
        {
          error: "No fue posible inspeccionar el estado actual del workflow cloud",
          details,
        },
        502,
      );
    }

    const runsPayload = await runsResponse.json();
    const activeRun = Array.isArray(runsPayload.workflow_runs)
      ? runsPayload.workflow_runs.find((run: Record<string, unknown>) => String(run.status ?? "") !== "completed")
      : null;

    if (activeRun) {
      return json({ job: runningJobFromRun(activeRun) }, 202);
    }

    const requestedAt = new Date().toISOString();

    const dispatchResponse = await fetch(
      `https://api.github.com/repos/${githubOwner}/${githubRepo}/actions/workflows/${workflowFile}/dispatches`,
      {
        method: "POST",
        headers: {
          "Accept": "application/vnd.github+json",
          "Authorization": `Bearer ${githubToken}`,
          "User-Agent": "contifico-refresh-dispatch",
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          ref: workflowRef,
          inputs: {
            scope,
            trigger_source: "dashboard",
            requested_at: requestedAt,
          },
        }),
      },
    );

    if (!dispatchResponse.ok) {
      const details = await dispatchResponse.text();
      return json(
        {
          error: "No fue posible disparar el workflow cloud",
          details,
        },
        502,
      );
    }

    return json(
      {
        job: {
          job_id: jobId(scope, requestedAt),
          status: "running",
          scope: `${scope}_plus_snapshot`,
          scope_key: scope,
          scope_label: scopeLabel(scope),
          stage: "queued",
          started_at: requestedAt,
          finished_at: null,
          duration_seconds: null,
          message: `${scopeLabel(scope)} despachado a GitHub Actions para actualizar Supabase y republicar el snapshot.`,
          html_url: `https://github.com/${githubOwner}/${githubRepo}/actions/workflows/${workflowFile}`,
        },
      },
      202,
    );
  } catch (error) {
    if (error instanceof AuthError) {
      return json({ error: error.message }, error.status);
    }
    return json({ error: String(error instanceof Error ? error.message : error) }, 500);
  }
});
