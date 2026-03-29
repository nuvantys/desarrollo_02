import { corsHeaders, json } from "../_shared/cors.ts";

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

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  if (request.method !== "POST") {
    return json({ error: "Method not allowed" }, 405);
  }

  try {
    const body = await request.json().catch(() => ({}));
    const scope = normalizeScope(body?.scope);

    const githubToken = env("GITHUB_WORKFLOW_TOKEN");
    const githubOwner = env("GITHUB_OWNER");
    const githubRepo = env("GITHUB_REPO");
    const workflowFile = Deno.env.get("GITHUB_WORKFLOW_FILE") || "contifico-cloud-refresh.yml";
    const workflowRef = Deno.env.get("GITHUB_REF") || "main";

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
            requested_at: new Date().toISOString(),
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

    const jobId = `${scope}-${Date.now()}`;
    return json(
      {
        job: {
          job_id: jobId,
          status: "running",
          scope: `${scope}_plus_snapshot`,
          scope_key: scope,
          scope_label: scopeLabel(scope),
          stage: "queued",
          started_at: new Date().toISOString(),
          finished_at: null,
          duration_seconds: null,
          message: `${scopeLabel(scope)} despachado a GitHub Actions para actualizar Supabase y republicar el snapshot.`,
          html_url: `https://github.com/${githubOwner}/${githubRepo}/actions/workflows/${workflowFile}`,
        },
      },
      202,
    );
  } catch (error) {
    return json({ error: String(error instanceof Error ? error.message : error) }, 500);
  }
});
