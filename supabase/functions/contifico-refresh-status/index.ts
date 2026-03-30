import { corsHeaders, json } from "../_shared/cors.ts";
import { AuthError, requireUser } from "../_shared/auth.ts";

const meaningfulSteps = [
  { key: "queued", label: "En cola", matches: [] as string[] },
  { key: "setup", label: "Preparando entorno", matches: ["setup job", "checkout", "setup python"] },
  { key: "dependencies", label: "Instalando dependencias", matches: ["install dependencies"] },
  { key: "sync", label: "Actualizando Supabase", matches: ["refresh supabase from contifico api"] },
  { key: "publish", label: "Publicando snapshot", matches: ["commit published snapshot"] },
  { key: "final", label: "Finalizado", matches: [] as string[] },
];

function env(name: string): string {
  const value = Deno.env.get(name);
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function normalizeText(value: unknown): string {
  return String(value ?? "").trim().toLowerCase();
}

function stepBucket(stepName: string): string | null {
  const normalized = normalizeText(stepName);
  for (const step of meaningfulSteps) {
    if (step.matches.some((match) => normalized.includes(match))) {
      return step.key;
    }
  }
  return null;
}

function stageLabelFromKey(key: string): string {
  return meaningfulSteps.find((step) => step.key === key)?.label || "Procesando";
}

function buildProgress(run: Record<string, unknown>, jobsPayload: Record<string, unknown> | null) {
  const status = normalizeText(run.status);
  const conclusion = normalizeText(run.conclusion);
  const jobs = Array.isArray(jobsPayload?.jobs) ? jobsPayload?.jobs as Array<Record<string, unknown>> : [];
  const refreshJob = jobs.find((job) => normalizeText(job.name) === "refresh") || jobs[0] || null;
  const rawSteps = Array.isArray(refreshJob?.steps) ? refreshJob?.steps as Array<Record<string, unknown>> : [];

  const stageStates = meaningfulSteps.map((step, index) => ({
    key: step.key,
    label: step.label,
    status: index === 0 ? "completed" : "pending",
  }));

  if (status === "completed") {
    const finalStatus = conclusion === "success" ? "completed" : "error";
    for (const stage of stageStates) {
      stage.status = stage.key === "final" ? finalStatus : "completed";
    }
    return {
      progress_percent: 100,
      progress_label: conclusion === "success" ? "100%" : "100% con error",
      stage: conclusion === "success" ? "finalizado" : "error",
      stage_detail: conclusion === "success" ? "Workflow terminado y snapshot publicado." : `Workflow termino con conclusion ${conclusion || "error"}.`,
      steps: stageStates,
      active_step: conclusion === "success" ? "Finalizado" : "Terminado con error",
      completed_steps: stageStates.filter((stage) => stage.status === "completed").length,
      total_steps: stageStates.length,
    };
  }

  if (status === "queued" || !refreshJob) {
    stageStates[0].status = "active";
    return {
      progress_percent: 6,
      progress_label: "6%",
      stage: "queued",
      stage_detail: "Esperando que GitHub Actions asigne runner y arranque el workflow.",
      steps: stageStates,
      active_step: "En cola",
      completed_steps: 0,
      total_steps: stageStates.length,
    };
  }

  let activeKey = "setup";
  const completedKeys = new Set<string>();

  for (const rawStep of rawSteps) {
    const bucket = stepBucket(String(rawStep.name ?? ""));
    if (!bucket) continue;
    const stepStatus = normalizeText(rawStep.status);
    const stepConclusion = normalizeText(rawStep.conclusion);
    if (stepStatus === "completed" && stepConclusion === "success") {
      completedKeys.add(bucket);
      continue;
    }
    if (stepStatus === "in_progress" || stepStatus === "queued" || !stepStatus) {
      activeKey = bucket;
      break;
    }
  }

  if ([...completedKeys].length) {
    const orderedKeys = meaningfulSteps.map((step) => step.key);
    const lastCompletedIndex = Math.max(...[...completedKeys].map((key) => orderedKeys.indexOf(key)));
    activeKey = orderedKeys[Math.min(lastCompletedIndex + 1, orderedKeys.length - 2)] || activeKey;
  }

  for (const stage of stageStates) {
    if (stage.key === "queued") {
      stage.status = "completed";
    } else if (completedKeys.has(stage.key)) {
      stage.status = "completed";
    } else if (stage.key === activeKey) {
      stage.status = "active";
    }
  }

  const weightedPercent = {
    queued: 6,
    setup: 18,
    dependencies: 34,
    sync: 72,
    publish: 92,
    final: 100,
  }[activeKey] || 12;

  const activeStepName =
    rawSteps.find((step) => normalizeText(step.status) === "in_progress")?.name ||
    rawSteps.find((step) => stepBucket(String(step.name ?? "")) === activeKey)?.name ||
    stageLabelFromKey(activeKey);

  const stageDetailMap: Record<string, string> = {
    setup: "Preparando runner, checkout del repo y entorno Python para ejecutar el pipeline.",
    dependencies: "Instalando dependencias necesarias para consultar Contifico, actualizar Supabase y regenerar el snapshot.",
    sync: "Ejecutando la extraccion desde Contifico y escribiendo la corrida en Supabase.",
    publish: "Versionando y publicando el snapshot regenerado para el frontend.",
  };

  return {
    progress_percent: weightedPercent,
    progress_label: `${weightedPercent}%`,
    stage: activeKey,
    stage_detail: stageDetailMap[activeKey] || "Workflow en ejecucion.",
    steps: stageStates,
    active_step: String(activeStepName),
    completed_steps: stageStates.filter((stage) => stage.status === "completed").length,
    total_steps: stageStates.length,
  };
}

function mapWorkflowStatus(run: Record<string, unknown> | null, jobsPayload: Record<string, unknown> | null) {
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
  const progress = buildProgress(run, jobsPayload);

  const scopeKey = displayTitle.toLowerCase().includes("backfill") ? "backfill" : "refresh";
  const scopeLabel = scopeKey === "backfill" ? "Refresh completo" : "Refresh rapido";
  const baseJob = {
    job_id: runId,
    status: status === "completed" ? (conclusion === "success" ? "success" : "error") : "running",
    scope: `${scopeKey}_plus_snapshot`,
    scope_key: scopeKey,
    scope_label: scopeLabel,
    stage: progress.stage,
    started_at: createdAt,
    finished_at: status === "completed" ? updatedAt : null,
    duration_seconds: null,
    message: displayTitle,
    error_text: status === "completed" && conclusion !== "success" ? `Workflow terminado con conclusion ${conclusion}.` : null,
    html_url: htmlUrl,
    progress_percent: progress.progress_percent,
    progress_label: progress.progress_label,
    stage_detail: progress.stage_detail,
    steps: progress.steps,
    active_step: progress.active_step,
    completed_steps: progress.completed_steps,
    total_steps: progress.total_steps,
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
    await requireUser(request);
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
    let jobsPayload: Record<string, unknown> | null = null;
    if (run?.id) {
      const jobsResponse = await fetch(
        `https://api.github.com/repos/${githubOwner}/${githubRepo}/actions/runs/${run.id}/jobs?per_page=100`,
        {
          headers: {
            "Accept": "application/vnd.github+json",
            "Authorization": `Bearer ${githubToken}`,
            "User-Agent": "contifico-refresh-status",
          },
        },
      );
      if (jobsResponse.ok) {
        jobsPayload = await jobsResponse.json();
      }
    }
    const mapped = mapWorkflowStatus(run, jobsPayload);
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
    if (error instanceof AuthError) {
      return json({ error: error.message }, error.status);
    }
    return json({ error: String(error instanceof Error ? error.message : error) }, 500);
  }
});
