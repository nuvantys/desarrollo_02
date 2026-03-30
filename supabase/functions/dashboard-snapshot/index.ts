import { createClient } from "npm:@supabase/supabase-js@2";
import { corsHeaders, json } from "../_shared/cors.ts";
import { AuthError, requireUser } from "../_shared/auth.ts";

const ALLOWED_FILES = new Set([
  "manifest.json",
  "overview.json",
  "commercial.json",
  "customers.json",
  "products.json",
  "inventory.json",
  "accounting.json",
  "quality.json",
  "technical.json",
  "tables.json",
  "database.json",
]);

Deno.serve(async (request) => {
  if (request.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  if (request.method !== "GET") {
    return json({ error: "Method not allowed" }, 405);
  }

  try {
    await requireUser(request);
    const url = new URL(request.url);
    const file = String(url.searchParams.get("file") || "").trim();
    if (!ALLOWED_FILES.has(file)) {
      return json({ error: "Archivo de snapshot no permitido." }, 400);
    }

    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    if (!supabaseUrl || !serviceRoleKey) {
      return json({ error: "Faltan variables de entorno de Supabase para leer el snapshot." }, 500);
    }
    const admin = createClient(supabaseUrl, serviceRoleKey);
    const { data, error } = await admin.rpc("get_snapshot_asset", { p_filename: file });

    if (error) {
      return json({ error: "No fue posible leer el snapshot privado.", details: error.message }, 500);
    }
    if (!data) {
      return json({ error: "El snapshot solicitado aun no existe en Supabase." }, 404);
    }

    return json(data);
  } catch (error) {
    if (error instanceof AuthError) {
      return json({ error: error.message }, error.status);
    }
    return json({ error: String(error instanceof Error ? error.message : error) }, 500);
  }
});
