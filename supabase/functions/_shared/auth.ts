import { createClient } from "npm:@supabase/supabase-js@2";

export class AuthError extends Error {
  status: number;

  constructor(message: string, status = 401) {
    super(message);
    this.status = status;
  }
}

function env(name: string): string {
  const value = Deno.env.get(name);
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

export function requireBearerToken(request: Request): string {
  const header = request.headers.get("Authorization") || "";
  const match = header.match(/^Bearer\s+(.+)$/i);
  const token = match?.[1]?.trim();
  if (!token) {
    throw new AuthError("Se requiere una sesion valida para usar este endpoint.", 401);
  }
  return token;
}

export async function requireUser(request: Request) {
  const token = requireBearerToken(request);
  const supabaseUrl = env("SUPABASE_URL");
  const supabaseAnonKey = env("SUPABASE_ANON_KEY");
  const authClient = createClient(supabaseUrl, supabaseAnonKey);
  const { data, error } = await authClient.auth.getUser(token);
  if (error || !data.user) {
    throw new AuthError("La sesion no es valida o ya expiro.", 401);
  }
  return data.user;
}

export function createAdminClient() {
  const supabaseUrl = env("SUPABASE_URL");
  const serviceRoleKey = env("SUPABASE_SERVICE_ROLE_KEY");
  return createClient(supabaseUrl, serviceRoleKey);
}
