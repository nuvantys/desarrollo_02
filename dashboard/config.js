window.CONTIFICO_CONFIG = {
  snapshotBase: "./data",
  bootstrapApiUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/dashboard-bootstrap",
  snapshotApiUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/dashboard-snapshot",
  refreshApiUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/contifico-refresh",
  refreshStatusUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/contifico-refresh-status",
  supabaseUrl: "",
  supabaseAnonKey: "",
  simpleLogin: {
    email: "admin@nuvantys.com",
    password: "Nuvant@1410",
  },
  hostedHint: "Publica esta carpeta en Vercel, Netlify o Pages. El dashboard usa login simple de frontend, lee snapshots publicados y dispara el refresh cloud por Edge Functions.",
};
