export default async function handler(request, response) {
  if (request.method !== "POST") {
    response.setHeader("Allow", "POST");
    return response.status(405).json({ ok: false, error: "method_not_allowed" });
  }

  const adminToken = String(process.env.ADMIN_API_TOKEN || "").trim();
  const backendBaseUrl = String(
    process.env.BACKEND_BASE_URL || process.env.VITE_API_BASE_URL || "https://stock-scanner-330056507478.europe-west1.run.app"
  ).trim();

  if (!adminToken) {
    return response.status(503).json({ ok: false, error: "admin_api_token_not_configured" });
  }

  try {
    const upstream = await fetch(`${backendBaseUrl}/scheduler/test-day-cycle`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Admin-Token": adminToken,
      },
      body: JSON.stringify(request.body || {}),
    });

    const data = await upstream.json();
    return response.status(upstream.status).json(data);
  } catch (error) {
    return response.status(500).json({
      ok: false,
      error: error?.message || "proxy_request_failed",
    });
  }
}
