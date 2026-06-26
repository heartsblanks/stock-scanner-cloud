const BACKEND_BASE_URL = String(
  process.env.BACKEND_BASE_URL || "https://stock-scanner-330056507478.europe-west1.run.app"
).replace(/\/$/, "");

export async function proxy(request, response, backendPath, allowedMethods = ["GET"]) {
  if (!allowedMethods.includes(request.method)) {
    response.setHeader("Allow", allowedMethods.join(", "));
    return response.status(405).json({ ok: false, error: "method_not_allowed" });
  }

  try {
    const url = new URL(BACKEND_BASE_URL + backendPath);

    if (request.query) {
      for (const [key, value] of Object.entries(request.query)) {
        if (value !== undefined && value !== null) {
          url.searchParams.set(key, String(value));
        }
      }
    }

    const options = { method: request.method, headers: { "Content-Type": "application/json" } };

    if (request.method !== "GET" && request.body && Object.keys(request.body).length > 0) {
      options.body = JSON.stringify(request.body);
    }

    const upstream = await fetch(url.toString(), options);
    const data = await upstream.json();
    return response.status(upstream.status).json(data);
  } catch (error) {
    return response.status(500).json({ ok: false, error: error?.message || "proxy_failed" });
  }
}
