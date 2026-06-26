const BACKEND_BASE_URL = String(
  process.env.BACKEND_BASE_URL || "https://stock-scanner-cloud.vercel.app"
).trim().replace(/\/$/, "");

export default async function handler(request, response) {
  // Vercel catch-all uses "...path" key (with dots), not "path"
  const rawPath = request.query["...path"] || request.query.path;
  const segments = Array.isArray(rawPath)
    ? rawPath
    : rawPath
    ? rawPath.split("/").filter(Boolean)
    : [];
  const path = "/" + segments.join("/");

  try {
    const url = new URL(BACKEND_BASE_URL + path);

    for (const [key, value] of Object.entries(request.query)) {
      if (key !== "...path" && key !== "path" && value !== undefined) {
        url.searchParams.set(key, String(value));
      }
    }

    const options = {
      method: request.method,
      headers: { "Content-Type": "application/json" },
    };

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
