

import axios from "axios";

// In production (Vercel), all API calls go through /api/* Vercel Functions which proxy to the backend.
// In local dev, set VITE_API_BASE_URL=http://localhost:8080 in .env.local to hit Flask directly.
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "/api";

export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    "Content-Type": "application/json",
  },
  timeout: 15000,
});

apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    const message =
      error?.response?.data?.error ||
      error?.message ||
      "Unexpected API error";

    return Promise.reject(new Error(message));
  }
);

export default apiClient;