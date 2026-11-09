import { initApiPassthrough } from "langgraph-nextjs-api-passthrough";

// Proxy chat API requests to the configured OriginTracer Agent Server.

export const { GET, POST, PUT, PATCH, DELETE, OPTIONS, runtime } =
  initApiPassthrough({
    apiUrl: process.env.LANGGRAPH_API_URL ?? "http://127.0.0.1:8123",
    apiKey: process.env.LANGGRAPH_API_KEY,
    runtime: "nodejs",
  });
