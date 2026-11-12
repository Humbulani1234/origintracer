import { initApiPassthrough } from "langgraph-nextjs-api-passthrough";
import { auth, oidcAuthEnabled } from "@/auth";

// Proxy chat API requests to the configured OriginTracer Agent Server.

export const { GET, POST, PUT, PATCH, DELETE, OPTIONS, runtime } =
  initApiPassthrough({
    apiUrl: process.env.LANGGRAPH_API_URL ?? "http://127.0.0.1:8123",
    apiKey: process.env.LANGGRAPH_API_KEY,
    runtime: "nodejs",
    headers: async (): Promise<Record<string, string>> => {
      if (!oidcAuthEnabled) return {};
      const session = await auth();
      if (!session?.accessToken) {
        throw new Error("Sign in is required to use the OriginTracer agent.");
      }
      return { Authorization: `Bearer ${session.accessToken}` };
    },
    disableWarningLog: true,
  });
