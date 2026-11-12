import { NextResponse } from "next/server";
import { originTracerAuthorization } from "@/lib/backend-auth";

export const dynamic = "force-dynamic";

const backendUrl = () =>
  (process.env.ORIGINTRACER_API_URL ?? "http://127.0.0.1:8001").replace(/\/+$/, "");

async function proxy(request: Request, method: "GET" | "POST") {
  const authorization = await originTracerAuthorization();
  if (!authorization) {
    return NextResponse.json({ detail: "Sign in is required." }, { status: 401 });
  }

  const headers = new Headers({ Authorization: authorization });
  let body: string | undefined;
  if (method === "POST") {
    headers.set("Content-Type", "application/json");
    body = await request.text();
  }
  try {
    const response = await fetch(`${backendUrl()}/api/v1/api-keys`, {
      method,
      headers,
      body,
      cache: "no-store",
    });
    return new NextResponse(response.body, {
      status: response.status,
      headers: {
        "Content-Type": response.headers.get("Content-Type") ?? "application/json",
      },
    });
  } catch {
    return NextResponse.json(
      { detail: "Could not reach the OriginTracer backend." },
      { status: 502 },
    );
  }
}

export const GET = (request: Request) => proxy(request, "GET");
export const POST = (request: Request) => proxy(request, "POST");
