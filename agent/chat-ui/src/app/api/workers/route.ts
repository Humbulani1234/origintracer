import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

export async function GET() {
  const backendUrl =
    process.env.ORIGINTRACER_API_URL ?? "http://127.0.0.1:8001";
  const apiKey = process.env.ORIGINTRACER_API_KEY ?? "test-key-123";

  try {
    const response = await fetch(`${backendUrl}/api/v1/workers`, {
      headers: { Authorization: `Bearer ${apiKey}` },
      cache: "no-store",
    });
    const body = await response.json().catch(() => ({}));
    return NextResponse.json(body, { status: response.status });
  } catch {
    return NextResponse.json(
      { detail: `Could not reach the OriginTracer backend at ${backendUrl}.` },
      { status: 502 },
    );
  }
}
