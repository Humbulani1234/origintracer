import { NextResponse } from "next/server";
import { originTracerAuthorization } from "@/lib/backend-auth";

export const dynamic = "force-dynamic";

export async function DELETE(
  _request: Request,
  context: { params: Promise<{ key_id: string }> },
) {
  const authorization = await originTracerAuthorization();
  if (!authorization) {
    return NextResponse.json({ detail: "Sign in is required." }, { status: 401 });
  }
  const { key_id: keyId } = await context.params;
  const backendUrl = (process.env.ORIGINTRACER_API_URL ?? "http://127.0.0.1:8001").replace(/\/+$/, "");
  try {
    const response = await fetch(
      `${backendUrl}/api/v1/api-keys/${encodeURIComponent(keyId)}`,
      {
        method: "DELETE",
        headers: { Authorization: authorization },
        cache: "no-store",
      },
    );
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
