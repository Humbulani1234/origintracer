import { NextResponse } from "next/server";
import { originTracerAuthorization } from "@/lib/backend-auth";

export const dynamic = "force-dynamic";

export async function GET(): Promise<NextResponse> {
  const backendUrl = process.env.ORIGINTRACER_API_URL ?? "http://127.0.0.1:8001";
  const authorization = await originTracerAuthorization();
  if (!authorization) {
    return NextResponse.json(
      { detail: "Sign in is required to view workers." },
      { status: 401 },
    );
  }

  try {
    const response = await fetch(
      `${backendUrl.replace(/\/+$/, "")}/api/v1/snapshots`,
      {
        headers: {
          Authorization: authorization,
        },
        cache: "no-store",
      }
    );

    const body = await response.json().catch(() => ({}));

    if (!response.ok) {
      return NextResponse.json(body, {
        status: response.status,
      });
    }

    const workers = Array.isArray(body?.data)
      ? body.data
          .filter((worker: any) => worker.online === true)
          .map((worker: any) => ({
            ...worker,
            pid: String(worker.worker_pid),
          }))
      : [];

    return NextResponse.json({ data: workers });
  } catch (error) {
    console.error("[api/workers] Backend request failed:", error);

    return NextResponse.json(
      {
        detail: "Could not reach the OriginTracer backend.",
      },
      { status: 502 }
    );
  }
}
