import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

export async function GET(): Promise<NextResponse> {
  const backendUrl = process.env.ORIGINTRACER_API_URL ?? "http://127.0.0.1:8001";
  const apiKey = process.env.ORIGINTRACER_API_KEY ?? "test-key-123";

  try {
    const response = await fetch(
      `${backendUrl.replace(/\/+$/, "")}/api/v1/snapshots`,
      {
        headers: {
          Authorization: `Bearer ${apiKey}`,
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
        detail: `Could not reach the OriginTracer backend at ${backendUrl}.`,
      },
      { status: 502 }
    );
  }
}