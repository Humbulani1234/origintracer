import { auth, oidcAuthEnabled } from "@/auth";

export async function originTracerAuthorization(): Promise<string | null> {
  if (oidcAuthEnabled) {
    const session = await auth();
    return session?.accessToken && !session.error
      ? `Bearer ${session.accessToken}`
      : null;
  }
  if (process.env.NODE_ENV !== "production") {
    return `Bearer ${process.env.ORIGINTRACER_API_KEY ?? "test-key-123"}`;
  }
  return null;
}
