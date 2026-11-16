"use client";

import { signIn } from "next-auth/react";

function GoogleMark() {
  return (
    <svg aria-hidden="true" viewBox="0 0 48 48" className="size-5">
      <path fill="#4285F4" d="M43.6 24.5c0-1.4-.1-2.7-.4-4H24v7.6h11a9.4 9.4 0 0 1-4.1 6.2v5h6.6c3.9-3.6 6.1-8.8 6.1-14.8Z" />
      <path fill="#34A853" d="M24 44c5.5 0 10.1-1.8 13.5-4.8l-6.6-5c-1.8 1.2-4 1.9-6.9 1.9-5.3 0-9.8-3.6-11.4-8.4H5.8v5.2A20 20 0 0 0 24 44Z" />
      <path fill="#FBBC05" d="M12.6 27.7a12 12 0 0 1 0-7.4v-5.2H5.8a20 20 0 0 0 0 17.8l6.8-5.2Z" />
      <path fill="#EA4335" d="M24 11.9c3 0 5.7 1 7.8 3.1l5.8-5.8C34.1 6 29.5 4 24 4A20 20 0 0 0 5.8 15.1l6.8 5.2c1.6-4.8 6.1-8.4 11.4-8.4Z" />
    </svg>
  );
}

export function SignInPanel({
  sessionExpired = false,
}: {
  sessionExpired?: boolean;
}) {
  const basePath = (process.env.NEXT_PUBLIC_BASE_PATH ?? "").replace(/\/+$/, "");

  function callbackUrl() {
    if (typeof window === "undefined") return `${basePath}/`;
    const url = new URL(window.location.href);
    url.searchParams.delete("session");
    return `${url.pathname}${url.search}`;
  }

  return (
    <main className="bg-background text-foreground flex min-h-dvh items-center justify-center p-5">
      <section className="w-full max-w-md rounded-2xl border bg-card p-8 shadow-sm">
        <div className="mb-8">
          <p className="text-muted-foreground text-xs font-semibold tracking-[0.2em]">
            ORIGINTRACER
          </p>
        </div>
        {sessionExpired && (
          <p className="text-muted-foreground mb-5 text-sm" role="status">
            Your sign-in session expired. Sign in again to continue.
          </p>
        )}
        <button
          type="button"
          className="h-11 w-full cursor-pointer rounded-lg border bg-white px-4 text-sm font-medium text-[#1f1f1f] hover:bg-[#f8faff]"
          onClick={() => signIn("keycloak", { redirectTo: callbackUrl() })}
        >
          Sign in or create an account
        </button>
        <div className="text-muted-foreground my-5 flex items-center gap-3 text-xs">
          <span className="h-px flex-1 bg-border" />or<span className="h-px flex-1 bg-border" />
        </div>
        <button
          type="button"
          className="flex h-11 w-full cursor-pointer items-center justify-center gap-3 rounded-lg border bg-white px-4 text-sm font-medium text-[#1f1f1f] hover:bg-[#f8faff]"
          onClick={() =>
            signIn("keycloak", { redirectTo: callbackUrl() }, { kc_idp_hint: "google" })
          }
        >
          <GoogleMark />
          Continue with Google
        </button>
      </section>
    </main>
  );
}
