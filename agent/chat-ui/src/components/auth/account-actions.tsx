"use client";

import { signOut } from "next-auth/react";
import { LoaderCircle, LogOut, UserRound } from "lucide-react";
import { useState } from "react";

export function AccountActions() {
  const basePath = (process.env.NEXT_PUBLIC_BASE_PATH ?? "").replace(/\/+$/, "");
  const [signingOut, setSigningOut] = useState(false);

  return (
    <div className="fixed top-3 right-3 z-40 flex items-center gap-2 rounded-full border bg-background/95 p-1.5 shadow-sm backdrop-blur">
      <a
        href={`${basePath}/account`}
        className="text-muted-foreground hover:text-foreground flex max-w-40 items-center gap-2 truncate rounded-full px-2 py-1 text-xs"
      >
        <UserRound className="size-4 shrink-0" />
        <span className="truncate">Account</span>
      </a>
      <button
        type="button"
        aria-label="Sign out"
        title={signingOut ? "Signing out…" : "Sign out"}
        disabled={signingOut}
        aria-busy={signingOut}
        className="cursor-pointer rounded-full p-2 hover:bg-muted disabled:cursor-wait"
        onClick={() => {
          setSigningOut(true);
          void signOut({ redirectTo: `${basePath}/` }).catch((error) => {
            console.error("Could not sign out", error);
            setSigningOut(false);
          });
        }}
      >
        {signingOut ? (
          <LoaderCircle className="size-4 animate-spin" />
        ) : (
          <LogOut className="size-4" />
        )}
      </button>
    </div>
  );
}
