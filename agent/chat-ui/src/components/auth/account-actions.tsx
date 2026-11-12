"use client";

import Link from "next/link";
import { signOut } from "next-auth/react";
import { LogOut, UserRound } from "lucide-react";

export function AccountActions() {
  const basePath = (process.env.NEXT_PUBLIC_BASE_PATH ?? "").replace(/\/+$/, "");

  return (
    <div className="fixed top-3 right-3 z-40 flex items-center gap-2 rounded-full border bg-background/95 p-1.5 shadow-sm backdrop-blur">
      <Link
        href={`${basePath}/account`}
        className="text-muted-foreground hover:text-foreground flex max-w-40 items-center gap-2 truncate rounded-full px-2 py-1 text-xs"
      >
        <UserRound className="size-4 shrink-0" />
        <span className="truncate">Account</span>
      </Link>
      <button
        type="button"
        aria-label="Sign out"
        title="Sign out"
        className="hover:bg-muted rounded-full p-2"
        onClick={() => signOut({ redirectTo: `${basePath}/` })}
      >
        <LogOut className="size-4" />
      </button>
    </div>
  );
}
