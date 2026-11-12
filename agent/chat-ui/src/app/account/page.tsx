import { auth, oidcAuthEnabled } from "@/auth";
import { AccountActions } from "@/components/auth/account-actions";
import { ApiKeyManager } from "@/components/auth/api-key-manager";
import { SignInPanel } from "@/components/auth/sign-in-panel";

export default async function AccountPage() {
  const session = oidcAuthEnabled ? await auth() : null;
  if (oidcAuthEnabled && (!session?.accessToken || session.error)) {
    return <SignInPanel />;
  }

  const basePath = (process.env.NEXT_BASE_PATH ?? "").replace(/\/+$/, "");
  return (
    <main className="bg-background text-foreground min-h-dvh p-5 sm:p-8">
      {oidcAuthEnabled && <AccountActions />}
      <div className="mx-auto max-w-3xl">
        <a
          href={`${basePath}/`}
          className="text-muted-foreground hover:text-foreground text-sm"
        >
          ← Back to chat
        </a>
        <h1 className="mt-6 text-2xl font-semibold">Account and worker keys</h1>
        <p className="text-muted-foreground mt-2 max-w-2xl text-sm leading-6">
          Create a separate key for each instrumented process. A key is shown once;
          copy it into that process’s environment and revoke it when it is no longer needed.
        </p>
        <div className="mt-8">
          <ApiKeyManager enabled={oidcAuthEnabled} />
        </div>
      </div>
    </main>
  );
}
