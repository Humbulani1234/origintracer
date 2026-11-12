import { AccountActions } from "@/components/auth/account-actions";
import { SignInPanel } from "@/components/auth/sign-in-panel";
import { ChatApp } from "@/components/chat-app";
import { auth, oidcAuthEnabled } from "@/auth";
import React from "react";

export default async function DemoPage(): Promise<React.ReactNode> {
  const session = oidcAuthEnabled ? await auth() : null;
  if (oidcAuthEnabled && (!session?.accessToken || session.error)) {
    return <SignInPanel />;
  }

  return (
    <>
      {oidcAuthEnabled && <AccountActions />}
      <ChatApp
        oidcEnabled={oidcAuthEnabled}
        authenticated={Boolean(session?.accessToken && !session.error)}
      />
    </>
  );
}
