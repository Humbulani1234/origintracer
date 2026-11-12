"use client";

import React from "react";
import { Thread } from "@/components/thread";
import { ArtifactProvider } from "@/components/thread/artifact";
import { Toaster } from "@/components/ui/sonner";
import { StreamProvider } from "@/providers/Stream";
import { ThreadProvider } from "@/providers/Thread";
import { WorkerProvider } from "@/providers/Worker";

export function ChatApp({
  oidcEnabled,
  authenticated,
}: {
  oidcEnabled: boolean;
  authenticated: boolean;
}) {
  return (
    <React.Suspense fallback={<div>Loading (layout)...</div>}>
      <Toaster />
      <WorkerProvider>
        <ThreadProvider>
          <StreamProvider>
            <ArtifactProvider>
              <Thread
                oidcEnabled={oidcEnabled}
                authenticated={authenticated}
              />
            </ArtifactProvider>
          </StreamProvider>
        </ThreadProvider>
      </WorkerProvider>
    </React.Suspense>
  );
}
