import React, {
  createContext,
  useContext,
  ReactNode,
  useState,
  useEffect,
  useMemo,
} from "react";
import { useStream } from "@langchain/langgraph-sdk/react";
import { type Message } from "@langchain/langgraph-sdk";
import {
  uiMessageReducer,
  isUIMessage,
  isRemoveUIMessage,
  type UIMessage,
  type RemoveUIMessage,
} from "@langchain/langgraph-sdk/react-ui";
import { useQueryState } from "nuqs";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { ArrowRight } from "lucide-react";
import { PasswordInput } from "@/components/ui/password-input";
import { getApiKey, setApiKey as storeApiKey } from "@/lib/api-key";
import { resolveApiUrl } from "@/lib/resolve-api-url";
import { useThreads } from "./Thread";
import { toast } from "sonner";
import { isStaticDemo } from "@/lib/static-demo";

export type StateType = { messages: Message[]; ui?: UIMessage[] };

const useTypedStream = useStream<
  StateType,
  {
    UpdateType: {
      messages?: Message[] | Message | string;
      ui?: (UIMessage | RemoveUIMessage)[] | UIMessage | RemoveUIMessage;
      context?: Record<string, unknown>;
      worker_pid?: number;
    };
    CustomEventType: UIMessage | RemoveUIMessage;
  }
>;

type StreamContextType = ReturnType<typeof useTypedStream> & {
  apiUrl: string;
};
const StreamContext = createContext<StreamContextType | undefined>(undefined);

async function sleep(ms = 4000) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function checkGraphStatus(
  apiUrl: string,
  apiKey: string | null,
  authScheme?: string,
): Promise<boolean> {
  try {
    const headers = new Headers();
    if (apiKey) headers.set("X-Api-Key", apiKey);
    if (authScheme) headers.set("X-Auth-Scheme", authScheme);

    const res = await fetch(`${apiUrl}/info`, { headers });
    return res.ok;
  } catch (error) {
    console.error(error);
    return false;
  }
}

const StreamSession = ({
  children,
  apiKey,
  apiUrl,
  assistantId,
  authScheme,
}: {
  children: ReactNode;
  apiKey: string | null;
  apiUrl: string;
  assistantId: string;
  authScheme?: string;
}) => {
  const [threadId, setThreadId] = useQueryState("threadId");
  const { getThreads, setThreads } = useThreads();
  const streamValue = useTypedStream({
    apiUrl,
    apiKey: apiKey ?? undefined,
    assistantId,
    ...(authScheme && {
      defaultHeaders: {
        "X-Auth-Scheme": authScheme,
      },
    }),
    threadId: threadId ?? null,
    fetchStateHistory: { limit: 100 },
    onCustomEvent: (event, options) => {
      if (isUIMessage(event) || isRemoveUIMessage(event)) {
        options.mutate((prev) => {
          const ui = uiMessageReducer(prev.ui ?? [], event);
          return { ...prev, ui };
        });
      }
    },
    onThreadId: (id) => {
      setThreadId(id);
      // Refetch threads list when thread ID changes.
      // Wait for some seconds before fetching so we're able to get the new thread that was created.
      sleep().then(() => getThreads().then(setThreads).catch(console.error));
    },
  });

  useEffect(() => {
    const streamError = streamValue.error;
    const message =
      streamError instanceof Error
        ? streamError.message
        : String(streamError ?? "");
    if (!message.includes("Your sign-in session expired.")) return;

    const url = new URL(window.location.href);
    url.searchParams.set("session", "expired");
    window.location.replace(`${url.pathname}${url.search}`);
  }, [streamValue.error]);

  useEffect(() => {
    if (
      isStaticDemo ||
      process.env.NEXT_PUBLIC_CHECK_AGENT_CONNECTION !== "true"
    ) {
      return;
    }

    checkGraphStatus(apiUrl, apiKey, authScheme).then((ok) => {
      if (!ok) {
        toast.error("Could not connect to the OriginTracer agent", {
          description: () => (
            <p>
              Check that the OriginTracer agent is running and reachable at{" "}
              <code>{apiUrl}</code>.
            </p>
          ),
          duration: 10000,
          richColors: true,
          closeButton: true,
        });
      }
    });
  }, [apiKey, apiUrl, authScheme]);

  return (
    <StreamContext.Provider value={{ ...streamValue, apiUrl }}>
      {children}
    </StreamContext.Provider>
  );
};

// Default values for the form
const DEFAULT_API_URL = "http://localhost:2024";
const DEFAULT_ASSISTANT_ID = "agent";
const AGENT_BUILDER_AUTH_SCHEME = "langsmith-api-key";

export const StreamProvider: React.FC<{ children: ReactNode }> = ({
  children,
}) => {
  // Get environment variables
  const envApiUrl: string | undefined = process.env.NEXT_PUBLIC_API_URL;
  const envAssistantId: string | undefined =
    process.env.NEXT_PUBLIC_ASSISTANT_ID;
  const envAuthScheme: string | undefined = process.env.NEXT_PUBLIC_AUTH_SCHEME;

  // Use URL params with env var fallbacks
  const [apiUrl, setApiUrl] = useQueryState("apiUrl", {
    defaultValue: envApiUrl || "",
  });
  const [assistantId, setAssistantId] = useQueryState("assistantId", {
    defaultValue: envAssistantId || "",
  });
  const [authScheme, setAuthScheme] = useQueryState("authScheme", {
    defaultValue: envAuthScheme || "",
  });
  const [isAgentBuilder, setIsAgentBuilder] = useState(
    () =>
      (authScheme || envAuthScheme || "").toLowerCase() ===
      AGENT_BUILDER_AUTH_SCHEME,
  );

  const finalApiUrl = resolveApiUrl(apiUrl, envApiUrl);
  const finalAssistantId = assistantId || envAssistantId;
  const finalAuthScheme = authScheme || envAuthScheme || "";

  const apiKey = useMemo(() => getApiKey(finalApiUrl) || "", [finalApiUrl]);

  // Show the form if we: don't have an API URL, or don't have an assistant ID
  if (!finalApiUrl || !finalAssistantId) {
    return (
      <div className="flex min-h-screen w-full items-center justify-center p-4">
        <div className="animate-in fade-in-0 zoom-in-95 bg-background flex max-w-3xl flex-col rounded-lg border shadow-lg">
          <div className="mt-14 flex flex-col gap-2 border-b p-6">
            <div className="flex flex-col items-start gap-2">
              <h1 className="text-xl font-semibold tracking-tight">
                OriginTracer
              </h1>
            </div>
            <p className="text-muted-foreground">
              Connect to your OriginTracer agent. Local development is usually
              preconfigured; change these values only when needed.
            </p>
          </div>
          <form
            onSubmit={(e) => {
              e.preventDefault();

              const form = e.target as HTMLFormElement;
              const formData = new FormData(form);
              const apiUrl = formData.get("apiUrl") as string;
              const assistantId = formData.get("assistantId") as string;
              const apiKey = formData.get("apiKey") as string;

              setApiUrl(apiUrl);
              storeApiKey(apiUrl, apiKey);
              setAssistantId(assistantId);
              setAuthScheme(isAgentBuilder ? AGENT_BUILDER_AUTH_SCHEME : "");

              form.reset();
            }}
            className="bg-muted/50 flex flex-col gap-6 p-6"
          >
            <div className="flex flex-col gap-2">
              <Label htmlFor="apiUrl">
                Agent API URL<span className="text-rose-500">*</span>
              </Label>
              <p className="text-muted-foreground text-sm">
                The address where the OriginTracer agent accepts requests.
              </p>
              <Input
                id="apiUrl"
                name="apiUrl"
                className="bg-background"
                defaultValue={apiUrl || DEFAULT_API_URL}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="assistantId">
                Agent ID<span className="text-rose-500">*</span>
              </Label>
              <p className="text-muted-foreground text-sm">
                The identifier for the OriginTracer agent you want to chat
                with.
              </p>
              <Input
                id="assistantId"
                name="assistantId"
                className="bg-background"
                defaultValue={assistantId || DEFAULT_ASSISTANT_ID}
                required
              />
            </div>

            <div className="flex flex-col gap-2">
              <Label htmlFor="apiKey">API key</Label>
              <p className="text-muted-foreground text-sm">
                Leave this empty for the local development server. If required,
                it is used to authenticate requests to the configured agent.
              </p>
              <PasswordInput
                id="apiKey"
                name="apiKey"
                defaultValue={apiKey ?? ""}
                className="bg-background"
                placeholder="lsv2_pt_..."
              />
            </div>

            <div className="flex flex-col gap-3">
              <div className="flex items-center justify-between gap-4">
                <div className="flex flex-col gap-1">
                  <Label htmlFor="agentBuilderEnabled">
                    Hosted agent authentication
                  </Label>
                  <p className="text-muted-foreground text-sm">
                    Enable only if your hosted agent requires this sign-in
                    method.
                  </p>
                </div>
                <Switch
                  id="agentBuilderEnabled"
                  checked={isAgentBuilder}
                  onCheckedChange={setIsAgentBuilder}
                />
              </div>
            </div>

            <div className="mt-2 flex justify-end">
              <Button
                type="submit"
                size="lg"
              >
                Continue
                <ArrowRight className="size-5" />
              </Button>
            </div>
          </form>
        </div>
      </div>
    );
  }

  return (
    <StreamSession
      apiKey={apiKey}
      apiUrl={finalApiUrl}
      assistantId={finalAssistantId}
      authScheme={finalAuthScheme || undefined}
    >
      {children}
    </StreamSession>
  );
};

// Create a custom hook to use the context
export const useStreamContext = (): StreamContextType => {
  const context = useContext(StreamContext);
  if (context === undefined) {
    throw new Error("useStreamContext must be used within a StreamProvider");
  }
  return context;
};

export default StreamContext;
