import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { useThreads } from "@/providers/Thread";
import { WorkerSelector } from "@/providers/Worker";
import { Thread } from "@langchain/langgraph-sdk";
import {
  BookOpen,
  Code2,
  Database,
  CircleHelp,
  MessageSquare,
  Moon,
  Server,
  SquarePen,
  Sun,
  Trash2,
  UserRound,
} from "lucide-react";
import { useQueryState } from "nuqs";
import { useEffect, useState } from "react";
import { getContentString } from "../utils";
import { useTheme } from "next-themes";
import { toast } from "sonner";
import * as Dialog from "@radix-ui/react-dialog";
import { X } from "lucide-react";
import { isStaticDemo } from "@/lib/static-demo";

function ThreadList({
  threads,
  loading,
  onDeleteThread,
  onThreadSelect,
}: {
  threads: Thread[];
  loading: boolean;
  onDeleteThread: (threadId: string) => Promise<void>;
  onThreadSelect?: () => void;
}) {
  const [threadId, setThreadId] = useQueryState("threadId");
  const [deletingThreadId, setDeletingThreadId] = useState<string | null>(null);
  const [confirmingDeleteThreadId, setConfirmingDeleteThreadId] =
    useState<string | null>(null);

  if (loading) {
    return (
      <div className="space-y-2 px-2">
        {Array.from({ length: 5 }).map((_, index) => (
          <Skeleton
            key={index}
            className="h-9 w-full"
          />
        ))}
      </div>
    );
  }

  if (!threads.length) {
    return (
      <p className="text-muted-foreground px-4 py-3 text-sm">
        Your conversations will appear here.
      </p>
    );
  }

  return (
    <div className="space-y-1 px-2">
      {threads.map((thread) => {
        let label = "New conversation";
        const values = thread.values;
        if (
          typeof values === "object" &&
          values &&
          "messages" in values &&
          Array.isArray(values.messages) &&
          values.messages.length > 0
        ) {
          label = getContentString(values.messages[0].content) || label;
        }

        const selected = thread.thread_id === threadId;
        const confirmingDelete = confirmingDeleteThreadId === thread.thread_id;
        return (
          <div
            key={thread.thread_id}
            className="group flex items-center gap-1"
          >
            {confirmingDelete ? (
              <div
                role="group"
                aria-label={`Confirm deletion of ${label}`}
                className="bg-destructive/10 flex h-9 min-w-0 flex-1 items-center gap-1 rounded-md px-2"
              >
                <span className="min-w-0 flex-1 truncate text-xs">
                  Delete conversation?
                </span>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  className="h-7 px-2 text-xs"
                  disabled={deletingThreadId === thread.thread_id}
                  onClick={() => setConfirmingDeleteThreadId(null)}
                >
                  Cancel
                </Button>
                <Button
                  type="button"
                  variant="destructive"
                  size="sm"
                  className="h-7 px-2 text-xs"
                  disabled={deletingThreadId === thread.thread_id}
                  onClick={async () => {
                    setDeletingThreadId(thread.thread_id);
                    try {
                      await onDeleteThread(thread.thread_id);
                      setConfirmingDeleteThreadId(null);
                      if (selected) await setThreadId(null);
                      onThreadSelect?.();
                    } catch (error) {
                      toast.error("Could not delete conversation", {
                        description:
                          error instanceof Error
                            ? error.message
                            : "Please try again.",
                      });
                    } finally {
                      setDeletingThreadId(null);
                    }
                  }}
                >
                  {deletingThreadId === thread.thread_id
                    ? "Deleting…"
                    : "Delete"}
                </Button>
              </div>
            ) : (
              <>
                <Button
                  variant="ghost"
                  className={`h-9 min-w-0 flex-1 justify-start overflow-hidden px-3 text-left font-normal ${
                    selected
                      ? "bg-sidebar-accent text-sidebar-accent-foreground font-medium"
                      : ""
                  }`}
                  title={label}
                  onClick={() => {
                    if (!selected) void setThreadId(thread.thread_id);
                    onThreadSelect?.();
                  }}
                >
                  <MessageSquare className="text-muted-foreground size-4 shrink-0" />
                  <span className="truncate">{label}</span>
                </Button>
                <Button
                  variant="ghost"
                  size="icon"
                  className="size-8 shrink-0 opacity-100 lg:opacity-0 lg:group-focus-within:opacity-100 lg:group-hover:opacity-100"
                  aria-label={`Delete conversation: ${label}`}
                  title="Delete conversation"
                  onClick={() =>
                    setConfirmingDeleteThreadId(thread.thread_id)
                  }
                >
                  <Trash2 className="size-4" />
                </Button>
              </>
            )}
          </div>
        );
      })}
    </div>
  );
}

function HowItWorksDialog() {
  const examples = [
    "Is this worker healthy, and what is it doing?",
    "Which operations are called most often?",
    "Show this worker’s graph and explain its busiest paths.",
    "Compare database latency with asyncio activity.",
    "Find a recent request trace and summarize its stages.",
    "Show saved causal-rule matches for this worker.",
  ];

  return (
    <Dialog.Root>
      <Dialog.Trigger asChild>
        <button
          type="button"
          className="hover:bg-sidebar-accent flex h-9 w-full items-center gap-3 rounded-lg px-3 text-left text-sm"
        >
          <CircleHelp className="text-muted-foreground size-4" />
          How it works
        </button>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-black/40" />
        <Dialog.Content className="bg-background text-foreground fixed top-1/2 left-1/2 z-50 max-h-[85dvh] w-[min(92vw,42rem)] -translate-x-1/2 -translate-y-1/2 overflow-y-auto rounded-2xl border p-6 shadow-xl">
          <Dialog.Close asChild>
            <button
              type="button"
              aria-label="Close guide"
              className="text-muted-foreground hover:bg-muted absolute top-4 right-4 rounded-md p-2"
            >
              <X className="size-4" />
            </button>
          </Dialog.Close>
          <Dialog.Title className="pr-10 text-xl font-semibold">
            How OriginTracer works
          </Dialog.Title>
          <Dialog.Description className="text-muted-foreground mt-2 text-sm leading-6">
            Ask in everyday language. OriginTracer gathers evidence from the
            selected worker and summarizes what the data shows.
          </Dialog.Description>

          <section className="mt-6">
            <h3 className="text-sm font-semibold">Questions you can ask</h3>
            <ul className="mt-2 grid gap-2 sm:grid-cols-2">
              {examples.map((example) => (
                <li
                  key={example}
                  className="bg-muted/50 rounded-lg px-3 py-2 text-sm leading-5"
                >
                  {example}
                </li>
              ))}
            </ul>
          </section>

          <section className="mt-6">
            <h3 className="text-sm font-semibold">
              A few useful things to know
            </h3>
            <ul className="text-muted-foreground mt-2 list-disc space-y-2 pl-5 text-sm leading-6">
              <li>
                The Worker selector in the rail sets the target for worker
                questions. Ask for an overview when you want all live workers.
              </li>
              <li>
                You don’t need special commands or exact phrases. Say what you
                want to understand and add details naturally.
              </li>
              <li>
                Replies cite their evidence. A detected pattern is an
                observation; it does not by itself prove a root cause.
              </li>
              <li>
                Status, graph, trace, event, and rule evidence may appear as
                compact tables or timelines inside the reply. Open evidence
                details when you need the underlying data.
              </li>
            </ul>
          </section>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function SidebarContents({
  onNewThread,
  onThreadSelect,
  mobile = false,
}: {
  onNewThread: () => void;
  onThreadSelect?: () => void;
  mobile?: boolean;
}) {
  const agentApiDocsUrl =
    process.env.NEXT_PUBLIC_AGENT_API_DOCS_URL ?? "http://127.0.0.1:2024/docs";
  const backendApiDocsUrl =
    process.env.NEXT_PUBLIC_BACKEND_API_DOCS_URL ?? "http://127.0.0.1:8001/docs";
  const showAgentApiDocs =
    !isStaticDemo || agentApiDocsUrl.startsWith("https://");
  const showBackendApiDocs =
    !isStaticDemo || backendApiDocsUrl.startsWith("https://");

  const {
    getThreads,
    deleteThread,
    threads,
    setThreads,
    threadsLoading,
    setThreadsLoading,
  } = useThreads();
  const { resolvedTheme, setTheme } = useTheme();
  const [themeReady, setThemeReady] = useState(false);
  const isDarkMode = resolvedTheme === "dark";

  useEffect(() => {
    setThemeReady(true);
  }, []);

  useEffect(() => {
    setThreadsLoading(true);
    getThreads()
      .then(setThreads)
      .catch(console.error)
      .finally(() => setThreadsLoading(false));
  }, [getThreads, setThreads, setThreadsLoading]);

  return (
    <div className="bg-sidebar text-sidebar-foreground flex h-full min-h-0 flex-col">
      <div className={`px-3 pb-3 ${mobile ? "pt-14" : "pt-4"}`}>
        <div className="text-muted-foreground px-1 pb-2 text-xs font-semibold tracking-wide uppercase">
          Explore
        </div>
        <a
          href="https://origintracer.app/docs/"
          target="_blank"
          rel="noreferrer"
          className="hover:bg-sidebar-accent flex h-9 items-center gap-3 rounded-lg px-3 text-sm"
        >
          <BookOpen className="text-muted-foreground size-4" />
          Documentation
        </a>
        <HowItWorksDialog />
        {showAgentApiDocs && (
          <a
            href={agentApiDocsUrl}
            target="_blank"
            rel="noreferrer"
            className="hover:bg-sidebar-accent flex h-9 items-center gap-3 rounded-lg px-3 text-sm"
          >
            <Code2 className="text-muted-foreground size-4" />
            Agent API
          </a>
        )}
        {showBackendApiDocs && (
          <a
            href={backendApiDocsUrl}
            target="_blank"
            rel="noreferrer"
            className="hover:bg-sidebar-accent flex h-9 items-center gap-3 rounded-lg px-3 text-sm"
          >
            <Database className="text-muted-foreground size-4" />
            OriginTracer API
          </a>
        )}
      </div>

      <div className="border-sidebar-border border-t px-3 pt-3 pb-3">
        <div className="text-muted-foreground px-1 pb-2 text-xs font-semibold tracking-wide uppercase">
          Chats
        </div>
        <Button
          variant="outline"
          className="border-sidebar-border bg-background hover:bg-accent h-9 w-full justify-start gap-2 font-medium"
          onClick={onNewThread}
        >
          <SquarePen className="size-4" />
          New chat
        </Button>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto pb-3">
        <ThreadList
          threads={threads}
          loading={threadsLoading}
          onDeleteThread={deleteThread}
          onThreadSelect={onThreadSelect}
        />
      </div>

      <div className="border-sidebar-border space-y-1 border-t p-3">
        <WorkerSelector />
        <div className="flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm">
          <Server className="text-muted-foreground size-4 shrink-0" />
          <div className="min-w-0 flex-1">
            <div className="font-medium">Agent server</div>
            <div className="text-muted-foreground truncate text-xs">
              {isStaticDemo
                ? "Demo frontend"
                : (process.env.NEXT_PUBLIC_AGENT_SERVER_LABEL ??
                  "127.0.0.1:2024")}
            </div>
          </div>
        </div>
        <Button
          variant="ghost"
          className="text-foreground h-10 w-full justify-start gap-3 px-3"
          aria-label={
            themeReady
              ? isDarkMode
                ? "Switch to light mode"
                : "Switch to dark mode"
              : "Color theme"
          }
          title={
            themeReady
              ? isDarkMode
                ? "Switch to light mode"
                : "Switch to dark mode"
              : "Color theme"
          }
          onClick={() => setTheme(isDarkMode ? "light" : "dark")}
        >
          {themeReady && isDarkMode ? (
            <Sun className="size-4" />
          ) : (
            <Moon className="size-4" />
          )}
          {themeReady ? (isDarkMode ? "Light mode" : "Dark mode") : "Theme"}
        </Button>
        <Button
          variant="ghost"
          className="h-10 w-full justify-start gap-3 px-3"
          disabled
          title="Sign-in will be available when accounts are configured."
        >
          <UserRound className="text-muted-foreground size-4" />
          Sign in
        </Button>
      </div>
    </div>
  );
}

export default function ThreadHistory({
  mobileOpen,
  onMobileOpenChange,
  onNewThread,
}: {
  mobileOpen: boolean;
  onMobileOpenChange: (open: boolean) => void;
  onNewThread: () => void;
}) {
  return (
    <>
      <aside className="border-sidebar-border hidden h-full w-72 shrink-0 border-r lg:block">
        <SidebarContents onNewThread={onNewThread} />
      </aside>
      <div className="lg:hidden">
        <Sheet
          open={mobileOpen}
          onOpenChange={onMobileOpenChange}
        >
          <SheetContent
            side="left"
            className="w-[300px] p-0"
          >
            <SheetHeader className="sr-only">
              <SheetTitle>OriginTracer navigation</SheetTitle>
            </SheetHeader>
            <SidebarContents
              mobile
              onNewThread={() => {
                onNewThread();
                onMobileOpenChange(false);
              }}
              onThreadSelect={() => onMobileOpenChange(false)}
            />
          </SheetContent>
        </Sheet>
      </div>
    </>
  );
}
