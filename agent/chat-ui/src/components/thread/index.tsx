import { v4 as uuidv4 } from "uuid";
import { Fragment, ReactNode, useEffect, useRef, useState, type FormEvent } from "react";
import { cn } from "@/lib/utils";
import { useStreamContext } from "@/providers/Stream";
import { useWorker } from "@/providers/Worker";
import { GitHubSVG } from "@/components/icons/github";
import { Button } from "../ui/button";
import { Checkpoint, Message } from "@langchain/langgraph-sdk";
import { ActionApprovalInterrupt, AssistantMessage, AssistantMessageLoading } from "./messages/ai";
import { HumanMessage } from "./messages/human";
import {
  DO_NOT_RENDER_ID_PREFIX,
  ensureToolCallsHaveResponses,
} from "@/lib/ensure-tool-responses";
import {
  ArrowDown,
  ArrowUp,
  BookOpen,
  Menu,
  Square,
  Users,
  XIcon,
} from "lucide-react";
import { useQueryState } from "nuqs";
import { StickToBottom, useStickToBottomContext } from "use-stick-to-bottom";
import ThreadHistory from "./history";
import { toast } from "sonner";
import { isStaticDemo, showStaticDemoNotice } from "@/lib/static-demo";
import { useFileUpload } from "@/hooks/use-file-upload";
import { ContentBlocksPreview } from "./ContentBlocksPreview";
import {
  useArtifactOpen,
  ArtifactContent,
  ArtifactTitle,
  useArtifactContext,
} from "./artifact";

function StickyToBottomContent(props: {
  content: ReactNode;
  footer?: ReactNode;
  className?: string;
  contentClassName?: string;
}) {
  const context = useStickToBottomContext();
  return (
    <div
      ref={context.scrollRef}
      style={{ width: "100%", height: "100%" }}
      className={props.className}
    >
      <div
        ref={context.contentRef}
        className={props.contentClassName}
      >
        {props.content}
      </div>

      {props.footer}
    </div>
  );
}

function ScrollToBottom(props: { className?: string }) {
  const { isAtBottom, scrollToBottom } = useStickToBottomContext();

  if (isAtBottom) return null;
  return (
    <Button
      variant="outline"
      className={props.className}
      onClick={() => scrollToBottom()}
    >
      <ArrowDown className="h-4 w-4" />
      <span>Scroll to bottom</span>
    </Button>
  );
}

export function Thread() {
  const [artifactContext, setArtifactContext] = useArtifactContext();
  const [artifactOpen, closeArtifact] = useArtifactOpen();

  const [threadId, _setThreadId] = useQueryState("threadId");
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false);
  const [input, setInput] = useState("");
  const inputRef = useRef<HTMLTextAreaElement | null>(null);
  const {
    contentBlocks,
    setContentBlocks,
    dropRef,
    removeBlock,
    resetBlocks: _resetBlocks,
    dragOver,
    handlePaste,
  } = useFileUpload();
  const stream = useStreamContext();
  const { workerPid } = useWorker();
  const messages = stream.messages;
  const visibleMessages = messages.filter(
    (message) => !message.id?.startsWith(DO_NOT_RENDER_ID_PREFIX),
  );
  const isLoading = stream.isLoading;
  const [loadingAnchorIndex, setLoadingAnchorIndex] = useState<number | null>(
    null,
  );

  useEffect(() => {
    const textarea = inputRef.current;
    if (!textarea) return;

    const maxHeight = 192;
    textarea.style.height = "auto";
    textarea.style.height = `${Math.min(textarea.scrollHeight, maxHeight)}px`;
    textarea.style.overflowY = textarea.scrollHeight > maxHeight ? "auto" : "hidden";
  }, [input]);

  useEffect(() => {
    if (!isLoading) setLoadingAnchorIndex(null);
  }, [isLoading]);

  const lastError = useRef<string | undefined>(undefined);

  const setThreadId = (id: string | null) => {
    setLoadingAnchorIndex(null);
    _setThreadId(id);

    // close artifact and reset artifact context
    closeArtifact();
    setArtifactContext({});
  };

  useEffect(() => {
    if (!stream.error) {
      lastError.current = undefined;
      return;
    }
    try {
      const message = (stream.error as any).message;
      if (!message || lastError.current === message) {
        // Message has already been logged. do not modify ref, return early.
        return;
      }

      // Suppress repeat notifications when the component rerenders.
      lastError.current = message;
      const showDetails = process.env.NODE_ENV !== "production";
      const description = showDetails ? (
        <p>
          <strong>Error:</strong> <code>{message}</code>
        </p>
      ) : (
        "Please try again. If the problem continues, contact support."
      );
      toast.error(
        showDetails
          ? "An error occurred. Please try again."
          : "Couldn't complete the request.",
        {
          description,
          richColors: true,
          closeButton: true,
        },
      );
    } catch {
      // no-op
    }
  }, [stream.error]);

  const submitMessage = (messageText: string) => {
    if (
      (messageText.trim().length === 0 && contentBlocks.length === 0) ||
      isLoading
    ) {
      return;
    }

    if (isStaticDemo) {
      showStaticDemoNotice();
      return;
    }

    const newHumanMessage: Message = {
      id: uuidv4(),
      type: "human",
      content: [
        ...(messageText.trim().length > 0
          ? [{ type: "text", text: messageText }]
          : []),
        ...contentBlocks,
      ] as Message["content"],
    };

    const toolMessages = ensureToolCallsHaveResponses(stream.messages);
    setLoadingAnchorIndex(visibleMessages.length);
    const context =
      Object.keys(artifactContext).length > 0 ? artifactContext : undefined;

    stream.submit(
      {
        messages: [...toolMessages, newHumanMessage],
        context,
        worker_pid: workerPid ? Number(workerPid) : undefined,
      },
      {
        streamMode: ["values"],
        streamSubgraphs: true,
        streamResumable: true,
        optimisticValues: (prev) => ({
          ...prev,
          context,
          messages: [
            ...(prev.messages ?? []),
            ...toolMessages,
            newHumanMessage,
          ],
        }),
      },
    );

    setInput("");
    setContentBlocks([]);
  };

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    submitMessage(input);
  };

  const handleRegenerate = (
    parentCheckpoint: Checkpoint | null | undefined,
    sourceMessage: Message,
    sourceWorkerPid: number | undefined,
  ) => {
    if (!parentCheckpoint) return;

    const promptIndex = visibleMessages.findIndex(
      (message) => message.id === sourceMessage.id,
    );
    setLoadingAnchorIndex(promptIndex >= 0 ? promptIndex : null);

    const replayMessage: Message = {
      id: uuidv4(),
      type: "human",
      content: sourceMessage.content,
    };
    stream.submit(
      {
        messages: [replayMessage],
        worker_pid:
          sourceWorkerPid ?? (workerPid ? Number(workerPid) : undefined),
      },
      {
        checkpoint: parentCheckpoint,
        streamMode: ["values"],
        streamSubgraphs: true,
        streamResumable: true,
      },
    );
  };

  const chatStarted = !!threadId || !!messages.length;
  const hasNoAIOrToolMessages = !messages.find(
    (m) => m.type === "ai" || m.type === "tool",
  );
  const startNewThread = () => {
    setThreadId(null);
    setMobileSidebarOpen(false);
  };

  return (
    <div className="flex h-dvh w-full overflow-hidden bg-background text-foreground">
      <ThreadHistory
        mobileOpen={mobileSidebarOpen}
        onMobileOpenChange={setMobileSidebarOpen}
        onNewThread={startNewThread}
      />
      <main className="flex min-h-0 min-w-0 flex-1 flex-col">
        <header className="flex h-14 shrink-0 items-center gap-3 bg-background px-3 sm:px-5">
          <Button
            variant="ghost"
            size="icon"
            className="lg:hidden"
            aria-label="Open navigation"
            title="Open navigation"
            onClick={() => setMobileSidebarOpen(true)}
          >
            <Menu className="size-5" />
          </Button>
          <h1 className="text-lg font-semibold tracking-tight">OriginTracer</h1>
        </header>

      <div
        className={cn(
          "grid min-h-0 w-full flex-1 grid-cols-[1fr_0fr] transition-all duration-300",
          artifactOpen && "grid-cols-[3fr_2fr]",
        )}
      >
        <div
          className={cn(
            "relative flex min-w-0 flex-1 flex-col overflow-hidden",
            !chatStarted && "grid-rows-[1fr]",
          )}
        >
          <StickToBottom className="relative flex-1 overflow-hidden">
            <StickyToBottomContent
              className={cn(
                "absolute inset-0 overflow-y-scroll px-4 [&::-webkit-scrollbar]:w-1.5 [&::-webkit-scrollbar-thumb]:rounded-full [&::-webkit-scrollbar-thumb]:bg-gray-300 [&::-webkit-scrollbar-track]:bg-transparent",
                !chatStarted && "mt-[25vh] flex flex-col items-stretch",
                chatStarted && "grid grid-rows-[1fr_auto]",
              )}
              contentClassName="mx-auto flex w-full min-w-0 max-w-3xl flex-col gap-4 px-4 pt-8 pb-16"
              content={
                <>
                  {visibleMessages.map((message, index) => {
                    if (message.type === "human") {
                      const isLatestHumanMessage = !visibleMessages
                        .slice(index + 1)
                        .some((later) => later.type === "human");

                      return (
                        <Fragment key={message.id || message.type + "-" + index}>
                          <HumanMessage
                            message={message}
                            isLoading={isLoading}
                            onEditSubmitStart={() => {
                              setLoadingAnchorIndex(index);
                            }}
                          />
                          {isLatestHumanMessage && stream.interrupt && (
                            <ActionApprovalInterrupt key="action-approval" />
                          )}
                          {isLoading && loadingAnchorIndex === index && (
                            <AssistantMessageLoading key="anchored-loading" />
                          )}
                        </Fragment>
                      );
                    }

                    return (
                      <Fragment key={message.id || message.type + "-" + index}>
                        <AssistantMessage
                          message={message}
                          isLoading={isLoading}
                          handleRegenerate={handleRegenerate}
                        />
                        {isLoading && loadingAnchorIndex === index && (
                          <AssistantMessageLoading key="anchored-loading" />
                        )}
                      </Fragment>
                    );
                  })}
                  {/* Special rendering case where there are no AI/tool messages, but there is an interrupt.
                    We need to render it outside of the messages list, since there are no messages to render */}
                  {hasNoAIOrToolMessages && !!stream.interrupt && (
                    <AssistantMessage
                      key="interrupt-msg"
                      message={undefined}
                      isLoading={isLoading}
                      handleRegenerate={handleRegenerate}
                    />
                  )}
                  {isLoading && loadingAnchorIndex === null && (
                    <AssistantMessageLoading />
                  )}
                </>
              }
              footer={
                <div className="sticky bottom-0 flex flex-col items-center gap-8 bg-background">
                  {!chatStarted && contentBlocks.length === 0 && (
                    <>
                      <h2 className="text-center text-xl font-medium tracking-tight">
                        What would you like to investigate?
                      </h2>
                      <button
                        type="button"
                        onClick={() =>
                          submitMessage("Give me a live overview of all workers.")
                        }
                        disabled={isLoading}
                        className="border-border bg-background hover:bg-muted z-10 inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-50"
                      >
                        <Users aria-hidden="true" className="size-4" />
                        Overview live workers
                      </button>
                    </>
                  )}

                  <ScrollToBottom className="animate-in fade-in-0 zoom-in-95 absolute bottom-full left-1/2 mb-4 -translate-x-1/2" />

                  <div
                    ref={dropRef}
                    className={cn(
                      "bg-muted relative z-10 mx-auto mb-8 w-full max-w-3xl rounded-xl border shadow-sm transition-colors focus-within:border-ring/50 focus-within:ring-2 focus-within:ring-ring/20",
                      dragOver
                        ? "border-primary border-2 border-dotted"
                        : "border-border border-solid",
                    )}
                  >
                    <form
                      onSubmit={handleSubmit}
                      className="mx-auto w-full min-w-0 max-w-3xl"
                    >
                      <ContentBlocksPreview
                        blocks={contentBlocks}
                        onRemove={removeBlock}
                      />
                      <div className="flex min-w-0 items-end gap-2 p-2">
                        <textarea
                          ref={inputRef}
                          value={input}
                          onChange={(e) => setInput(e.target.value)}
                          onPaste={handlePaste}
                          placeholder="Type your message..."
                          rows={1}
                          className="max-h-48 min-h-10 min-w-0 w-0 max-w-full flex-1 resize-none overflow-y-hidden border-none bg-transparent px-3 py-2 text-base leading-6 shadow-none outline-none placeholder:text-muted-foreground focus-visible:ring-0"
                        />
                        {stream.isLoading ? (
                          <Button
                            key="stop"
                            type="button"
                            onClick={() => stream.stop()}
                            size="icon"
                            variant="outline"
                            className="size-9 shrink-0 rounded-full"
                            aria-label="Cancel response"
                            title="Cancel response"
                          >
                            <Square className="h-3.5 w-3.5 fill-current" />
                          </Button>
                        ) : (
                          <Button
                            type="submit"
                            size="icon"
                            className="size-9 shrink-0 rounded-full shadow-sm transition-transform hover:scale-105 active:scale-95"
                            aria-label="Send message"
                            title="Send message"
                            disabled={
                              isLoading ||
                              (!input.trim() && contentBlocks.length === 0)
                            }
                          >
                            <ArrowUp className="h-4 w-4" />
                          </Button>
                        )}
                      </div>
                    </form>
                  </div>
                  {!chatStarted && (
                    <nav
                      aria-label="OriginTracer resources"
                      className="z-10 mx-auto -mt-4 mb-8 flex w-full max-w-3xl flex-wrap justify-center gap-2 px-4"
                    >
                      <a
                        href="https://github.com/Humbulani1234/origintracer"
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-2 rounded-full border border-border bg-background px-4 py-2 text-sm text-foreground transition-colors hover:bg-muted"
                      >
                        <GitHubSVG width="16" height="16" />
                        GitHub
                      </a>
                      <a
                        href="https://origintracer.app/docs/"
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-2 rounded-full border border-border bg-background px-4 py-2 text-sm text-foreground transition-colors hover:bg-muted"
                      >
                        <BookOpen aria-hidden="true" className="size-4" />
                        Docs
                      </a>
                    </nav>
                  )}
                </div>
              }
            />
          </StickToBottom>
        </div>
        <div className="relative flex flex-col">
          <div className="absolute inset-0 flex min-w-[30vw] flex-col">
            <div className="grid grid-cols-[1fr_auto] border-b p-4">
              <ArtifactTitle className="truncate overflow-hidden" />
              <button
                onClick={closeArtifact}
                className="cursor-pointer"
              >
                <XIcon className="size-5" />
              </button>
            </div>
            <ArtifactContent className="relative flex-grow" />
          </div>
        </div>
      </div>
      </main>
    </div>
  );
}
