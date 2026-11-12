import { useStreamContext } from "@/providers/Stream";
import { Message } from "@langchain/langgraph-sdk";
import { useState } from "react";
import { getContentString } from "../utils";
import { cn } from "@/lib/utils";
import { Textarea } from "@/components/ui/textarea";
import { BranchSwitcher, CommandBar } from "./shared";
import { MultimodalPreview } from "@/components/thread/MultimodalPreview";
import { isBase64ContentBlock } from "@/lib/multimodal-utils";
import { ensureToolCallsHaveResponses } from "@/lib/ensure-tool-responses";
import { v4 as uuidv4 } from "uuid";
import { toast } from "sonner";

function EditableContent({
  value,
  setValue,
  onSubmit,
}: {
  value: string;
  setValue: React.Dispatch<React.SetStateAction<string>>;
  onSubmit: () => void;
}) {
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      onSubmit();
    }
  };

  return (
    <Textarea
      value={value}
      onChange={(e) => setValue(e.target.value)}
      onKeyDown={handleKeyDown}
      className="focus-visible:ring-0"
    />
  );
}

export function HumanMessage({
  message,
  isLoading,
  onEditSubmitStart,
}: {
  message: Message;
  isLoading: boolean;
  onEditSubmitStart?: () => void;
}) {
  const thread = useStreamContext();
  const meta = thread.getMessagesMetadata(message);
  const parentCheckpoint = meta?.firstSeenState?.parent_checkpoint;

  const [isEditing, setIsEditing] = useState(false);
  const [value, setValue] = useState("");
  const contentString = getContentString(message.content);

  const handleSubmitEdit = async () => {
    const parentCheckpointId = parentCheckpoint?.checkpoint_id;
    let parentState = parentCheckpointId
      ? (Array.isArray(thread.history) ? thread.history : []).find(
          (state) => state.checkpoint?.checkpoint_id === parentCheckpointId,
        )
      : undefined;

    // History can be temporarily unavailable while the stream provider is
    // restoring a conversation. Fetch the exact parent checkpoint as a
    // fallback so older messages remain editable without relying on the
    // in-memory history list.
    if (!parentState && parentCheckpoint) {
      try {
        parentState = await thread.client.threads.getState(
          parentCheckpoint.thread_id,
          parentCheckpoint,
        );
      } catch (error) {
        console.error("Failed to load the edit parent checkpoint", error);
      }
    }

    if (!parentCheckpoint || !parentState) {
      toast.error("Could not edit this message", {
        description:
          "Its parent checkpoint could not be loaded. Refresh the conversation and try again.",
      });
      return;
    }

    const parentValues = parentState.values;
    const parentMessages = parentValues.messages ?? [];
    const toolMessages = ensureToolCallsHaveResponses(parentMessages);
    const editedMessageId = uuidv4();
    const newMessage: Message = {
      id: editedMessageId,
      type: "human",
      content: value,
    };
    const inputMessages = [...toolMessages, newMessage];
    const originalWorkerPid = Number(meta?.firstSeenState?.values.worker_pid);
    const workerPid =
      Number.isSafeInteger(originalWorkerPid) && originalWorkerPid > 0
        ? originalWorkerPid
        : undefined;

    setIsEditing(false);
    onEditSubmitStart?.();
    thread.submit(
      {
        messages: inputMessages,
        ...(workerPid === undefined ? {} : { worker_pid: workerPid }),
      },
      {
        checkpoint: parentCheckpoint,
        streamMode: ["values"],
        streamSubgraphs: true,
        streamResumable: true,
        optimisticValues: () => {
          return {
            ...parentValues,
            ...(workerPid === undefined ? {} : { worker_pid: workerPid }),
            messages: [...parentMessages, ...inputMessages],
          };
        },
      },
    );
  };

  return (
    <div
      className={cn(
        "group ml-auto flex w-full min-w-0 items-center gap-2",
        isEditing && "w-full max-w-xl",
      )}
    >
      <div
        className={cn(
          "flex w-full min-w-0 flex-col gap-2",
          isEditing && "w-full",
        )}
      >
        {isEditing ? (
          <EditableContent
            value={value}
            setValue={setValue}
            onSubmit={handleSubmitEdit}
          />
        ) : (
          <div className="flex flex-col gap-2">
            {/* Render images and files if no text */}
            {Array.isArray(message.content) && message.content.length > 0 && (
              <div className="flex flex-wrap items-end justify-end gap-2">
                {message.content.reduce<React.ReactNode[]>(
                  (acc, block, idx) => {
                    if (isBase64ContentBlock(block)) {
                      acc.push(
                        <MultimodalPreview
                          key={idx}
                          block={block}
                          size="md"
                        />,
                      );
                    }
                    return acc;
                  },
                  [],
                )}
              </div>
            )}
            {/* Render text if present, otherwise fallback to file/image name */}
            {contentString ? (
              <p className="bg-muted ml-auto w-fit max-w-[min(95%,48rem)] rounded-3xl px-4 py-2 text-left break-words whitespace-pre-wrap sm:max-w-[min(85%,48rem)]">
                {contentString}
              </p>
            ) : null}
          </div>
        )}

        <div
          className={cn(
            "ml-auto flex items-center gap-2 transition-opacity",
            "opacity-0 group-focus-within:opacity-100 group-hover:opacity-100",
            isEditing && "opacity-100",
          )}
        >
          <BranchSwitcher
            branch={meta?.branch}
            branchOptions={meta?.branchOptions}
            onSelect={(branch) => thread.setBranch(branch)}
            isLoading={isLoading}
          />
          <CommandBar
            isLoading={isLoading}
            content={contentString}
            isEditing={isEditing}
            setIsEditing={(c) => {
              if (c) {
                setValue(contentString);
              }
              setIsEditing(c);
            }}
            handleSubmitEdit={handleSubmitEdit}
            isHumanMessage={true}
          />
        </div>
      </div>
    </div>
  );
}
