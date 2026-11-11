import { parsePartialJson } from "@langchain/core/output_parsers";
import { useStreamContext } from "@/providers/Stream";
import { AIMessage, Checkpoint, Message } from "@langchain/langgraph-sdk";
import { useStream } from "@langchain/langgraph-sdk/react";
import { getContentString } from "../utils";
import { BranchSwitcher, CommandBar } from "./shared";
import { MarkdownText } from "../markdown-text";
import { LoadExternalComponent } from "@langchain/langgraph-sdk/react-ui";
import { cn } from "@/lib/utils";
import { ToolCalls, ToolResult } from "./tool-calls";
import { MessageContentComplex } from "@langchain/core/messages";
import { Fragment } from "react/jsx-runtime";
import { isAgentInboxInterruptSchema } from "@/lib/agent-inbox-interrupt";
import { ThreadView } from "../agent-inbox";
import { useQueryState, parseAsBoolean } from "nuqs";
import { GenericInterruptView } from "./generic-interrupt";
import { useArtifact } from "../artifact";
import { LoaderCircle } from "lucide-react";
import {
  hasOriginTracerEvidenceView,
  OriginTracerEvidence,
} from "./origintracer-evidence";

function CustomComponent({
  message,
  thread,
}: {
  message: Message;
  thread: ReturnType<typeof useStreamContext>;
}) {
  const artifact = useArtifact();
  const { values } = useStreamContext();
  const customComponents = values.ui?.filter(
    (ui) => ui.metadata?.message_id === message.id,
  );

  if (!customComponents?.length) return null;
  return (
    <Fragment key={message.id}>
      {customComponents.map((customComponent) => (
        <LoadExternalComponent
          key={customComponent.id}
          stream={thread as unknown as ReturnType<typeof useStream>}
          message={customComponent}
          meta={{ ui: customComponent, artifact }}
        />
      ))}
    </Fragment>
  );
}

function parseAnthropicStreamedToolCalls(
  content: MessageContentComplex[],
): AIMessage["tool_calls"] {
  const toolCallContents = content.filter((c) => c.type === "tool_use" && c.id);

  return toolCallContents.map((tc) => {
    const toolCall = tc as Record<string, any>;
    let json: Record<string, any> = {};
    if (toolCall?.input) {
      try {
        json = parsePartialJson(toolCall.input) ?? {};
      } catch {
        // Pass
      }
    }
    return {
      name: toolCall.name ?? "",
      id: toolCall.id ?? "",
      args: json,
      type: "tool_call",
    };
  });
}

type ReportRecord = Record<string, unknown>;

function asRecords(value: unknown): ReportRecord[] {
  return Array.isArray(value)
    ? value.filter(
        (item): item is ReportRecord =>
          typeof item === "object" && item !== null && !Array.isArray(item),
      )
    : [];
}

function asStrings(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string")
    : [];
}

function readableEvidenceLabel(value: unknown): string {
  return typeof value === "string"
    ? value
        .replace(/^get_worker_/, "")
        .replaceAll("_", " ")
        .replace(/\b\w/g, (letter) => letter.toUpperCase())
    : "Observation";
}

function humanizeFieldName(value: string): string {
  return value
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function isRecord(value: unknown): value is ReportRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function scalarText(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return "";
}

function StructuredValue({
  value,
  depth = 0,
}: {
  value: unknown;
  depth?: number;
}): React.ReactNode {
  if (value === null || value === undefined || typeof value !== "object") {
    return <span className="break-words">{scalarText(value)}</span>;
  }

  if (depth >= 4) {
    return (
      <pre className="max-h-48 overflow-auto text-xs break-words whitespace-pre-wrap">
        {JSON.stringify(value, null, 2)}
      </pre>
    );
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return <span className="text-muted-foreground">No rows</span>;
    }

    if (value.every(isRecord)) {
      const rows = value as ReportRecord[];
      const columns = Array.from(
        new Set(rows.flatMap((row) => Object.keys(row))),
      ).slice(0, 7);
      const table = (shownRows: ReportRecord[]) => (
        <div className="w-full max-w-full min-w-0 overflow-x-auto rounded-md border">
          <table className="min-w-full border-collapse text-left text-xs">
            <thead className="bg-muted/70 text-muted-foreground">
              <tr>
                {columns.map((column) => (
                  <th
                    key={column}
                    className="px-2.5 py-2 font-medium"
                  >
                    {humanizeFieldName(column)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {shownRows.map((row, rowIndex) => (
                <tr
                  key={rowIndex}
                  className="border-t align-top"
                >
                  {columns.map((column) => (
                    <td
                      key={column}
                      className="max-w-64 px-2.5 py-2 break-words"
                    >
                      {isRecord(row[column]) || Array.isArray(row[column]) ? (
                        <details>
                          <summary className="text-muted-foreground cursor-pointer">
                            {Array.isArray(row[column])
                              ? `${(row[column] as unknown[]).length} items`
                              : `${Object.keys(row[column] as ReportRecord).length} fields`}
                          </summary>
                          <div className="mt-2 min-w-48">
                            <StructuredValue
                              value={row[column]}
                              depth={depth + 1}
                            />
                          </div>
                        </details>
                      ) : (
                        scalarText(row[column])
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );

      return (
        <div className="space-y-2">
          {table(rows.slice(0, 8))}
          {rows.length > 8 && (
            <details>
              <summary className="text-muted-foreground cursor-pointer text-xs">
                Show all {rows.length} rows
              </summary>
              <div className="mt-2">{table(rows)}</div>
            </details>
          )}
          {columns.length < Object.keys(rows[0]).length && (
            <p className="text-muted-foreground text-xs">
              Showing the first {columns.length} fields in the table.
            </p>
          )}
        </div>
      );
    }

    return (
      <ul className="space-y-1 text-sm">
        {value.map((item, index) => (
          <li
            key={index}
            className="break-words"
          >
            {isRecord(item) || Array.isArray(item) ? (
              <StructuredValue
                value={item}
                depth={depth + 1}
              />
            ) : (
              scalarText(item)
            )}
          </li>
        ))}
      </ul>
    );
  }

  const entries = Object.entries(value);
  if (entries.length === 0) {
    return <span className="text-muted-foreground">No fields returned</span>;
  }

  return (
    <dl className="divide-y rounded-md border px-3">
      {entries.map(([key, child]) => (
        <div
          key={key}
          className="grid grid-cols-[minmax(7rem,0.35fr)_minmax(0,1fr)] gap-3 py-2.5 text-sm"
        >
          <dt className="text-muted-foreground break-words">
            {humanizeFieldName(key)}
          </dt>
          <dd className="min-w-0 break-words">
            {isRecord(child) || Array.isArray(child) ? (
              <StructuredValue
                value={child}
                depth={depth + 1}
              />
            ) : (
              scalarText(child)
            )}
          </dd>
        </div>
      ))}
    </dl>
  );
}

function EvidenceReferences({
  confidence,
  evidenceIds,
}: {
  confidence?: unknown;
  evidenceIds: string[];
}) {
  if (typeof confidence !== "string" && evidenceIds.length === 0) {
    return null;
  }

  return (
    <div className="text-muted-foreground mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
      {typeof confidence === "string" && (
        <span className="bg-muted rounded-full px-2 py-0.5">
          {confidence} confidence
        </span>
      )}
      {evidenceIds.length > 0 && (
        <span>Evidence: {evidenceIds.join(", ")}</span>
      )}
    </div>
  );
}

function ReportEvidence({ report }: { report: ReportRecord }) {
  const observations = asRecords(report.observations);
  const limitations = asStrings(report.limitations);
  const unresolved = asStrings(report.unresolved_questions);
  const nextSteps = asStrings(report.next_steps);
  const hypotheses = asRecords(report.hypotheses);
  const findings = asRecords(report.findings);
  const hasExplicitAnswer = typeof report.answer === "string";
  const reportAnswer = hasExplicitAnswer ? report.answer : report.summary;
  const summaryText =
    typeof reportAnswer === "string" &&
    reportAnswer !== "The available tool observations are summarized below."
      ? reportAnswer
      : "No concise assessment summary was returned for this investigation.";
  const summaryFinding = findings.find(
    (finding) => finding.statement === summaryText,
  );
  const summaryEvidenceIds = asStrings(report.summary_evidence_ids);
  const summaryRepeatsObservation = observations.some(
    (observation) =>
      typeof observation.summary === "string" &&
      observation.summary.trim() === summaryText.trim(),
  );
  const hasComponentDiagnostics = observations.some(
    (item) => item.tool === "get_worker_component_diagnostics",
  );
  const otherFindings = findings.filter((finding) => {
    if (finding === summaryFinding) return false;
    if (hasComponentDiagnostics) {
      const statement = String(finding.statement ?? "");
      const generatedKind =
        finding.kind === "observed_measurements" ||
        finding.kind === "detected_rule_patterns";
      const legacyGeneratedSummary =
        statement.startsWith("Observed measurements:") ||
        statement.startsWith(
          "Detected rule patterns (patterns do not establish root cause):",
        );
      if (generatedKind || legacyGeneratedSummary) return false;
    }
    return true;
  });
  const evidenceDetailsCount =
    observations.length +
    limitations.length +
    unresolved.length +
    nextSteps.length;

  return (
    <div className="w-full max-w-3xl min-w-0 space-y-5 py-1">
      {(!summaryRepeatsObservation || hasExplicitAnswer) && (
        <section>
          <MarkdownText compact>{summaryText}</MarkdownText>
          {(summaryEvidenceIds.length > 0 || summaryFinding) && (
            <EvidenceReferences
              confidence={summaryFinding?.confidence}
              evidenceIds={
                summaryEvidenceIds.length > 0
                  ? summaryEvidenceIds
                  : asStrings(summaryFinding?.evidence_ids)
              }
            />
          )}
        </section>
      )}

      {observations.some((item) => hasOriginTracerEvidenceView(item.tool)) && (
        <section className="space-y-3">
          {observations
            .filter((item) => hasOriginTracerEvidenceView(item.tool))
            .map((item, index) => (
              <OriginTracerEvidence
                key={`${String(item.evidence_id ?? item.tool ?? "evidence")}-${index}`}
                observation={item}
              />
            ))}
        </section>
      )}

      {otherFindings.length > 0 && (
        <section className="border-border space-y-3 border-l-2 pl-4">
          <p className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
            Findings
          </p>
          {otherFindings.map((finding, index) => (
            <article
              key={`${String(finding.statement ?? "finding")}-${index}`}
              className="min-w-0 rounded-lg border px-3 py-2.5"
            >
              <div className="text-sm leading-6 break-words">
                <MarkdownText compact>
                  {String(finding.statement ?? "")}
                </MarkdownText>
              </div>
              <EvidenceReferences
                confidence={finding.confidence}
                evidenceIds={asStrings(finding.evidence_ids)}
              />
            </article>
          ))}
        </section>
      )}

      {hypotheses.length > 0 && (
        <section className="space-y-3">
          <p className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
            Explanation checks
          </p>
          {hypotheses.map((item, index) => (
            <div
              key={`${String(item.claim ?? "hypothesis")}-${index}`}
              className="rounded-lg border px-3 py-2.5"
            >
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="text-sm font-medium">
                  <MarkdownText compact>
                    {String(item.claim ?? "")}
                  </MarkdownText>
                </div>
                <span className="bg-muted text-muted-foreground rounded-full px-2 py-0.5 text-xs capitalize">
                  {String(item.status ?? "inconclusive")}
                </span>
              </div>
              {typeof item.explanation === "string" && item.explanation && (
                <div className="text-muted-foreground mt-1 text-sm">
                  <MarkdownText compact>{item.explanation}</MarkdownText>
                </div>
              )}
              <EvidenceReferences
                evidenceIds={[
                  ...asStrings(item.evidence_for),
                  ...asStrings(item.evidence_against),
                ].filter((id, index, ids) => ids.indexOf(id) === index)}
              />
            </div>
          ))}
        </section>
      )}

      {evidenceDetailsCount > 0 && (
        <details className="rounded-xl border">
          <summary className="cursor-pointer list-none px-3 py-2.5 text-sm font-medium marker:hidden">
            Evidence and follow-up details
            <span className="text-muted-foreground ml-2 text-xs font-normal">
              {evidenceDetailsCount} items
            </span>
          </summary>
          <div className="space-y-4 border-t px-3 py-3">
            {observations.length > 0 && (
              <section className="space-y-2">
                <p className="text-muted-foreground text-xs font-medium tracking-wide uppercase">
                  Evidence sources
                </p>
                {observations.map((item, index) => {
                  const evidenceId =
                    typeof item.evidence_id === "string"
                      ? item.evidence_id
                      : `observation-${index + 1}`;
                  return (
                    <details
                      id={`evidence-${evidenceId}`}
                      key={`${evidenceId}-${index}`}
                      className="bg-muted/40 rounded-lg px-3 py-2"
                    >
                      <summary className="cursor-pointer list-none text-sm marker:hidden">
                        <span className="font-medium">
                          {readableEvidenceLabel(item.tool)}
                        </span>
                        {typeof item.evidence_id === "string" && (
                          <span className="text-muted-foreground ml-2 text-xs">
                            {item.evidence_id}
                          </span>
                        )}
                      </summary>
                      <div className="mt-3 space-y-3 border-t pt-2">
                        {item.structured_data !== undefined ? (
                          <details className="text-muted-foreground text-xs">
                            <summary className="cursor-pointer">
                              Show raw structured response
                            </summary>
                            <div className="text-foreground mt-2">
                              <StructuredValue value={item.structured_data} />
                            </div>
                          </details>
                        ) : typeof item.summary === "string" ? (
                          <details className="rounded-md border px-2.5 py-2">
                            <summary className="cursor-pointer text-xs font-medium">
                              Show original text summary
                            </summary>
                            <div className="mt-2 text-sm">
                              <MarkdownText compact>
                                {item.summary}
                              </MarkdownText>
                            </div>
                          </details>
                        ) : null}
                        {(typeof item.source === "string" ||
                          typeof item.query === "string") && (
                          <details className="text-muted-foreground text-xs">
                            <summary className="cursor-pointer">
                              Source details
                            </summary>
                            <p className="mt-1 break-words">
                              {[item.source, item.query]
                                .filter(
                                  (value): value is string =>
                                    typeof value === "string",
                                )
                                .join(" · ")}
                            </p>
                          </details>
                        )}
                      </div>
                    </details>
                  );
                })}
              </section>
            )}
            {limitations.length > 0 && (
              <section>
                <p className="text-muted-foreground mb-1 text-xs font-medium tracking-wide uppercase">
                  Evidence limits
                </p>
                <ul className="text-muted-foreground list-disc space-y-1 pl-5 text-sm leading-6">
                  {limitations.map((item, index) => (
                    <li key={`${item}-${index}`}>
                      <MarkdownText compact>{item}</MarkdownText>
                    </li>
                  ))}
                </ul>
              </section>
            )}
            {unresolved.length > 0 && (
              <section>
                <p className="text-muted-foreground mb-1 text-xs font-medium tracking-wide uppercase">
                  Still unclear
                </p>
                <ul className="text-muted-foreground list-disc space-y-1 pl-5 text-sm leading-6">
                  {unresolved.map((item, index) => (
                    <li key={`${item}-${index}`}>
                      <MarkdownText compact>{item}</MarkdownText>
                    </li>
                  ))}
                </ul>
              </section>
            )}
            {nextSteps.length > 0 && (
              <section>
                <p className="text-muted-foreground mb-1 text-xs font-medium tracking-wide uppercase">
                  Next checks
                </p>
                <ul className="text-muted-foreground list-disc space-y-1 pl-5 text-sm leading-6">
                  {nextSteps.map((item, index) => (
                    <li key={`${item}-${index}`}>
                      <MarkdownText compact>{item}</MarkdownText>
                    </li>
                  ))}
                </ul>
              </section>
            )}
          </div>
        </details>
      )}
    </div>
  );
}

interface InterruptProps {
  interrupt?: unknown;
  isLastMessage: boolean;
  hasNoAIOrToolMessages: boolean;
}

function Interrupt({
  interrupt,
  isLastMessage,
  hasNoAIOrToolMessages,
}: InterruptProps) {
  const firstInterrupt = Array.isArray(interrupt) ? interrupt[0] : interrupt;
  const fallbackValue: Record<string, any> =
    firstInterrupt && typeof firstInterrupt === "object"
      ? (("value" in firstInterrupt
          ? firstInterrupt.value
          : firstInterrupt) as Record<string, any>)
      : {};
  const thread = useStreamContext();
  const resume = (value: unknown) =>
    thread.submit({}, { command: { resume: value } });
  const interruptType = fallbackValue?.type;

  if (interruptType === "origintracer_worker_overview_scope") {
    return (
      <div className="bg-card text-card-foreground my-2 max-w-xl rounded-xl border p-4 shadow-sm">
        <p className="text-muted-foreground text-sm">
          {fallbackValue.message ?? "Which workers should I include?"}
        </p>
        <div className="mt-3 flex gap-2">
          <button
            className="rounded-lg bg-gray-900 px-3 py-2 text-sm text-white hover:bg-gray-700"
            onClick={() => resume({ scope: "all" })}
          >
            All live workers
          </button>
          {fallbackValue.selected_worker && (
            <button
              className="rounded-lg border px-3 py-2 text-sm hover:bg-gray-50"
              onClick={() => resume({ scope: "selected" })}
            >
              Selected worker
            </button>
          )}
        </div>
      </div>
    );
  }

  if (interruptType === "origintracer_action_approval") {
    return null;
  }

  return (
    <>
      {isAgentInboxInterruptSchema(interrupt) &&
        (isLastMessage || hasNoAIOrToolMessages) && (
          <ThreadView interrupt={interrupt} />
        )}
      {interrupt &&
      !isAgentInboxInterruptSchema(interrupt) &&
      (isLastMessage || hasNoAIOrToolMessages) ? (
        <GenericInterruptView interrupt={fallbackValue} />
      ) : null}
    </>
  );
}

export function ActionApprovalInterrupt() {
  const thread = useStreamContext();
  const firstInterrupt = Array.isArray(thread.interrupt)
    ? thread.interrupt[0]
    : thread.interrupt;
  const value: Record<string, any> =
    firstInterrupt && typeof firstInterrupt === "object"
      ? (("value" in firstInterrupt
          ? firstInterrupt.value
          : firstInterrupt) as Record<string, any>)
      : {};

  if (value.type !== "origintracer_action_approval") return null;

  const resume = (approved: boolean) =>
    thread.submit({}, { command: { resume: { approved } } });

  return (
    <div className="bg-card text-card-foreground my-2 max-w-xl rounded-xl border p-4 shadow-sm">
      <p className="text-muted-foreground text-sm">
        {value.message ?? "Approve this action before it runs."}
      </p>
      <p className="mt-2 text-sm font-medium">
        {String(value.action ?? "OriginTracer action").replaceAll("_", " ")}
      </p>
      <div className="mt-3 flex gap-2">
        <button
          className="rounded-lg bg-gray-900 px-3 py-2 text-sm text-white hover:bg-gray-700"
          onClick={() => resume(true)}
        >
          Approve
        </button>
        <button
          className="rounded-lg border px-3 py-2 text-sm hover:bg-gray-50"
          onClick={() => resume(false)}
        >
          Cancel
        </button>
      </div>
    </div>
  );
}

function ActionResultCard({
  actionName,
  result,
}: {
  actionName: string;
  result: unknown;
}) {
  return (
    <section className="w-full min-w-0 space-y-3 rounded-xl border p-3 sm:p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-medium">Action result</h3>
        <span className="bg-muted rounded-full px-2.5 py-1 text-xs">
          {readableEvidenceLabel(actionName)}
        </span>
      </div>
      <StructuredValue value={result} />
    </section>
  );
}

export function AssistantMessage({
  message,
  isLoading,
  handleRegenerate,
}: {
  message: Message | undefined;
  isLoading: boolean;
  handleRegenerate?: (
    parentCheckpoint: Checkpoint | null | undefined,
    sourceMessage: Message,
    sourceWorkerPid: number | undefined,
  ) => void;
}) {
  const content = message?.content ?? [];
  const contentString = getContentString(content);
  const report = (message as any)?.additional_kwargs?.origintracer_report as
    ReportRecord | undefined;
  const [hideToolCalls] = useQueryState(
    "hideToolCalls",
    parseAsBoolean.withDefault(true),
  );

  const thread = useStreamContext();
  const streamValues = thread.values as Record<string, unknown> | null;
  const isLastMessage =
    thread.messages[thread.messages.length - 1].id === message?.id;
  const hasNoAIOrToolMessages = !thread.messages.find(
    (m) => m.type === "ai" || m.type === "tool",
  );
  const latestHumanIndex = thread.messages.findLastIndex(
    (item) => item.type === "human",
  );
  const currentMessageIndex = message
    ? thread.messages.findIndex((item) => item.id === message.id)
    : -1;
  const isCurrentTurnMessage =
    latestHumanIndex >= 0 && currentMessageIndex > latestHumanIndex;
  const isTransientInvestigationMessage =
    thread.isLoading &&
    streamValues?.response_mode === "investigation" &&
    isCurrentTurnMessage &&
    !report;
  const meta = message ? thread.getMessagesMetadata(message) : undefined;
  const threadInterrupt = thread.interrupt;

  const precedingHumanMessage =
    currentMessageIndex >= 0
      ? thread.messages
          .slice(0, currentMessageIndex)
          .findLast((item) => item.type === "human")
      : undefined;
  const precedingHumanMetadata = precedingHumanMessage
    ? thread.getMessagesMetadata(precedingHumanMessage)
    : undefined;
  const precedingHumanCheckpoint =
    precedingHumanMetadata?.firstSeenState?.parent_checkpoint;
  const storedWorkerPid =
    precedingHumanMetadata?.firstSeenState?.values.worker_pid;
  const parsedWorkerPid = Number(storedWorkerPid);
  const precedingWorkerPid =
    Number.isSafeInteger(parsedWorkerPid) && parsedWorkerPid > 0
      ? parsedWorkerPid
      : undefined;
  const anthropicStreamedToolCalls = Array.isArray(content)
    ? parseAnthropicStreamedToolCalls(content)
    : undefined;

  const hasToolCalls =
    message &&
    "tool_calls" in message &&
    message.tool_calls &&
    message.tool_calls.length > 0;
  const toolCallsHaveContents =
    hasToolCalls &&
    message.tool_calls?.some(
      (tc) => tc.args && Object.keys(tc.args).length > 0,
    );
  const hasAnthropicToolCalls = !!anthropicStreamedToolCalls?.length;
  const isInternalToolCallMessage = Boolean(
    hasToolCalls || hasAnthropicToolCalls,
  );
  const isToolResult = message?.type === "tool";
  const proposedAction = isRecord(streamValues?.proposed_action)
    ? streamValues.proposed_action
    : {};
  const actionResult = streamValues?.action_result;
  const hasStructuredActionResult =
    message?.type === "ai" &&
    isCurrentTurnMessage &&
    contentString.startsWith("Action " + String.fromCharCode(96)) &&
    contentString.includes(" completed. Result:") &&
    (isRecord(actionResult) || Array.isArray(actionResult));

  if (
    (isToolResult && hideToolCalls) ||
    isTransientInvestigationMessage ||
    (isInternalToolCallMessage && hideToolCalls)
  ) {
    return null;
  }

  return (
    <div className="group mr-auto flex w-full min-w-0 items-start gap-2">
      <div className="flex w-full min-w-0 flex-col gap-2">
        {isToolResult ? (
          <>
            <ToolResult message={message} />
            <Interrupt
              interrupt={threadInterrupt}
              isLastMessage={isLastMessage}
              hasNoAIOrToolMessages={hasNoAIOrToolMessages}
            />
          </>
        ) : (
          <>
            {report ? (
              <ReportEvidence report={report} />
            ) : hasStructuredActionResult ? (
              <ActionResultCard
                actionName={String(
                  proposedAction.name ?? "OriginTracer action",
                )}
                result={actionResult}
              />
            ) : (
              !isInternalToolCallMessage &&
              contentString.length > 0 && (
                <div className="py-1">
                  <MarkdownText>{contentString}</MarkdownText>
                </div>
              )
            )}

            {!hideToolCalls && (
              <>
                {(hasToolCalls && toolCallsHaveContents && (
                  <ToolCalls toolCalls={message.tool_calls} />
                )) ||
                  (hasAnthropicToolCalls && (
                    <ToolCalls toolCalls={anthropicStreamedToolCalls} />
                  )) ||
                  (hasToolCalls && (
                    <ToolCalls toolCalls={message.tool_calls} />
                  ))}
              </>
            )}

            {message && (
              <CustomComponent
                message={message}
                thread={thread}
              />
            )}
            <Interrupt
              interrupt={threadInterrupt}
              isLastMessage={isLastMessage}
              hasNoAIOrToolMessages={hasNoAIOrToolMessages}
            />
            <div
              className={cn(
                "mr-auto flex items-center gap-2 transition-opacity",
                "opacity-0 group-focus-within:opacity-100 group-hover:opacity-100",
              )}
            >
              <BranchSwitcher
                branch={meta?.branch}
                branchOptions={meta?.branchOptions}
                onSelect={(branch) => thread.setBranch(branch)}
                isLoading={isLoading}
              />
              <CommandBar
                content={contentString}
                isLoading={isLoading}
                isAiMessage={true}
                handleRegenerate={
                  handleRegenerate &&
                  precedingHumanMessage &&
                  precedingHumanCheckpoint
                    ? () =>
                        handleRegenerate(
                          precedingHumanCheckpoint,
                          precedingHumanMessage,
                          precedingWorkerPid,
                        )
                    : undefined
                }
              />
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export function AssistantMessageLoading() {
  return (
    <div className="mr-auto flex items-start gap-2" role="status" aria-live="polite">
      <div className="bg-muted text-muted-foreground flex min-h-9 items-center gap-2 rounded-2xl px-3 py-2 text-sm">
        <LoaderCircle
          aria-hidden="true"
          className="text-primary size-4 motion-safe:animate-spin"
        />
        <span>Working…</span>
      </div>
    </div>
  );
}
