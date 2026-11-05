const THREAD_IDS_KEY_PREFIX = "origintracer.chat-ui.threads.";

function storageKey(assistantId: string) {
  return `${THREAD_IDS_KEY_PREFIX}${assistantId}`;
}

export function rememberChatUiThread(threadId: string, assistantId: string) {
  if (typeof window === "undefined" || !threadId || !assistantId) return;

  try {
    const key = storageKey(assistantId);
    const stored = JSON.parse(window.localStorage.getItem(key) ?? "[]");
    const threadIds = Array.isArray(stored) ? stored : [];
    if (!threadIds.includes(threadId)) {
      window.localStorage.setItem(key, JSON.stringify([...threadIds, threadId]));
    }
  } catch {
    // History filtering is best-effort when browser storage is unavailable.
  }
}

export function getChatUiThreadIds(assistantId: string): string[] {
  if (typeof window === "undefined" || !assistantId) return [];

  try {
    const stored = JSON.parse(
      window.localStorage.getItem(storageKey(assistantId)) ?? "[]",
    );
    return Array.isArray(stored)
      ? stored.filter((threadId): threadId is string => typeof threadId === "string")
      : [];
  } catch {
    return [];
  }
}

export function forgetChatUiThread(threadId: string, assistantId: string) {
  if (typeof window === "undefined" || !threadId || !assistantId) return;

  try {
    const threadIds = getChatUiThreadIds(assistantId).filter(
      (storedId) => storedId !== threadId,
    );
    window.localStorage.setItem(storageKey(assistantId), JSON.stringify(threadIds));
  } catch {
    // The server-side deletion remains complete if browser storage is unavailable.
  }
}
