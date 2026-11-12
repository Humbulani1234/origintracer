"use client";

import { useCallback, useEffect, useState, type FormEvent } from "react";

type ApiKey = {
  key_id: string;
  name: string;
  key_prefix: string;
  created_at: string;
  revoked_at: string | null;
};

export function ApiKeyManager({ enabled }: { enabled: boolean }) {
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [name, setName] = useState("");
  const [newKey, setNewKey] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const basePath = (process.env.NEXT_PUBLIC_BASE_PATH ?? "").replace(/\/+$/, "");

  const loadKeys = useCallback(async () => {
    const response = await fetch(`${basePath}/api/api-keys`, { cache: "no-store" });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body.detail || "Could not load API keys.");
    setKeys(body.data ?? []);
  }, [basePath]);

  useEffect(() => {
    if (enabled) void loadKeys().catch((err) => setError(err.message));
  }, [enabled, loadKeys]);

  async function createKey(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError("");
    setNewKey("");
    try {
      const response = await fetch(`${basePath}/api/api-keys`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(body.detail || "Could not create API key.");
      setNewKey(body.api_key);
      setName("");
      await loadKeys();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not create API key.");
    } finally {
      setBusy(false);
    }
  }

  async function revoke(keyId: string) {
    setBusy(true);
    setError("");
    try {
      const response = await fetch(
        `${basePath}/api/api-keys/${encodeURIComponent(keyId)}`,
        { method: "DELETE" },
      );
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || "Could not revoke API key.");
      }
      await loadKeys();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not revoke API key.");
    } finally {
      setBusy(false);
    }
  }

  if (!enabled) {
    return (
      <div className="rounded-xl border p-5 text-sm">
        OIDC is not configured here. Local development can continue using the configured development key.
      </div>
    );
  }

  return (
    <section className="space-y-5">
      <form onSubmit={createKey} className="flex flex-col gap-3 rounded-xl border p-5 sm:flex-row">
        <input
          aria-label="Key name"
          className="h-10 min-w-0 flex-1 rounded-lg border bg-background px-3 text-sm"
          value={name}
          onChange={(event) => setName(event.target.value)}
          placeholder="e.g. books-api production"
          maxLength={80}
          required
        />
        <button
          disabled={busy}
          className="h-10 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground disabled:opacity-50"
        >
          Create worker key
        </button>
      </form>

      {newKey && (
        <div className="rounded-xl border border-amber-500/50 bg-amber-500/5 p-5">
          <p className="font-medium">Copy this key now</p>
          <p className="text-muted-foreground mt-1 text-sm">
            It will not be shown again. Store it as ORIGINTRACER_API_KEY in the instrumented process environment.
          </p>
          <code className="mt-3 block overflow-x-auto rounded-md bg-muted p-3 text-xs">{newKey}</code>
        </div>
      )}

      {error && (
        <p role="alert" className="rounded-lg bg-destructive/10 p-3 text-sm text-destructive">
          {error}
        </p>
      )}

      <div className="overflow-x-auto rounded-xl border">
        <table className="w-full min-w-[34rem] text-left text-sm">
          <thead className="bg-muted/60">
            <tr>
              <th className="px-4 py-3 font-medium">Name</th>
              <th className="px-4 py-3 font-medium">Key prefix</th>
              <th className="px-4 py-3 font-medium">Status</th>
              <th className="px-4 py-3"><span className="sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            {keys.map((key) => (
              <tr key={key.key_id} className="border-t">
                <td className="px-4 py-3">{key.name}</td>
                <td className="px-4 py-3 font-mono text-xs">{key.key_prefix}…</td>
                <td className="px-4 py-3">{key.revoked_at ? "Revoked" : "Active"}</td>
                <td className="px-4 py-3 text-right">
                  {!key.revoked_at && (
                    <button
                      type="button"
                      disabled={busy}
                      onClick={() => void revoke(key.key_id)}
                      className="text-destructive hover:underline disabled:opacity-50"
                    >
                      Revoke
                    </button>
                  )}
                </td>
              </tr>
            ))}
            {keys.length === 0 && (
              <tr>
                <td colSpan={4} className="text-muted-foreground px-4 py-6 text-center">
                  No worker keys created yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
