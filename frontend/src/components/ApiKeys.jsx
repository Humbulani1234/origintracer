import { useCallback, useEffect, useState } from "react";
import { api } from "../api/client";
import { useAuth } from "../auth/AuthContext";

export default function ApiKeys() {
  const { enabled } = useAuth();
  const [keys, setKeys] = useState([]);
  const [name, setName] = useState("");
  const [createdKey, setCreatedKey] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const load = useCallback(async () => {
    const result = await api.apiKeys();
    setKeys(result.data ?? []);
  }, []);

  useEffect(() => {
    if (enabled) load().catch((err) => setError(err.message));
  }, [enabled, load]);

  async function create(event) {
    event.preventDefault();
    setBusy(true); setError(""); setCreatedKey("");
    try {
      const result = await api.createApiKey(name);
      setCreatedKey(result.api_key); setName(""); await load();
    } catch (err) { setError(err.message); }
    finally { setBusy(false); }
  }

  async function revoke(keyId) {
    setBusy(true); setError("");
    try { await api.revokeApiKey(keyId); await load(); }
    catch (err) { setError(err.message); }
    finally { setBusy(false); }
  }

  if (!enabled) return <section className="auth-note">Configure OIDC sign-in to manage worker API keys.</section>;
  return <section className="api-keys">
    <p>Create a separate key for each instrumented worker. A new key is shown once; store it in that worker’s environment.</p>
    <form onSubmit={create}>
      <input value={name} onChange={(event) => setName(event.target.value)} maxLength={80} placeholder="Worker key name" aria-label="Worker key name" required />
      <button disabled={busy}>Create key</button>
    </form>
    {createdKey && <div className="created-key"><strong>Copy this key now; it will not be shown again.</strong><code>{createdKey}</code></div>}
    {error && <div className="auth-error" role="alert">{error}</div>}
    <div className="api-key-table"><table><thead><tr><th>Name</th><th>Prefix</th><th>Status</th><th /></tr></thead><tbody>
      {keys.map((key) => <tr key={key.key_id}><td>{key.name}</td><td><code>{key.key_prefix}…</code></td><td>{key.revoked_at ? "Revoked" : "Active"}</td><td>{!key.revoked_at && <button disabled={busy} onClick={() => revoke(key.key_id)}>Revoke</button>}</td></tr>)}
      {!keys.length && <tr><td colSpan="4">No worker keys created yet.</td></tr>}
    </tbody></table></div>
  </section>;
}
