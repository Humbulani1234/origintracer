import { useAuth } from "../auth/AuthContext";
import { userManager } from "../auth/oidc";

function GoogleMark() {
  return <svg aria-hidden="true" viewBox="0 0 48 48" width="20" height="20">
    <path fill="#4285F4" d="M43.6 24.5c0-1.4-.1-2.7-.4-4H24v7.6h11a9.4 9.4 0 0 1-4.1 6.2v5h6.6c3.9-3.6 6.1-8.8 6.1-14.8Z" />
    <path fill="#34A853" d="M24 44c5.5 0 10.1-1.8 13.5-4.8l-6.6-5c-1.8 1.2-4 1.9-6.9 1.9-5.3 0-9.8-3.6-11.4-8.4H5.8v5.2A20 20 0 0 0 24 44Z" />
    <path fill="#FBBC05" d="M12.6 27.7a12 12 0 0 1 0-7.4v-5.2H5.8a20 20 0 0 0 0 17.8l6.8-5.2Z" />
    <path fill="#EA4335" d="M24 11.9c3 0 5.7 1 7.8 3.1l5.8-5.8C34.1 6 29.5 4 24 4A20 20 0 0 0 5.8 15.1l6.8 5.2c1.6-4.8 6.1-8.4 11.4-8.4Z" />
  </svg>;
}

export default function AuthGate({ children }) {
  const { user, ready, error, enabled } = useAuth();
  if (!enabled || (ready && user)) return children;
  if (!ready) return <main className="auth-screen"><p>Checking your sign-in…</p></main>;
  return <main className="auth-screen"><section className="auth-card">
    <div className="auth-brand">ORIGINTRACER</div>
    {error && <div className="auth-error" role="alert">{error}</div>}
    <button className="auth-primary" onClick={() => userManager.signinRedirect()}>Sign in or create an account</button>
    <div className="auth-divider"><span />or<span /></div>
    <button className="auth-google" onClick={() => userManager.signinRedirect({ extraQueryParams: { kc_idp_hint: "google" } })}>
      <GoogleMark /> Continue with Google
    </button>
  </section></main>;
}

export function AuthControls() {
  const { user, enabled } = useAuth();
  if (!enabled || !user) return null;
  return <div className="auth-controls">
    <span>Account</span>
    <button onClick={() => userManager.signoutRedirect()}>Sign out</button>
  </div>;
}
