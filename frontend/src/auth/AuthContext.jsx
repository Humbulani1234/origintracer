import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { api } from "../api/client";
import { callbackPath, oidcEnabled, silentCallbackPath, userManager } from "./oidc";

let callbackResult;

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [ready, setReady] = useState(!oidcEnabled);
  const [error, setError] = useState("");
  const [signingOut, setSigningOut] = useState(false);

  useEffect(() => {
    if (!userManager) return undefined;
    let active = true;
    const clearUser = () => setUser(null);
    userManager.events.addUserLoaded(setUser);
    userManager.events.addUserUnloaded(clearUser);
    api.setAccessTokenProvider(async () => {
      const current = await userManager.getUser();
      return current?.expired ? null : current?.access_token ?? null;
    });

    async function initialize() {
      try {
        if (window.location.pathname === silentCallbackPath) {
          await userManager.signinSilentCallback();
          return;
        }
        if (window.location.pathname === callbackPath) {
          callbackResult ??= userManager.signinRedirectCallback();
          await callbackResult;
          window.history.replaceState({}, "", import.meta.env.BASE_URL);
        }
        const current = await userManager.getUser();
        if (active) setUser(current?.expired ? null : current);
      } catch {
        if (active) setError("Sign-in could not be completed. Please try again.");
      } finally {
        if (active) setReady(true);
      }
    }
    void initialize();
    return () => {
      active = false;
      userManager.events.removeUserLoaded(setUser);
      userManager.events.removeUserUnloaded(clearUser);
    };
  }, []);

  const value = useMemo(
    () => ({ user, ready, error, enabled: oidcEnabled, signingOut, setSigningOut }),
    [user, ready, error, signingOut],
  );
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const value = useContext(AuthContext);
  if (!value) throw new Error("useAuth must be used inside AuthProvider");
  return value;
}
