import { UserManager, WebStorageStateStore } from "oidc-client-ts";

const authority = import.meta.env.VITE_OIDC_AUTHORITY;
const clientId = import.meta.env.VITE_OIDC_CLIENT_ID;
export const oidcEnabled = Boolean(authority && clientId);

const basePath = import.meta.env.BASE_URL || "/";
const appUrl = `${window.location.origin}${basePath}`;
export const callbackPath = `${basePath.replace(/\/$/, "")}/auth/callback`;
export const silentCallbackPath = `${basePath.replace(/\/$/, "")}/auth/silent-callback`;

export const userManager = oidcEnabled
  ? new UserManager({
      authority,
      client_id: clientId,
      redirect_uri:
        import.meta.env.VITE_OIDC_REDIRECT_URI || `${appUrl}auth/callback`,
      silent_redirect_uri: `${appUrl}auth/silent-callback`,
      post_logout_redirect_uri: appUrl,
      response_type: "code",
      scope: "openid profile email",
      userStore: new WebStorageStateStore({ store: window.sessionStorage }),
      automaticSilentRenew: true,
    })
  : null;
