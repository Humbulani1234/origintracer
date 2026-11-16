import NextAuth from "next-auth";
import Keycloak from "next-auth/providers/keycloak";

const issuer = process.env.AUTH_KEYCLOAK_ISSUER;
const clientId = process.env.AUTH_KEYCLOAK_ID;
const clientSecret = process.env.AUTH_KEYCLOAK_SECRET;

export const oidcAuthEnabled = Boolean(issuer && clientId && clientSecret);

async function refreshAccessToken(token: Record<string, unknown>) {
  const refreshToken = token.refreshToken;
  if (!issuer || !clientId || !clientSecret || typeof refreshToken !== "string") {
    return {
      ...token,
      accessToken: undefined,
      accessTokenExpiresAt: 0,
      error: "RefreshAccessTokenError",
    };
  }
  try {
    const response = await fetch(
      `${issuer.replace(/\/+$/, "")}/protocol/openid-connect/token`,
      {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          client_id: clientId,
          client_secret: clientSecret,
          grant_type: "refresh_token",
          refresh_token: refreshToken,
        }),
        cache: "no-store",
      },
    );
    if (!response.ok) throw new Error("Token refresh failed");
    const refreshed = await response.json();
    return {
      ...token,
      accessToken: refreshed.access_token,
      accessTokenExpiresAt: Date.now() + Number(refreshed.expires_in ?? 0) * 1000,
      refreshToken: refreshed.refresh_token ?? refreshToken,
      error: undefined,
    };
  } catch {
    return {
      ...token,
      accessToken: undefined,
      accessTokenExpiresAt: 0,
      error: "RefreshAccessTokenError",
    };
  }
}

export const { handlers, auth, signIn, signOut } = NextAuth({
  trustHost: true,
  secret: process.env.AUTH_SECRET,
  basePath: `${process.env.NEXT_BASE_PATH ?? ""}/api/auth`,
  providers:
    oidcAuthEnabled && issuer && clientId && clientSecret
      ? [
          Keycloak({
            issuer,
            clientId,
            clientSecret,
            authorization: { params: { scope: "openid profile email" } },
          }),
        ]
      : [],
  session: { strategy: "jwt" },
  callbacks: {
    async jwt({ token, account }) {
      if (account) {
        return {
          ...token,
          accessToken: account.access_token,
          accessTokenExpiresAt: Number(account.expires_at ?? 0) * 1000 || Date.now(),
          refreshToken: account.refresh_token,
          error: undefined,
        };
      }
      const expiresAt = Number(token.accessTokenExpiresAt ?? 0);
      if (typeof token.accessToken === "string" && Date.now() < expiresAt - 30_000) {
        return token;
      }
      return refreshAccessToken(token);
    },
    async session({ session, token }) {
      session.accessToken =
        !token.error && typeof token.accessToken === "string"
          ? token.accessToken
          : undefined;
      session.error =
        typeof token.error === "string" ? token.error : undefined;
      return session;
    },
  },
});
