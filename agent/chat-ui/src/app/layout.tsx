import type { Metadata } from "next";
import "./globals.css";
import React from "react";
import { NuqsAdapter } from "nuqs/adapters/next/app";
import { AppThemeProvider } from "@/providers/Theme";

export const metadata: Metadata = {
  title: "OriginTracer",
  description: "Chat with your OriginTracer investigation agent",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="font-sans">
        <AppThemeProvider>
          <NuqsAdapter>{children}</NuqsAdapter>
        </AppThemeProvider>
      </body>
    </html>
  );
}
