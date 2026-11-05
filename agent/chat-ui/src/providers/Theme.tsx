"use client";

import { ThemeProvider } from "next-themes";
import type { ComponentProps } from "react";

export function AppThemeProvider({ children, ...props }: ComponentProps<typeof ThemeProvider>) {
  return (
    <ThemeProvider
      attribute="class"
      defaultTheme="light"
      enableSystem={false}
      disableTransitionOnChange
      {...props}
    >
      {children}
    </ThemeProvider>
  );
}
