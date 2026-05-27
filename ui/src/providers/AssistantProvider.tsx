import React, { createContext, useContext, useMemo } from "react";

/**
 * Configuration for the genomic assistant provider.
 *
 * Designed for inversion of control — the host application supplies
 * auth, navigation, and endpoint configuration so the assistant
 * components remain decoupled from any specific framework or auth provider.
 */
export interface AssistantProviderProps {
  /** URL of the MCP server endpoint (e.g., "/api/mcp"). */
  mcpEndpoint: string;

  /** Optional auth token provider — decouples from Auth0/Cognito/etc. */
  getAuthToken?: () => Promise<string>;

  /** Navigation callback — decouples from React Router / Next.js router. */
  onNavigate?: (url: string) => void;

  /** Optional CopilotKit runtime URL override. */
  copilotRuntimeUrl?: string;

  children: React.ReactNode;
}

interface AssistantContextValue {
  mcpEndpoint: string;
  getAuthToken?: () => Promise<string>;
  onNavigate?: (url: string) => void;
}

const AssistantContext = createContext<AssistantContextValue | null>(null);

/**
 * Root provider for the genomic AI assistant.
 *
 * Wrap your application (or a subtree) with this provider to enable
 * the assistant panel and MCP tool integration.
 *
 * ```tsx
 * <AssistantProvider mcpEndpoint="/api/mcp" onNavigate={(url) => navigate(url)}>
 *   <App />
 *   <AssistantPanel />
 * </AssistantProvider>
 * ```
 */
export function AssistantProvider({
  mcpEndpoint,
  getAuthToken,
  onNavigate,
  copilotRuntimeUrl: _copilotRuntimeUrl,
  children,
}: AssistantProviderProps) {
  const value = useMemo(
    () => ({ mcpEndpoint, getAuthToken, onNavigate }),
    [mcpEndpoint, getAuthToken, onNavigate]
  );

  return (
    <AssistantContext.Provider value={value}>
      {/* TODO: Wrap with CopilotKit provider once wired up */}
      {children}
    </AssistantContext.Provider>
  );
}

/** Access the assistant context from a child component. */
export function useAssistantContext(): AssistantContextValue {
  const ctx = useContext(AssistantContext);
  if (!ctx) {
    throw new Error(
      "useAssistantContext must be used within an <AssistantProvider>"
    );
  }
  return ctx;
}
