import React from "react";

/**
 * Display mode for the assistant panel.
 */
export type AssistantDisplayMode = "side" | "modal" | "floating";

/**
 * Props for the assistant chat panel.
 */
export interface AssistantPanelProps {
  /** How the panel is rendered — sidebar, modal overlay, or floating bubble. */
  defaultMode?: AssistantDisplayMode;

  /** Panel title shown in the header. */
  title?: string;

  /** Placeholder text for the chat input. */
  placeholder?: string;

  /** Initial system instructions prepended to the conversation. */
  instructions?: string;

  /** Whether the panel starts open. */
  defaultOpen?: boolean;

  /** CSS class name for the outer container. */
  className?: string;
}

/**
 * AI assistant chat panel that connects to the MCP server.
 *
 * Renders a chat interface with tool execution visualization.
 * Must be used within an `<AssistantProvider>`.
 *
 * ```tsx
 * <AssistantPanel defaultMode="side" title="Genomic Assistant" />
 * ```
 */
export function AssistantPanel({
  defaultMode: _defaultMode = "side",
  title = "Genomic Assistant",
  placeholder = "Ask about a variant, gene, or region...",
  instructions: _instructions,
  defaultOpen: _defaultOpen = false,
  className,
}: AssistantPanelProps) {
  // TODO: Wire up CopilotKit chat component with MCP tool rendering
  return (
    <div className={className} data-testid="assistant-panel">
      <div style={{ padding: "1rem", borderLeft: "1px solid #e0e0e0" }}>
        <h3>{title}</h3>
        <p style={{ color: "#666", fontSize: "0.875rem" }}>{placeholder}</p>
        <p style={{ color: "#999", fontSize: "0.75rem" }}>
          Assistant panel — connect CopilotKit to enable chat
        </p>
      </div>
    </div>
  );
}
