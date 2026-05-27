/**
 * MCP tool definition as returned by the genohype MCP server.
 */
export interface McpToolDefinition {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
}

/**
 * Result of an MCP tool call.
 */
export interface McpToolResult {
  content: unknown;
  error?: string;
}

/**
 * Client for communicating with a genohype MCP server.
 *
 * Handles HTTP transport to the MCP server's tools/list and tools/call
 * endpoints. Used by the bridge server to forward CopilotKit requests.
 */
export class McpClient {
  private endpoint: string;
  private authToken?: string;

  constructor(endpoint: string, authToken?: string) {
    this.endpoint = endpoint.replace(/\/$/, "");
    this.authToken = authToken;
  }

  /** List all available tools from the MCP server. */
  async listTools(): Promise<McpToolDefinition[]> {
    const res = await fetch(`${this.endpoint}/tools/list`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify({}),
    });

    if (!res.ok) {
      throw new Error(`MCP tools/list failed: ${res.status} ${res.statusText}`);
    }

    return res.json();
  }

  /** Call a tool on the MCP server. */
  async callTool(
    name: string,
    args: Record<string, unknown>
  ): Promise<McpToolResult> {
    const res = await fetch(`${this.endpoint}/tools/call`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify({ name, arguments: args }),
    });

    if (!res.ok) {
      throw new Error(`MCP tools/call failed: ${res.status} ${res.statusText}`);
    }

    return res.json();
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (this.authToken) {
      h["Authorization"] = `Bearer ${this.authToken}`;
    }
    return h;
  }
}
