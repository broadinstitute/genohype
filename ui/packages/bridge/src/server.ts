import express from "express";
import { McpClient } from "./mcp-client";

export interface BridgeConfig {
  /** Port for the bridge server (default: 4111). */
  port?: number;

  /** URL of the genohype MCP server endpoint. */
  mcpEndpoint: string;

  /** Optional auth token to forward to the MCP server. */
  authToken?: string;
}

/**
 * Create and start the CopilotKit ↔ MCP bridge server.
 *
 * This Express server translates between the CopilotKit runtime protocol
 * and the genohype MCP server. It:
 * 1. Receives tool call requests from the CopilotKit frontend
 * 2. Forwards them to the genohype MCP server
 * 3. Streams results back to the CopilotKit runtime
 */
export function createBridgeServer(config: BridgeConfig) {
  const app = express();
  app.use(express.json());

  const mcpClient = new McpClient(config.mcpEndpoint, config.authToken);

  // Health check
  app.get("/health", (_req, res) => {
    res.json({ status: "ok" });
  });

  // CopilotKit runtime endpoint — receives tool call requests
  app.post("/api/copilotkit", async (req, res) => {
    try {
      // TODO: Implement CopilotKit runtime protocol handling
      // 1. Parse the CopilotKit request format
      // 2. Map tool calls to MCP tools/call requests
      // 3. Forward to the MCP server via mcpClient
      // 4. Format and return results in CopilotKit response format
      const { action } = req.body;

      if (action === "list-tools") {
        const tools = await mcpClient.listTools();
        res.json({ tools });
      } else if (action === "call-tool") {
        const { name, arguments: args } = req.body;
        const result = await mcpClient.callTool(name, args);
        res.json({ result });
      } else {
        res.status(400).json({ error: `unknown action: ${action}` });
      }
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "internal error";
      res.status(500).json({ error: message });
    }
  });

  return app;
}

// CLI entry point
if (process.argv[1] && import.meta.url.endsWith(process.argv[1])) {
  const port = parseInt(process.env.PORT ?? "4111", 10);
  const mcpEndpoint =
    process.env.MCP_ENDPOINT ?? "http://localhost:3000/api/mcp";

  const app = createBridgeServer({ port, mcpEndpoint });
  app.listen(port, () => {
    console.log(`copilot-bridge listening on :${port}`);
    console.log(`MCP endpoint: ${mcpEndpoint}`);
  });
}
