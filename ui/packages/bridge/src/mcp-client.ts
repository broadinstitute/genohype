import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import type { MCPClient, MCPTool } from '@copilotkit/runtime'

export interface McpSpawnConfig {
  /** Command to spawn (e.g., 'backend' or '/path/to/backend'). */
  command: string
  /** Arguments for the command (e.g., ['--config', 'gbl.toml', 'mcp', 'stdio']). */
  args: string[]
  /** Extra environment variables. */
  env?: Record<string, string>
}

/**
 * MCP client that spawns a local process over stdio.
 *
 * Adapted from the gnomAD browser's copilotkit-server/src/mcp-client.ts.
 * Uses @modelcontextprotocol/sdk StdioClientTransport to spawn the
 * genohype Rust binary with `mcp stdio` subcommand.
 */
export class LocalMCPClient implements MCPClient {
  private client!: Client
  private connected = false

  constructor(private config: McpSpawnConfig) {}

  async connect(): Promise<void> {
    // Build environment: inherit process.env, overlay config.env
    const env: Record<string, string> = {}
    for (const [key, value] of Object.entries(process.env)) {
      if (value !== undefined) env[key] = value
    }
    if (this.config.env) Object.assign(env, this.config.env)

    const transport = new StdioClientTransport({
      command: this.config.command,
      args: this.config.args,
      env,
    })

    this.client = new Client(
      { name: 'genohype-copilot-bridge', version: '1.0.0' },
      { capabilities: {} },
    )

    await this.client.connect(transport)
    this.connected = true
  }

  /**
   * Normalize JSON Schema types for CopilotKit compatibility.
   * Flattens ["string", "null"] → "string" since CopilotKit expects scalar type strings.
   */
  private normalizeProperties(props: Record<string, any>): Record<string, any> {
    const normalized: Record<string, any> = {}
    for (const [key, prop] of Object.entries(props)) {
      const p = { ...prop }
      if (Array.isArray(p.type)) {
        // Take the first non-null type
        p.type = p.type.find((t: string) => t !== 'null') || p.type[0]
      }
      normalized[key] = p
    }
    return normalized
  }

  async tools(): Promise<Record<string, MCPTool>> {
    if (!this.connected) await this.connect()

    const response = await this.client.listTools()
    const toolsMap: Record<string, MCPTool> = {}

    for (const tool of response.tools) {
      const rawProps = (tool.inputSchema as any)?.properties || {}
      const normalizedProps = this.normalizeProperties(rawProps)
      const schema = tool.inputSchema ? {
        parameters: {
          properties: normalizedProps,
          required: (tool.inputSchema as any).required || [],
          jsonSchema: { ...tool.inputSchema as any, properties: normalizedProps },
        },
      } : undefined

      toolsMap[tool.name] = {
        description: tool.description,
        schema,
        execute: async (args: any) => {
          const result = await this.client.callTool({
            name: tool.name,
            arguments: args,
          })

          const structuredContent =
            (result as any).structuredContent || result._meta?.structuredContent
          if (structuredContent) {
            return { content: result.content, structuredContent }
          }
          return result.content
        },
      }
    }

    return toolsMap
  }

  async close(): Promise<void> {
    if (this.connected) {
      await this.client.close()
      this.connected = false
    }
  }
}
