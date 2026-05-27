import express from 'express'
import cors from 'cors'
import {
  CopilotRuntime,
  GoogleGenerativeAIAdapter,
  copilotRuntimeNodeHttpEndpoint,
  convertMCPToolsToActions,
  generateMcpToolInstructions,
} from '@copilotkit/runtime'
import { LocalMCPClient, type McpSpawnConfig } from './mcp-client'
import { chatDb } from './db/database'
import { isAuthEnabled, verifyJwt, setUserFunctions } from './auth/auth'
import copilotRoutes from './routes/index'

// Wire up auth user functions to the database
setUserFunctions(
  (userId: string) => chatDb.getUser(userId),
  (user: any) => chatDb.upsertUser(user)
)

// Store userId per thread for the current request
const threadUserIdMap = new Map<string, string>()
let currentRequestUserId: string | null = null

export interface BridgeConfig {
  /** Port for the bridge server (default: 4111). */
  port?: number

  /** MCP server spawn configuration. */
  mcpConfig: McpSpawnConfig

  /** Google AI API key (or set GOOGLE_GENERATIVE_AI_API_KEY env var). */
  googleApiKey?: string

  /** Model to use (default: 'gemini-2.5-flash'). */
  model?: string

  /** Allowed CORS origins. */
  corsOrigins?: string[]

  /** Enable PostgreSQL persistence (requires DATABASE_URL). */
  enablePersistence?: boolean
}

/**
 * Create and configure the CopilotKit <-> MCP bridge server.
 *
 * Spawns the genohype Rust binary as an MCP stdio server, discovers its
 * tools, converts them to CopilotKit actions, and exposes them via the
 * CopilotKit runtime endpoint.
 */
export async function createBridgeServer(config: BridgeConfig) {
  const app = express()

  const apiKey = config.googleApiKey || process.env.GOOGLE_GENERATIVE_AI_API_KEY || ''
  const modelName = config.model || process.env.COPILOT_MODEL || 'gemini-3.1-flash'
  const enablePersistence = config.enablePersistence ?? !!process.env.DATABASE_URL

  // Spawn MCP server and discover tools
  console.log('Connecting to MCP server...')
  const mcpClient = new LocalMCPClient(config.mcpConfig)
  await mcpClient.connect()
  const mcpTools = await mcpClient.tools()
  console.log(`Discovered ${Object.keys(mcpTools).length} MCP tools:`, Object.keys(mcpTools).join(', '))

  // Convert MCP tools to CopilotKit actions
  const mcpActions = convertMCPToolsToActions(mcpTools, 'local://genohype')
  const mcpInstructions = generateMcpToolInstructions(mcpTools)

  const serviceAdapter = new GoogleGenerativeAIAdapter({
    model: modelName,
    apiKey,
  })

  // Persistence middleware — saves messages to Postgres after each request
  const middleware = enablePersistence ? {
    onBeforeRequest: async ({ threadId, inputMessages }: any) => {
      try {
        if (!threadId) return
        console.log(`[bridge] Chat request: threadId=${threadId}, messages=${inputMessages?.length || 0}`)
        if (currentRequestUserId && !threadUserIdMap.has(threadId)) {
          threadUserIdMap.set(threadId, currentRequestUserId)
        }
        const userId = threadUserIdMap.get(threadId)
        if (isAuthEnabled && userId) {
          const ownerId = await chatDb.getThreadOwner(threadId)
          if (ownerId && ownerId !== userId) {
            throw new Error('User does not have access to this thread.')
          }
        }
      } catch (error: any) {
        console.error('[bridge] onBeforeRequest error:', error.message)
      }
    },
    onAfterRequest: async ({ threadId, inputMessages, outputMessages, properties }: any) => {
      try {
        if (!threadId) return
        const userId = isAuthEnabled ? (threadUserIdMap.get(threadId) ?? 'anonymous') : 'anonymous'
        const allMessages = [...(inputMessages || []), ...(outputMessages || [])]
        const model = properties?.forwardedParameters?.model
        await chatDb.saveMessages(threadId, userId, allMessages, model)
        console.log(`[bridge] Saved ${allMessages.length} messages for thread ${threadId}`)
        threadUserIdMap.delete(threadId)
      } catch (error: any) {
        console.error('[bridge] onAfterRequest error:', error.message)
      }
    },
  } : undefined

  const runtime = new CopilotRuntime({
    actions: mcpActions,
    ...(middleware ? { middleware } : {}),
  })

  const handler = copilotRuntimeNodeHttpEndpoint({
    endpoint: '/api/copilotkit',
    runtime,
    serviceAdapter,
  })

  const corsOptions = {
    origin: config.corsOrigins || [
      'http://localhost:5173', // Vite dev
      'http://localhost:3000',
    ],
    credentials: true,
  }

  // Extract userId from JWT before CopilotKit processes the request
  app.use('/api/copilotkit', async (req, _res, next) => {
    if (isAuthEnabled && req.headers.authorization?.startsWith('Bearer ')) {
      try {
        const token = req.headers.authorization.substring(7)
        const payload = await verifyJwt(token)
        if (payload?.sub) {
          currentRequestUserId = payload.sub as string
          // Upsert user
          await chatDb.upsertUser({
            userId: currentRequestUserId,
            email: payload.email as string,
            name: payload.name as string,
          })
        }
      } catch {
        // Continue without auth
      }
    } else {
      currentRequestUserId = 'anonymous'
    }
    next()
  })

  // Health check
  app.get('/health', async (_req, res) => {
    const dbHealthy = enablePersistence ? await chatDb.healthCheck() : true
    res.json({
      status: dbHealthy ? 'ok' : 'degraded',
      model: modelName,
      tools: Object.keys(mcpTools).length,
      persistence: enablePersistence,
      database: enablePersistence ? (dbHealthy ? 'connected' : 'disconnected') : 'disabled',
    })
  })

  app.use(cors(corsOptions))

  // Persistence routes — mounted at explicit sub-paths only
  // Uses express.json only on these sub-paths to avoid consuming the body stream
  // before CopilotKit's handler can read it
  if (enablePersistence) {
    const jsonParser = express.json({ limit: '50mb' })
    app.use('/api/copilotkit/threads', jsonParser, (await import('./routes/threads')).default)
    app.use('/api/copilotkit/admin', jsonParser, (await import('./routes/admin')).default)
    app.use('/api/copilotkit/users', jsonParser, (await import('./routes/users')).default)
    // Misc routes handle /feedback, /tool_results, /health, /analytics, etc.
    app.use('/api/copilotkit/feedback', jsonParser, (await import('./routes/misc')).default)
    app.use('/api/copilotkit/tool_results', jsonParser, (await import('./routes/misc')).default)
    app.use('/api/copilotkit/health', jsonParser, (await import('./routes/misc')).default)
    app.use('/api/copilotkit/analytics', jsonParser, (await import('./routes/misc')).default)
    console.log('[bridge] Persistence routes mounted')
  }

  // CopilotKit runtime endpoint — catch-all for the runtime (POST, info, etc.)
  app.all('/api/copilotkit', (req, res, next) => {
    ;(async () => handler(req, res))().catch(next)
  })
  app.all('/api/copilotkit/*', (req, res, next) => {
    ;(async () => handler(req, res))().catch(next)
  })

  return app
}

export { LocalMCPClient, type McpSpawnConfig } from './mcp-client'
export { chatDb } from './db/database'
export type { ChatThread, ChatMessage, Feedback, User } from './db/database'

// CLI entry point
const isMain = process.argv[1] && (
  import.meta.url.endsWith(process.argv[1]) ||
  process.argv[1].endsWith('server.js') ||
  process.argv[1].endsWith('server.ts')
)

if (isMain) {
  const port = parseInt(process.env.PORT ?? '4111', 10)

  const mcpCommand = process.env.MCP_COMMAND || 'backend'
  const mcpArgs = (process.env.MCP_ARGS || '--config examples/gnomad/gbl.toml mcp stdio').split(' ')

  createBridgeServer({
    port,
    mcpConfig: { command: mcpCommand, args: mcpArgs },
  }).then(app => {
    app.listen(port, () => {
      console.log(`copilot-bridge listening on :${port}`)
      console.log(`MCP command: ${mcpCommand} ${mcpArgs.join(' ')}`)
    })
  }).catch(err => {
    console.error('Failed to start bridge:', err)
    process.exit(1)
  })
}
