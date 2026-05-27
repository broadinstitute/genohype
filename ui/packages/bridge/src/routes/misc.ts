import express from 'express'
import { chatDb } from '../db/database'
import { checkJwt, addUserToRequest, isAuthEnabled, isViewerOrAdmin, verifyJwt } from '../auth/auth'

const router = express.Router()

router.get('/tool_results/:resultId', checkJwt, async (req, res) => {
  try {
    const userId = isAuthEnabled ? (req as any).auth.payload.sub : 'anonymous'
    const resultData = await chatDb.getToolResult(req.params.resultId, userId)
    if (resultData) {
      res.json(resultData)
    } else {
      res.status(404).json({ error: 'Tool result not found or access denied' })
    }
  } catch (error: any) {
    console.error('Failed to get tool result:', error.message)
    res.status(500).json({ error: 'Failed to get tool result' })
  }
})

router.get('/health', async (_req, res) => {
  const dbHealthy = await chatDb.healthCheck()
  res.json({
    status: dbHealthy ? 'healthy' : 'degraded',
    database: dbHealthy ? 'connected' : 'disconnected',
  })
})

router.post('/feedback', async (req, res) => {
  try {
    let userId = 'anonymous'
    if (isAuthEnabled && req.headers.authorization?.startsWith('Bearer ')) {
      try {
        const token = req.headers.authorization.substring(7)
        const payload = await verifyJwt(token)
        userId = (payload?.sub as string) || 'anonymous'
        if (userId !== 'anonymous') {
          await chatDb.upsertUser({ userId, email: payload?.email as string, name: payload?.name as string })
        }
      } catch {
        // Use anonymous
      }
    }
    await chatDb.saveFeedback({ ...req.body, userId })
    res.status(201).json({ success: true })
  } catch (error: any) {
    console.error('Failed to save feedback:', error.message)
    res.status(500).json({ error: 'Failed to save feedback' })
  }
})

router.get('/feedback', checkJwt, addUserToRequest, isViewerOrAdmin, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit as string) || 50
    const offset = parseInt(req.query.offset as string) || 0
    const feedback = await chatDb.getFeedback(limit, offset)
    res.json(feedback)
  } catch (error: any) {
    console.error('Failed to get feedback:', error.message)
    res.status(500).json({ error: 'Failed to get feedback' })
  }
})

router.post('/analytics/event', async (req, res) => {
  try {
    let userId: string | undefined
    if (isAuthEnabled && req.headers.authorization) {
      try {
        const token = req.headers.authorization.substring(7)
        userId = (await verifyJwt(token))?.sub as string
      } catch {
        // Ignore auth errors for analytics
      }
    }
    const { threadId, eventType, payload, sessionId } = req.body
    if (!eventType) return res.status(400).json({ error: 'eventType is required' })
    await chatDb.saveAnalyticsEvent({ userId, threadId, eventType, payload, sessionId })
    res.status(201).json({ success: true })
  } catch (error: any) {
    console.error('Failed to save analytics event:', error.message)
    res.status(500).json({ error: 'Failed to save analytics event' })
  }
})

router.get('/users', checkJwt, addUserToRequest, isViewerOrAdmin, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit as string) || 50
    const offset = parseInt(req.query.offset as string) || 0
    const users = await chatDb.getUsers(limit, offset)
    res.json(users)
  } catch (error: any) {
    console.error('Failed to get users:', error.message)
    res.status(500).json({ error: 'Failed to get users' })
  }
})

export default router
