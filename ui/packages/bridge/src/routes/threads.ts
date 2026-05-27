import express from 'express'
import { chatDb } from '../db/database'
import { checkJwt, isAuthEnabled } from '../auth/auth'

const router = express.Router()

router.get('/', checkJwt, async (req, res) => {
  try {
    const userId = isAuthEnabled ? (req as any).auth.payload.sub : 'anonymous'
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100)
    const offset = parseInt(req.query.offset as string) || 0
    const threads = await chatDb.listThreads(userId, limit, offset)
    res.json(threads)
  } catch (error: any) {
    console.error('Failed to list threads:', error.message)
    res.status(500).json({ error: 'Failed to list threads' })
  }
})

router.post('/', checkJwt, async (req, res) => {
  try {
    const userId = isAuthEnabled ? (req as any).auth.payload.sub : 'anonymous'
    const { threadId, model } = req.body
    if (!threadId) return res.status(400).json({ error: 'threadId is required' })
    await chatDb.ensureThread(threadId, userId, model)
    res.json({ success: true, threadId })
  } catch (error: any) {
    console.error('Failed to create thread:', error.message)
    res.status(500).json({ error: 'Failed to create thread' })
  }
})

router.post('/:threadId/context', checkJwt, async (req, res) => {
  try {
    const userId = isAuthEnabled ? (req as any).auth.payload.sub : 'anonymous'
    const { threadId } = req.params
    const { context } = req.body
    if (!context || !context.type || !context.id) {
      return res.status(400).json({ error: 'Invalid context object provided.' })
    }
    await chatDb.addContextToThread(threadId, userId, context)
    res.status(200).json({ success: true })
  } catch (error: any) {
    console.error('Failed to add context to thread:', error.message)
    res.status(500).json({ error: 'Failed to update thread context' })
  }
})

router.get('/:threadId/messages', checkJwt, async (req, res) => {
  try {
    const userId = isAuthEnabled ? (req as any).auth.payload.sub : 'anonymous'
    const messages = await chatDb.getMessages(req.params.threadId, userId)
    res.json(messages)
  } catch (error: any) {
    console.error('Failed to get messages:', error.message)
    res.status(500).json({ error: 'Failed to get messages' })
  }
})

router.delete('/:threadId', checkJwt, async (req, res) => {
  try {
    const userId = isAuthEnabled ? (req as any).auth.payload.sub : 'anonymous'
    await chatDb.deleteThread(req.params.threadId, userId)
    res.json({ success: true })
  } catch (error: any) {
    console.error('Failed to delete thread:', error.message)
    res.status(500).json({ error: 'Failed to delete thread' })
  }
})

export default router
