import express from 'express'
import { chatDb } from '../db/database'
import { checkJwt, addUserToRequest, isAdmin } from '../auth/auth'

const router = express.Router()

router.use(checkJwt, addUserToRequest, isAdmin)

router.get('/stats', async (_req, res) => {
  try {
    const stats = await chatDb.getUsageStats()
    res.json(stats)
  } catch (error: any) {
    console.error('Failed to get usage stats:', error.message)
    res.status(500).json({ error: 'Failed to get usage stats' })
  }
})

router.get('/stats/suggestions', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit as string) || 20
    const stats = await chatDb.getSuggestionStats(limit)
    res.json(stats)
  } catch (error: any) {
    console.error('Failed to get suggestion stats:', error.message)
    res.status(500).json({ error: 'Failed to get suggestion stats' })
  }
})

router.get('/threads', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit as string) || 50
    const offset = parseInt(req.query.offset as string) || 0
    const threads = await chatDb.getAllThreadsForAdmin(limit, offset)
    res.json(threads)
  } catch (error: any) {
    console.error('Failed to get all threads for admin:', error.message)
    res.status(500).json({ error: 'Failed to get threads' })
  }
})

router.get('/threads/:threadId/messages', async (req, res) => {
  try {
    const messages = await chatDb.getMessagesForAdmin(req.params.threadId)
    if (messages === null) {
      return res.status(403).json({ error: 'Access to this thread is denied by user privacy settings.' })
    }
    res.json(messages)
  } catch (error: any) {
    console.error('Failed to get messages for admin:', error.message)
    res.status(500).json({ error: 'Failed to get messages' })
  }
})

router.delete('/threads/:threadId', async (req, res) => {
  try {
    await chatDb.deleteThreadAsAdmin(req.params.threadId)
    res.json({ success: true })
  } catch (error: any) {
    console.error('Failed to delete thread as admin:', error.message)
    res.status(500).json({ error: 'Failed to delete thread' })
  }
})

export default router
