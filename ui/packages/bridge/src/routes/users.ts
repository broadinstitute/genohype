import express from 'express'
import { chatDb } from '../db/database'
import { checkJwt, addUserToRequest, isAdmin } from '../auth/auth'

const router = express.Router()

router.get('/me', checkJwt, addUserToRequest, (req, res) => {
  if (!req.user) return res.status(404).json({ error: 'User not found' })
  res.json(req.user)
})

router.put('/me/preferences', checkJwt, addUserToRequest, async (req, res) => {
  try {
    if (!req.user) return res.status(401).json({ error: 'Unauthorized' })
    const { allowAdminViewing } = req.body
    if (typeof allowAdminViewing !== 'boolean') {
      return res.status(400).json({ error: 'Invalid value for allowAdminViewing' })
    }
    await chatDb.updateUserPrivacy(req.user.userId, allowAdminViewing)
    res.json({ success: true })
  } catch (error: any) {
    console.error('Failed to update user preferences:', error.message)
    res.status(500).json({ error: 'Failed to update user preferences' })
  }
})

router.put('/:userId/role', checkJwt, addUserToRequest, isAdmin, async (req, res) => {
  try {
    const { userId } = req.params
    const { role } = req.body
    if (!['user', 'viewer', 'admin'].includes(role)) {
      return res.status(400).json({ error: 'Invalid role provided' })
    }
    await chatDb.updateUserRole(userId, role)
    res.json({ success: true, userId, role })
  } catch (error: any) {
    console.error('Failed to update user role:', error.message)
    res.status(500).json({ error: 'Failed to update user role' })
  }
})

export default router
