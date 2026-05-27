import express from 'express'
import adminRoutes from './admin'
import threadRoutes from './threads'
import userRoutes from './users'
import miscRoutes from './misc'

const router = express.Router()

// JSON body parser with increased limit for tool results
router.use(express.json({ limit: '50mb' }))

router.use('/admin', adminRoutes)
router.use('/threads', threadRoutes)
router.use('/users', userRoutes)
// Mount misc routes at the root of /api/copilotkit
router.use('/', miscRoutes)

export default router
