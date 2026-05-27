import { Request, Response, NextFunction } from 'express'
import { createRemoteJWKSet, jwtVerify, JWTPayload } from 'jose'

// Extend Express Request type
declare global {
  namespace Express {
    interface Request {
      user?: {
        userId: string
        email?: string
        name?: string
        role?: 'user' | 'viewer' | 'admin'
        allowAdminViewing?: boolean
      } | null
    }
  }
}

export const isAuthEnabled = process.env.AUTH_ENABLED === 'true'

const ISSUER_BASE_URL = process.env.AUTH_ISSUER_BASE_URL || ''
const AUDIENCE = process.env.AUTH_AUDIENCE || ''

const JWKS = isAuthEnabled && ISSUER_BASE_URL
  ? createRemoteJWKSet(new URL(`${ISSUER_BASE_URL}.well-known/jwks.json`))
  : null

export const verifyJwt = async (token: string): Promise<JWTPayload | null> => {
  if (!isAuthEnabled || !JWKS) return null
  const { payload } = await jwtVerify(token, JWKS, {
    issuer: ISSUER_BASE_URL,
    audience: AUDIENCE,
  })
  return payload
}

// JWT validation middleware
export const checkJwt = async (req: Request, res: Response, next: NextFunction) => {
  if (!isAuthEnabled) return next()

  const authHeader = req.headers.authorization
  if (!authHeader?.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Missing or invalid authorization header' })
  }

  try {
    const token = authHeader.substring(7)
    const payload = await verifyJwt(token)
    if (!payload) {
      return res.status(401).json({ error: 'Invalid token' })
    }
    ;(req as any).auth = { payload }
    next()
  } catch (error: any) {
    console.error('JWT verification failed:', error.message)
    return res.status(401).json({ error: 'Invalid token' })
  }
}

// Middleware to fetch user from DB and attach to request
// Note: requires chatDb to be passed in or imported
let _getUserFn: ((userId: string) => Promise<any>) | null = null
let _upsertUserFn: ((user: any) => Promise<void>) | null = null

export const setUserFunctions = (
  getUser: (userId: string) => Promise<any>,
  upsertUser: (user: any) => Promise<void>
) => {
  _getUserFn = getUser
  _upsertUserFn = upsertUser
}

export const addUserToRequest = async (req: Request, res: Response, next: NextFunction) => {
  if (!isAuthEnabled) {
    req.user = null
    return next()
  }

  try {
    const userId = (req as any).auth?.payload?.sub
    const userEmail = (req as any).auth?.payload?.email
    const userName = (req as any).auth?.payload?.name

    if (userId && _upsertUserFn && _getUserFn) {
      await _upsertUserFn({ userId, email: userEmail, name: userName })
      const user = await _getUserFn(userId)
      req.user = user
    }
    next()
  } catch (error: any) {
    console.error('Failed to add user to request:', error.message)
    res.status(500).json({ error: 'Internal server error' })
  }
}

export const isAdmin = (req: Request, res: Response, next: NextFunction) => {
  if (!isAuthEnabled) return next()
  if (req.user?.role === 'admin') return next()
  return res.status(403).json({ error: 'Forbidden: Admins only' })
}

export const isViewerOrAdmin = (req: Request, res: Response, next: NextFunction) => {
  if (!isAuthEnabled) return next()
  if (req.user?.role === 'admin' || req.user?.role === 'viewer') return next()
  return res.status(403).json({ error: 'Forbidden: Viewers and Admins only' })
}
