import React, { createContext, useContext, useMemo, useState, useCallback, useRef } from 'react'
import { CopilotKit } from '@copilotkit/react-core'
import type { AssistantDisplayMode } from '../components/AssistantPanel'

export interface AssistantProviderProps {
  /** URL of the CopilotKit runtime endpoint (e.g., "/api/copilotkit"). */
  runtimeUrl: string

  /** Optional auth token provider — decouples from Auth0/Cognito/etc. */
  getAuthToken?: () => Promise<string>

  /** Navigation callback — decouples from React Router / Next.js router. */
  onNavigate?: (url: string) => void

  /** Initial display mode for the panel. */
  defaultMode?: AssistantDisplayMode

  /** Sidebar rendered outside CopilotKit (survives thread switches). */
  persistentSidebar?: React.ReactNode

  children: React.ReactNode
}

interface AssistantContextValue {
  runtimeUrl: string
  getAuthToken?: () => Promise<string>
  onNavigate?: (url: string) => void
  threadId: string
  setThreadId: (id: string) => void
  newChat: () => void
  displayMode: AssistantDisplayMode
  setDisplayMode: (mode: AssistantDisplayMode) => void
  persistentSidebar?: React.ReactNode
  /** Incremented on each thread change — triggers useThreadLoader */
  threadVersion: number
}

const AssistantContext = createContext<AssistantContextValue | null>(null)

const generateId = () => typeof crypto !== 'undefined' && crypto.randomUUID
  ? crypto.randomUUID()
  : Math.random().toString(36).slice(2) + Date.now().toString(36)

export function AssistantProvider({
  runtimeUrl,
  getAuthToken,
  onNavigate,
  defaultMode = 'closed',
  persistentSidebar,
  children,
}: AssistantProviderProps) {
  const [threadId, setThreadId] = useState(generateId)
  const [displayMode, setDisplayMode] = useState<AssistantDisplayMode>(defaultMode)
  const [threadVersion, setThreadVersion] = useState(0)

  // Wrap setThreadId to also bump the version counter
  const handleSetThreadId = useCallback((id: string) => {
    setThreadId(id)
    setThreadVersion(v => v + 1)
  }, [])

  const newChat = useCallback(async () => {
    const id = generateId()
    setThreadId(id)
    setThreadVersion(v => v + 1)
    // Eagerly create the thread in Postgres so it appears in the sidebar immediately
    try {
      const headers: Record<string, string> = { 'Content-Type': 'application/json' }
      if (getAuthToken) {
        try { headers.Authorization = `Bearer ${await getAuthToken()}` } catch { /* noop */ }
      }
      await fetch(`${runtimeUrl}/threads`, {
        method: 'POST',
        headers,
        body: JSON.stringify({ threadId: id }),
      })
    } catch { /* thread will be created on first message anyway */ }
  }, [runtimeUrl, getAuthToken])

  const value = useMemo(
    () => ({ runtimeUrl, getAuthToken, onNavigate, threadId, setThreadId: handleSetThreadId, newChat, displayMode, setDisplayMode, persistentSidebar, threadVersion }),
    [runtimeUrl, getAuthToken, onNavigate, threadId, handleSetThreadId, newChat, displayMode, persistentSidebar, threadVersion]
  )

  // No key={threadId} — we use setMessages to clear/load instead of remounting
  return (
    <AssistantContext.Provider value={value}>
      <CopilotKit runtimeUrl={runtimeUrl}>
        {children}
      </CopilotKit>
    </AssistantContext.Provider>
  )
}

/** Access the assistant context from a child component. */
export function useAssistantContext(): AssistantContextValue {
  const ctx = useContext(AssistantContext)
  if (!ctx) {
    throw new Error('useAssistantContext must be used within an <AssistantProvider>')
  }
  return ctx
}
