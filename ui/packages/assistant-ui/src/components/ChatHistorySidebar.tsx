import React, { useEffect, useState, useCallback, useMemo } from 'react'
import styled from 'styled-components'

interface Thread {
  threadId: string
  title: string | null
  updatedAt: string
  messageCount: number
  contexts: { type: string; id: string }[]
}

export interface ChatHistorySidebarProps {
  currentThreadId: string
  onNewChat: () => void
  onSelectThread: (threadId: string) => void
  onRefreshRef?: (refreshFn: () => void) => void
  currentContext?: { type: string; id: string } | null
  currentMessageCount?: number
  /** Fetch threads from the backend. Consumer provides auth handling. */
  fetchThreads: () => Promise<Thread[]>
  /** Delete a thread by ID. */
  deleteThread?: (threadId: string) => Promise<void>
}

const SidebarContainer = styled.div`
  width: 320px;
  background: #f7f7f7;
  border-right: 1px solid #e0e0e0;
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
`

const SidebarHeader = styled.div`
  padding: 16px;
  border-bottom: 1px solid #e0e0e0;
`

const NewChatButton = styled.button`
  width: 100%;
  padding: 10px 16px;
  background: #0d79d0;
  color: white;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: background 0.2s;

  &:hover { background: #0a5fa3; }
`

const ThreadList = styled.div`
  flex: 1;
  overflow-y: auto;
  padding: 8px;
`

const ThreadItem = styled.div<{ isActive: boolean }>`
  padding: 12px;
  margin-bottom: 4px;
  border-radius: 6px;
  cursor: pointer;
  background: ${(props) => (props.isActive ? '#e3f2fd' : 'white')};
  border: 1px solid ${(props) => (props.isActive ? '#90caf9' : '#e0e0e0')};
  transition: all 0.15s;
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 8px;

  &:hover {
    background: ${(props) => (props.isActive ? '#e3f2fd' : '#f5f5f5')};
  }
`

const ThreadContent = styled.div`
  flex: 1;
  min-width: 0;
`

const DeleteButton = styled.button`
  padding: 4px 8px;
  background: transparent;
  border: 1px solid #e0e0e0;
  border-radius: 4px;
  color: #666;
  font-size: 11px;
  cursor: pointer;
  opacity: 0.6;

  &:hover {
    background: #fee;
    border-color: #d32f2f;
    color: #d32f2f;
    opacity: 1;
  }
`

const ThreadTitle = styled.div`
  font-size: 13px;
  font-weight: 500;
  color: #333;
  overflow: hidden;
  text-overflow: ellipsis;
  margin-bottom: 4px;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  line-height: 1.4;
`

const ThreadMeta = styled.div`
  font-size: 11px;
  color: #666;
  display: flex;
  gap: 8px;
`

const ContextList = styled.div`
  margin-top: 8px;
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
`

const ContextPill = styled.div`
  background: #eef;
  border: 1px solid #cce;
  color: #557;
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 500;
  white-space: nowrap;

  .context-type {
    font-weight: 600;
    margin-right: 4px;
  }
`

const LoadingState = styled.div`
  padding: 20px;
  text-align: center;
  color: #666;
  font-size: 13px;
`

const formatDate = (dateString: string) => {
  const date = new Date(dateString)
  const now = new Date()
  const diffMs = now.getTime() - date.getTime()
  const diffMins = Math.floor(diffMs / 60000)
  const diffHours = Math.floor(diffMs / 3600000)
  const diffDays = Math.floor(diffMs / 86400000)

  if (diffMins < 1) return 'Just now'
  if (diffMins < 60) return `${diffMins}m ago`
  if (diffHours < 24) return `${diffHours}h ago`
  if (diffDays < 7) return `${diffDays}d ago`
  return date.toLocaleDateString()
}

export function ChatHistorySidebar({
  currentThreadId,
  onNewChat,
  onSelectThread,
  onRefreshRef,
  currentContext,
  currentMessageCount = 0,
  fetchThreads: fetchThreadsFn,
  deleteThread: deleteThreadFn,
}: ChatHistorySidebarProps) {
  const [threads, setThreads] = useState<Thread[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const doFetch = useCallback(async (isInitialLoad = false) => {
    try {
      if (isInitialLoad) setLoading(true)
      const data = await fetchThreadsFn()
      const relevant = data.filter((t) => t.messageCount > 0 || t.threadId === currentThreadId)
      setThreads(relevant)
      setError(null)
    } catch (err: any) {
      setError(err.message)
    } finally {
      if (isInitialLoad) setLoading(false)
    }
  }, [fetchThreadsFn])

  useEffect(() => {
    if (onRefreshRef) onRefreshRef(() => doFetch(false))
  }, [onRefreshRef, doFetch])

  useEffect(() => {
    doFetch(true)
    const interval = setInterval(() => doFetch(false), 30000)
    return () => clearInterval(interval)
  }, [doFetch])

  useEffect(() => {
    if (currentThreadId) doFetch(false)
  }, [currentThreadId, doFetch])

  const getUniqueContexts = (contexts: { type: string; id: string }[] = []) => {
    if (!contexts) return []
    const unique: Record<string, { type: string; id: string }> = {}
    for (let i = contexts.length - 1; i >= 0; i--) {
      const key = `${contexts[i].type}:${contexts[i].id}`
      if (!unique[key]) unique[key] = contexts[i]
    }
    return Object.values(unique).reverse().slice(0, 3)
  }

  const displayThreads = useMemo(() => {
    const existing = threads.find(t => t.threadId === currentThreadId)
    if (!existing && currentThreadId && currentContext) {
      return [{
        threadId: currentThreadId,
        title: `Chat about ${currentContext.id}`,
        updatedAt: new Date().toISOString(),
        messageCount: currentMessageCount,
        contexts: [currentContext],
      }, ...threads]
    }
    if (existing && currentMessageCount > existing.messageCount) {
      return threads.map(t => t.threadId === currentThreadId ? { ...t, messageCount: currentMessageCount } : t)
    }
    return threads
  }, [threads, currentThreadId, currentContext, currentMessageCount])

  const handleDelete = async (threadId: string, e: React.MouseEvent) => {
    e.stopPropagation()
    if (!deleteThreadFn) return
    try {
      await deleteThreadFn(threadId)
      setThreads(threads.filter(t => t.threadId !== threadId))
      if (threadId === currentThreadId) onNewChat()
    } catch (err: any) {
      console.error('Failed to delete thread:', err)
    }
  }

  return (
    <SidebarContainer>
      <SidebarHeader>
        <NewChatButton onClick={onNewChat}>+ New Chat</NewChatButton>
      </SidebarHeader>
      <ThreadList>
        {loading && <LoadingState>Loading history...</LoadingState>}
        {error && <LoadingState>Error: {error}</LoadingState>}
        {!loading && !error && displayThreads.length === 0 && (
          <LoadingState>No chat history yet</LoadingState>
        )}
        {displayThreads.map((thread) => (
          <ThreadItem
            key={thread.threadId}
            isActive={thread.threadId === currentThreadId}
            onClick={() => onSelectThread(thread.threadId)}
          >
            <ThreadContent>
              <ThreadTitle>{thread.title || 'New conversation'}</ThreadTitle>
              <ThreadMeta>
                <span>{thread.messageCount} messages</span>
                <span>{formatDate(thread.updatedAt)}</span>
              </ThreadMeta>
              <ContextList>
                {getUniqueContexts(thread.contexts).map((ctx) => (
                  <ContextPill key={`${ctx.type}-${ctx.id}`} title={`${ctx.type}: ${ctx.id}`}>
                    <span className="context-type">{ctx.type}</span>
                    <span>{ctx.id}</span>
                  </ContextPill>
                ))}
              </ContextList>
            </ThreadContent>
            {deleteThreadFn && (
              <DeleteButton onClick={(e) => handleDelete(thread.threadId, e)} title="Delete conversation">
                Delete
              </DeleteButton>
            )}
          </ThreadItem>
        ))}
      </ThreadList>
    </SidebarContainer>
  )
}
