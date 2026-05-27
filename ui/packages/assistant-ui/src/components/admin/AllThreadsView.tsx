import React, { useEffect, useState } from 'react'
import styled from 'styled-components'
import { useAdminFetch } from './useAdminFetch'

const Container = styled.div`padding: 20px; overflow-y: auto; height: 100%;`
const Table = styled.table`width: 100%; border-collapse: collapse; font-size: 14px;`
const Th = styled.th`text-align: left; padding: 12px; background: #f5f5f5; border-bottom: 2px solid #e0e0e0; font-weight: 600; color: #333;`
const Td = styled.td`padding: 12px; border-bottom: 1px solid #e0e0e0; vertical-align: top;`
const Tr = styled.tr`cursor: pointer; &:hover { background: #f9f9f9; }`
const StatusMessage = styled.div<{ color?: string }>`text-align: center; padding: 40px; color: ${p => p.color || '#666'};`
const Pagination = styled.div`display: flex; justify-content: space-between; align-items: center; margin-top: 1em; padding: 10px 0;`
const PageButton = styled.button`padding: 8px 16px; border: 1px solid #e0e0e0; border-radius: 4px; background: white; cursor: pointer; &:hover { background: #f7f7f7; } &:disabled { opacity: 0.5; cursor: not-allowed; }`
const ThreadLink = styled.div`color: #0d79d0; cursor: pointer; text-align: left; &:hover { text-decoration: underline; }`

const MessagesContainer = styled.div`
  margin-top: 20px; padding: 20px; background: #f9f9f9;
  border: 1px solid #e0e0e0; border-radius: 6px; max-height: 600px; overflow-y: auto;
`

const Message = styled.div<{ role: string }>`
  margin-bottom: 16px; padding: 12px;
  background: ${props => props.role === 'user' ? '#e3f2fd' : '#fff'};
  border: 1px solid ${props => props.role === 'user' ? '#90caf9' : '#e0e0e0'};
  border-radius: 6px;
`

const MessageRole = styled.div`
  font-weight: 600; font-size: 12px; text-transform: uppercase; color: #666;
  margin-bottom: 6px; display: flex; align-items: center; gap: 8px;
`

const MessageContent = styled.div<{ isCollapsed?: boolean }>`
  font-size: 14px; color: #333; white-space: pre-wrap; word-break: break-word;
  max-height: ${props => props.isCollapsed ? '60px' : 'none'};
  overflow: ${props => props.isCollapsed ? 'hidden' : 'visible'};
  position: relative;
`

const TokenBadge = styled.span<{ type: 'input' | 'output' }>`
  background: ${props => props.type === 'input' ? '#e3f2fd' : '#f3e5f5'};
  color: ${props => props.type === 'input' ? '#1976d2' : '#7b1fa2'};
  padding: 2px 8px; border-radius: 3px; font-weight: 500; font-size: 11px;
`

const MessageTokens = styled.div`
  font-size: 11px; color: #666; margin-top: 6px; padding-top: 6px;
  border-top: 1px solid #e0e0e0; display: flex; gap: 12px;
`

const ThreadStats = styled.div`
  background: #f5f5f5; border: 1px solid #e0e0e0; border-radius: 6px;
  padding: 16px; margin-bottom: 16px;
  display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px;
`

const StatItem = styled.div`display: flex; flex-direction: column;`
const StatLabel = styled.div`font-size: 12px; color: #666; text-transform: uppercase; font-weight: 600; margin-bottom: 4px;`
const StatValue = styled.div`font-size: 20px; color: #333; font-weight: 600;`
const BackButton = styled(PageButton)`margin-bottom: 12px;`
const DeleteButton = styled.button`
  background: #d32f2f; color: white; border: none; padding: 6px 12px;
  border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: 500;
  &:hover { background: #b71c1c; } &:disabled { background: #ccc; cursor: not-allowed; }
`
const CollapseButton = styled.button`
  background: none; border: none; color: #0d79d0; cursor: pointer;
  font-size: 11px; padding: 2px 6px; border-radius: 3px;
  &:hover { background: #e3f2fd; }
`

const MODEL_PRICING: Record<string, { input: number; output: number }> = {
  'gemini-3.1-flash': { input: 0.30, output: 2.50 },
  'gemini-3.1-pro': { input: 2.00, output: 12.00 },
  'gemini-2.5-flash': { input: 0.30, output: 2.50 },
  'gemini-2.5-pro': { input: 1.25, output: 10.00 },
  'gemini-2.0-flash': { input: 0.10, output: 0.40 },
}

const calculateCost = (inputTokens: number, outputTokens: number, model?: string): number => {
  const pricing = model && MODEL_PRICING[model] ? MODEL_PRICING[model] : MODEL_PRICING['gemini-2.5-flash']
  return (inputTokens / 1_000_000) * pricing.input + (outputTokens / 1_000_000) * pricing.output
}

const formatUserDisplay = (item: any): string => {
  if (item.userEmail) return item.userEmail
  if (item.userName) return item.userName
  const userId = item.userId
  if (!userId) return 'anonymous'
  if (userId.includes('|')) {
    const [provider, id] = userId.split('|')
    return `${provider.replace('oauth2', '').replace('-', '')}|${id.substring(0, 8)}...`
  }
  return userId.length > 16 ? userId.substring(0, 16) + '...' : userId
}

export const AllThreadsView = () => {
  const [threads, setThreads] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [page, setPage] = useState(0)
  const [selectedThreadId, setSelectedThreadId] = useState<string | null>(null)
  const [messages, setMessages] = useState<any[]>([])
  const [loadingMessages, setLoadingMessages] = useState(false)
  const [deletingThreadId, setDeletingThreadId] = useState<string | null>(null)
  const [collapsedMessages, setCollapsedMessages] = useState<Set<number>>(new Set())
  const limit = 20
  const { fetchWithAuth } = useAdminFetch()

  useEffect(() => {
    const fetchThreads = async () => {
      setLoading(true)
      try {
        const response = await fetchWithAuth(`/admin/threads?limit=${limit}&offset=${page * limit}`)
        if (!response.ok) throw new Error('Failed to fetch threads')
        setThreads(await response.json())
        setError(null)
      } catch (err: any) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchThreads()
  }, [page, fetchWithAuth])

  const handleThreadClick = async (threadId: string) => {
    setSelectedThreadId(threadId)
    setLoadingMessages(true)
    try {
      const response = await fetchWithAuth(`/admin/threads/${threadId}/messages`)
      if (!response.ok) {
        if (response.status === 403) throw new Error('Access denied by user privacy settings.')
        throw new Error('Failed to fetch messages')
      }
      setMessages(await response.json())
    } catch (err: any) {
      console.error(err.message)
    } finally {
      setLoadingMessages(false)
    }
  }

  const handleBackToList = () => {
    setSelectedThreadId(null)
    setMessages([])
    setCollapsedMessages(new Set())
  }

  useEffect(() => {
    if (messages.length > 0) {
      setCollapsedMessages(new Set(
        messages.map((msg, idx) => msg.role === 'system' ? idx : -1).filter(idx => idx !== -1)
      ))
    }
  }, [messages.length])

  const handleDeleteThread = async (threadId: string, event: React.MouseEvent) => {
    event.stopPropagation()
    setDeletingThreadId(threadId)
    try {
      const response = await fetchWithAuth(`/admin/threads/${threadId}`, { method: 'DELETE' })
      if (!response.ok) throw new Error('Failed to delete thread')
      setThreads(threads.filter(t => t.threadId !== threadId))
    } catch (err: any) {
      console.error('Failed to delete thread:', err.message)
    } finally {
      setDeletingThreadId(null)
    }
  }

  // Thread detail view
  if (selectedThreadId && messages.length > 0) {
    const currentThread = threads.find(t => t.threadId === selectedThreadId)
    const totalRequestTokens = parseInt(currentThread?.totalRequestTokens) || 0
    const totalOutputTokens = parseInt(currentThread?.totalOutputTokens) || 0
    const totalTokens = totalRequestTokens + totalOutputTokens
    const estimatedCost = calculateCost(totalRequestTokens, totalOutputTokens, currentThread?.model)

    return (
      <Container>
        <BackButton onClick={handleBackToList}>&larr; Back to Threads List</BackButton>
        <ThreadStats>
          <StatItem><StatLabel>Messages</StatLabel><StatValue>{messages.length}</StatValue></StatItem>
          <StatItem><StatLabel>Request Tokens</StatLabel><StatValue style={{ color: '#1976d2' }}>{totalRequestTokens.toLocaleString()}</StatValue></StatItem>
          <StatItem><StatLabel>Output Tokens</StatLabel><StatValue>{totalOutputTokens.toLocaleString()}</StatValue></StatItem>
          <StatItem><StatLabel>Total Tokens</StatLabel><StatValue style={{ fontWeight: '700' }}>{totalTokens.toLocaleString()}</StatValue></StatItem>
          <StatItem><StatLabel>Est. Cost</StatLabel><StatValue style={{ fontSize: '16px', color: '#2e7d32' }}>${estimatedCost.toFixed(4)}</StatValue></StatItem>
          <StatItem><StatLabel>Model</StatLabel><StatValue style={{ fontSize: '16px' }}>{currentThread?.model || 'N/A'}</StatValue></StatItem>
        </ThreadStats>
        <MessagesContainer>
          {loadingMessages ? <StatusMessage>Loading messages...</StatusMessage> : (
            messages.map((msg, idx) => {
              const isCollapsed = collapsedMessages.has(idx)
              const isSystem = msg.role === 'system'
              const contentLength = (msg.content || '').length
              let displayContent = msg.content
              if (!displayContent) {
                if (msg.messageType === 'ActionExecutionMessage') displayContent = '[Tool call]'
                else if (msg.messageType === 'ResultMessage') displayContent = '[Tool result]'
                else displayContent = `[${msg.messageType || 'No content'}]`
              }
              const displayRole = msg.role || (msg.messageType === 'ActionExecutionMessage' || msg.messageType === 'ResultMessage' ? 'assistant' : 'unknown')

              return (
                <Message key={idx} role={displayRole}>
                  <MessageRole>
                    {displayRole}
                    {isSystem && contentLength > 200 && (
                      <CollapseButton onClick={() => {
                        setCollapsedMessages(prev => {
                          const s = new Set(prev)
                          s.has(idx) ? s.delete(idx) : s.add(idx)
                          return s
                        })
                      }}>
                        {isCollapsed ? '+ Expand' : '- Collapse'}
                      </CollapseButton>
                    )}
                  </MessageRole>
                  <MessageContent isCollapsed={isSystem && isCollapsed && contentLength > 200}>{displayContent}</MessageContent>
                  {(msg.inputTokens > 0 || msg.outputTokens > 0) && (
                    <MessageTokens>
                      {msg.inputTokens > 0 && <TokenBadge type="input">&darr; {msg.inputTokens.toLocaleString()} in</TokenBadge>}
                      {msg.outputTokens > 0 && <TokenBadge type="output">&uarr; {msg.outputTokens.toLocaleString()} out</TokenBadge>}
                    </MessageTokens>
                  )}
                </Message>
              )
            })
          )}
        </MessagesContainer>
      </Container>
    )
  }

  if (loading) return <StatusMessage>Loading threads...</StatusMessage>
  if (error) return <StatusMessage color="#d32f2f">Error: {error}</StatusMessage>
  if (threads.length === 0) return <StatusMessage>No threads available.</StatusMessage>

  return (
    <Container>
      <Table>
        <thead>
          <tr><Th>Title</Th><Th>User</Th><Th>Messages</Th><Th>Request Tokens</Th><Th>Total Tokens</Th><Th>Est. Cost</Th><Th>Model</Th><Th>Last Updated</Th><Th>Actions</Th></tr>
        </thead>
        <tbody>
          {threads.map((thread) => {
            const reqTokens = parseInt(thread.totalRequestTokens) || 0
            const outTokens = parseInt(thread.totalOutputTokens) || 0
            const totalTokens = reqTokens + outTokens
            const cost = calculateCost(reqTokens, outTokens, thread.model)
            return (
              <Tr key={thread.threadId} onClick={() => handleThreadClick(thread.threadId)}>
                <Td><ThreadLink>{thread.title || thread.threadId.substring(0, 16)}</ThreadLink></Td>
                <Td style={{ fontSize: '13px' }} title={thread.userId || 'anonymous'}>{formatUserDisplay(thread)}</Td>
                <Td>{thread.messageCount}</Td>
                <Td style={{ fontSize: '13px', color: '#1976d2', fontWeight: 500 }}>{reqTokens.toLocaleString()}</Td>
                <Td style={{ fontSize: '13px', fontWeight: 500 }}>{totalTokens.toLocaleString()}</Td>
                <Td style={{ fontSize: '13px', color: '#2e7d32', fontWeight: 500 }}>${cost.toFixed(4)}</Td>
                <Td style={{ fontSize: '13px' }}>{thread.model || 'N/A'}</Td>
                <Td style={{ fontSize: '13px' }}>{new Date(thread.updatedAt).toLocaleString()}</Td>
                <Td>
                  <DeleteButton onClick={(e) => handleDeleteThread(thread.threadId, e)} disabled={deletingThreadId === thread.threadId}>
                    {deletingThreadId === thread.threadId ? 'Deleting...' : 'Delete'}
                  </DeleteButton>
                </Td>
              </Tr>
            )
          })}
        </tbody>
      </Table>
      <Pagination>
        <PageButton onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}>Previous</PageButton>
        <span>Page {page + 1}</span>
        <PageButton onClick={() => setPage(p => p + 1)} disabled={threads.length < limit}>Next</PageButton>
      </Pagination>
    </Container>
  )
}
