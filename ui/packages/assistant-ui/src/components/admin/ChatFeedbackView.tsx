import React, { useEffect, useState } from 'react'
import styled from 'styled-components'
import { useAdminFetch } from './useAdminFetch'

const Container = styled.div`padding: 20px; overflow-y: auto; height: 100%;`
const Table = styled.table`width: 100%; border-collapse: collapse; font-size: 14px;`
const Th = styled.th`text-align: left; padding: 12px; background: #f5f5f5; border-bottom: 2px solid #e0e0e0; font-weight: 600; color: #333;`
const Td = styled.td`padding: 12px; border-bottom: 1px solid #e0e0e0; vertical-align: top;`
const Tr = styled.tr`&:hover { background: #f9f9f9; }`
const StatusMessage = styled.div<{ color?: string }>`text-align: center; padding: 40px; color: ${p => p.color || '#666'};`
const Pagination = styled.div`display: flex; justify-content: space-between; align-items: center; margin-top: 1em; padding: 10px 0;`
const PageButton = styled.button`padding: 8px 16px; border: 1px solid #e0e0e0; border-radius: 4px; background: white; cursor: pointer; &:hover { background: #f7f7f7; } &:disabled { opacity: 0.5; cursor: not-allowed; }`

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

export const ChatFeedbackView = () => {
  const [feedback, setFeedback] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [page, setPage] = useState(0)
  const limit = 20
  const { fetchWithAuth } = useAdminFetch()

  useEffect(() => {
    const fetchFeedback = async () => {
      setLoading(true)
      try {
        const response = await fetchWithAuth(`/feedback?limit=${limit}&offset=${page * limit}`)
        if (!response.ok) throw new Error('Failed to fetch feedback')
        setFeedback(await response.json())
        setError(null)
      } catch (err: any) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchFeedback()
  }, [page, fetchWithAuth])

  if (loading) return <StatusMessage>Loading feedback...</StatusMessage>
  if (error) return <StatusMessage color="#d32f2f">Error: {error}</StatusMessage>
  if (feedback.length === 0) return <StatusMessage>No feedback submitted yet.</StatusMessage>

  return (
    <Container>
      <Table>
        <thead>
          <tr><Th>Date</Th><Th>Source</Th><Th>Rating</Th><Th>Feedback</Th><Th>Thread</Th><Th>User</Th></tr>
        </thead>
        <tbody>
          {feedback.map((item) => (
            <Tr key={item.id}>
              <Td>{new Date(item.createdAt).toLocaleString()}</Td>
              <Td>{item.source}</Td>
              <Td>{item.rating === 1 ? '+1' : item.rating === -1 ? '-1' : 'N/A'}</Td>
              <Td style={{ maxWidth: '300px', wordBreak: 'break-word' }}>{item.feedbackText || '-'}</Td>
              <Td>{item.threadTitle || (item.threadId ? item.threadId.substring(0, 8) : 'N/A')}</Td>
              <Td style={{ fontSize: '13px' }} title={item.userId || 'anonymous'}>{formatUserDisplay(item)}</Td>
            </Tr>
          ))}
        </tbody>
      </Table>
      <Pagination>
        <PageButton onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}>Previous</PageButton>
        <span>Page {page + 1}</span>
        <PageButton onClick={() => setPage(p => p + 1)} disabled={feedback.length < limit}>Next</PageButton>
      </Pagination>
    </Container>
  )
}
