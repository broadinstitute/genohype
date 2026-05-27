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
const UserIdText = styled.span`font-family: monospace; font-size: 12px; color: #666;`

const formatUserId = (userId: string): string => {
  if (userId.includes('|')) {
    const [provider, id] = userId.split('|')
    return `${provider.replace('oauth2', '').replace('-', '')}|${id.substring(0, 8)}...`
  }
  return userId.length > 20 ? userId.substring(0, 20) + '...' : userId
}

const formatRelativeTime = (dateString: string): string => {
  const diffMs = Date.now() - new Date(dateString).getTime()
  const diffMins = Math.floor(diffMs / 60000)
  const diffHours = Math.floor(diffMs / 3600000)
  const diffDays = Math.floor(diffMs / 86400000)
  if (diffMins < 1) return 'Just now'
  if (diffMins < 60) return `${diffMins} min${diffMins > 1 ? 's' : ''} ago`
  if (diffHours < 24) return `${diffHours} hour${diffHours > 1 ? 's' : ''} ago`
  if (diffDays < 7) return `${diffDays} day${diffDays > 1 ? 's' : ''} ago`
  return new Date(dateString).toLocaleDateString()
}

export const UsersView = () => {
  const [users, setUsers] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [page, setPage] = useState(0)
  const limit = 20
  const { fetchWithAuth } = useAdminFetch()

  useEffect(() => {
    const fetchUsers = async () => {
      setLoading(true)
      try {
        const response = await fetchWithAuth(`/users?limit=${limit}&offset=${page * limit}`)
        if (!response.ok) throw new Error('Failed to fetch users')
        setUsers(await response.json())
        setError(null)
      } catch (err: any) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchUsers()
  }, [page, fetchWithAuth])

  if (loading) return <StatusMessage>Loading users...</StatusMessage>
  if (error) return <StatusMessage color="#d32f2f">Error: {error}</StatusMessage>
  if (users.length === 0) return <StatusMessage>No users found.</StatusMessage>

  return (
    <Container>
      <Table>
        <thead>
          <tr><Th>Email</Th><Th>Name</Th><Th>User ID</Th><Th>First Seen</Th><Th>Last Seen</Th></tr>
        </thead>
        <tbody>
          {users.map((user) => (
            <Tr key={user.userId}>
              <Td style={{ fontWeight: 500 }}>{user.email || <span style={{ color: '#999', fontStyle: 'italic' }}>No email</span>}</Td>
              <Td>{user.name || '-'}</Td>
              <Td><UserIdText title={user.userId}>{formatUserId(user.userId)}</UserIdText></Td>
              <Td style={{ fontSize: '13px', color: '#666' }}>{new Date(user.createdAt).toLocaleDateString()}</Td>
              <Td style={{ fontSize: '13px', color: '#666' }}>{formatRelativeTime(user.lastSeenAt)}</Td>
            </Tr>
          ))}
        </tbody>
      </Table>
      <Pagination>
        <PageButton onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}>Previous</PageButton>
        <span>Page {page + 1}</span>
        <PageButton onClick={() => setPage(p => p + 1)} disabled={users.length < limit}>Next</PageButton>
      </Pagination>
    </Container>
  )
}
