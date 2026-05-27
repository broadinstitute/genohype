import React, { useEffect, useState } from 'react'
import styled from 'styled-components'
import { useAdminFetch } from './useAdminFetch'

const Container = styled.div`padding: 20px; overflow-y: auto; height: 100%;`
const Table = styled.table`width: 100%; border-collapse: collapse; font-size: 14px;`
const Th = styled.th`text-align: left; padding: 12px; background: #f5f5f5; border-bottom: 2px solid #e0e0e0; font-weight: 600; color: #333;`
const Td = styled.td`padding: 12px; border-bottom: 1px solid #e0e0e0; vertical-align: top;`
const StatusMessage = styled.div<{ color?: string }>`text-align: center; padding: 40px; color: ${p => p.color || '#666'};`
const ClickCount = styled.div`font-weight: 600; color: #0d79d0;`

export const InteractionAnalysisView = () => {
  const [stats, setStats] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const { fetchWithAuth } = useAdminFetch()

  useEffect(() => {
    const fetchStats = async () => {
      setLoading(true)
      try {
        const response = await fetchWithAuth('/admin/stats/suggestions')
        if (!response.ok) throw new Error('Failed to fetch suggestion stats')
        setStats(await response.json())
        setError(null)
      } catch (err: any) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    fetchStats()
  }, [fetchWithAuth])

  if (loading) return <StatusMessage>Loading interaction data...</StatusMessage>
  if (error) return <StatusMessage color="#d32f2f">Error: {error}</StatusMessage>
  if (stats.length === 0) return <StatusMessage>No interaction data available yet.</StatusMessage>

  return (
    <Container>
      <p style={{ fontSize: '14px', color: '#666', marginBottom: '20px' }}>
        Most frequently clicked suggestion pills, helping understand which features users find most useful.
      </p>
      <Table>
        <thead><tr><Th>Rank</Th><Th>Suggestion Title</Th><Th>Suggestion Message</Th><Th>Click Count</Th></tr></thead>
        <tbody>
          {stats.map((stat, idx) => (
            <tr key={idx}>
              <Td>{idx + 1}</Td>
              <Td style={{ fontWeight: 500 }}>{stat.suggestion_title || 'N/A'}</Td>
              <Td style={{ maxWidth: '400px' }}>{stat.suggestion_message || 'N/A'}</Td>
              <Td><ClickCount>{stat.click_count}</ClickCount></Td>
            </tr>
          ))}
        </tbody>
      </Table>
    </Container>
  )
}
