import React, { useEffect, useState } from 'react'
import styled from 'styled-components'
import { useAdminFetch } from './useAdminFetch'

const Container = styled.div`padding: 20px; overflow-y: auto; height: 100%;`
const StatsGrid = styled.div`display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 16px; margin-bottom: 24px;`
const StatCard = styled.div`background: white; border: 1px solid #e0e0e0; border-radius: 6px; padding: 16px;`
const StatLabel = styled.div`font-size: 12px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;`
const StatValue = styled.div`font-size: 24px; font-weight: 600; color: #333;`
const Table = styled.table`width: 100%; border-collapse: collapse; font-size: 14px; margin-top: 16px;`
const Th = styled.th`text-align: left; padding: 12px; background: #f5f5f5; border-bottom: 2px solid #e0e0e0; font-weight: 600; color: #333;`
const Td = styled.td`padding: 12px; border-bottom: 1px solid #e0e0e0;`
const StatusMessage = styled.div<{ color?: string }>`text-align: center; padding: 40px; color: ${p => p.color || '#666'};`
const Section = styled.div`margin-bottom: 32px;`
const SectionTitle = styled.h3`font-size: 1em; margin-bottom: 12px; color: #333;`

const MODEL_PRICING: Record<string, { input: number; output: number }> = {
  'gemini-3.1-flash': { input: 0.30, output: 2.50 },
  'gemini-3.1-pro': { input: 2.00, output: 12.00 },
  'gemini-2.5-flash': { input: 0.30, output: 2.50 },
  'gemini-2.5-pro': { input: 1.25, output: 10.00 },
  'gemini-2.0-flash': { input: 0.10, output: 0.40 },
}

const formatNumber = (num: number | null | undefined): string => {
  if (num === null || num === undefined) return '0'
  return num.toLocaleString()
}

const calculateCost = (requestTokens: number, outputTokens: number, model: string): number => {
  const pricing = MODEL_PRICING[model] || MODEL_PRICING['gemini-2.5-flash']
  return (requestTokens / 1_000_000) * pricing.input + (outputTokens / 1_000_000) * pricing.output
}

export const UsageStatsView = () => {
  const [stats, setStats] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const { fetchWithAuth } = useAdminFetch()

  useEffect(() => {
    const fetchStats = async () => {
      setLoading(true)
      try {
        const response = await fetchWithAuth('/admin/stats')
        if (!response.ok) throw new Error('Failed to fetch stats')
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

  if (loading) return <StatusMessage>Loading statistics...</StatusMessage>
  if (error) return <StatusMessage color="#d32f2f">Error: {error}</StatusMessage>

  const totals = stats.reduce(
    (acc, stat) => ({
      threads: acc.threads + parseInt(stat.total_threads || 0),
      users: acc.users + parseInt(stat.total_users || 0),
      messages: acc.messages + parseInt(stat.total_messages || 0),
      requestTokens: acc.requestTokens + parseInt(stat.total_request_tokens || 0),
      outputTokens: acc.outputTokens + parseInt(stat.total_output_tokens || 0),
    }),
    { threads: 0, users: 0, messages: 0, requestTokens: 0, outputTokens: 0 }
  )

  return (
    <Container>
      <Section>
        <SectionTitle>Overall Usage</SectionTitle>
        <StatsGrid>
          <StatCard><StatLabel>Total Threads</StatLabel><StatValue>{formatNumber(totals.threads)}</StatValue></StatCard>
          <StatCard><StatLabel>Total Users</StatLabel><StatValue>{formatNumber(totals.users)}</StatValue></StatCard>
          <StatCard><StatLabel>Total Messages</StatLabel><StatValue>{formatNumber(totals.messages)}</StatValue></StatCard>
          <StatCard><StatLabel>Total Request Tokens</StatLabel><StatValue>{formatNumber(totals.requestTokens)}</StatValue></StatCard>
          <StatCard><StatLabel>Output Tokens</StatLabel><StatValue>{formatNumber(totals.outputTokens)}</StatValue></StatCard>
        </StatsGrid>
      </Section>

      <Section>
        <SectionTitle>Usage by Model</SectionTitle>
        <Table>
          <thead><tr><Th>Model</Th><Th>Threads</Th><Th>Messages</Th><Th>Request Tokens</Th><Th>Output Tokens</Th><Th>Est. Cost (USD)</Th></tr></thead>
          <tbody>
            {stats.map((stat, idx) => {
              const reqTokens = parseInt(stat.total_request_tokens || 0)
              const outTokens = parseInt(stat.total_output_tokens || 0)
              return (
                <tr key={idx}>
                  <Td>{stat.model || 'Unknown'}</Td>
                  <Td>{formatNumber(stat.total_threads)}</Td>
                  <Td>{formatNumber(stat.total_messages)}</Td>
                  <Td>{formatNumber(reqTokens)}</Td>
                  <Td>{formatNumber(outTokens)}</Td>
                  <Td>${calculateCost(reqTokens, outTokens, stat.model || 'gemini-2.5-flash').toFixed(4)}</Td>
                </tr>
              )
            })}
          </tbody>
        </Table>
      </Section>

      <Section>
        <SectionTitle>Token Breakdown by Model</SectionTitle>
        <Table>
          <thead><tr><Th>Model</Th><Th>System Prompt</Th><Th>Tool Definitions</Th><Th>History</Th><Th>User Message</Th></tr></thead>
          <tbody>
            {stats.map((stat, idx) => (
              <tr key={idx}>
                <Td>{stat.model || 'Unknown'}</Td>
                <Td>{formatNumber(stat.total_system_prompt_tokens)}</Td>
                <Td>{formatNumber(stat.total_tool_definition_tokens)}</Td>
                <Td>{formatNumber(stat.total_history_tokens)}</Td>
                <Td>{formatNumber(stat.total_user_message_tokens)}</Td>
              </tr>
            ))}
          </tbody>
        </Table>
      </Section>
    </Container>
  )
}
