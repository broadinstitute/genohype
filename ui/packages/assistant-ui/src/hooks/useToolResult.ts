import { useState, useEffect } from 'react'
import { useAssistantContext } from '../providers/AssistantProvider'

export const useToolResult = (result: any) => {
  const [data, setData] = useState<any>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<Error | null>(null)
  const { runtimeUrl, getAuthToken } = useAssistantContext()

  useEffect(() => {
    const resolveResult = async () => {
      if (!result) {
        setData(null)
        return
      }

      const structuredContent = result.structuredContent || result

      if (structuredContent?.toolResultId) {
        setIsLoading(true)
        setError(null)
        try {
          const headers: HeadersInit = {}
          if (getAuthToken) {
            try {
              const token = await getAuthToken()
              headers.Authorization = `Bearer ${token}`
            } catch {
              // Continue without auth
            }
          }
          const response = await fetch(`${runtimeUrl}/tool_results/${structuredContent.toolResultId}`, { headers })
          if (!response.ok) {
            throw new Error(`Failed to fetch tool result: ${response.statusText}`)
          }
          const fetchedData = await response.json()
          setData(fetchedData)
        } catch (e: any) {
          setError(e)
        } finally {
          setIsLoading(false)
        }
      } else {
        setData(structuredContent)
        setIsLoading(false)
        setError(null)
      }
    }

    resolveResult()
  }, [result, runtimeUrl, getAuthToken])

  return { data, isLoading, error }
}
