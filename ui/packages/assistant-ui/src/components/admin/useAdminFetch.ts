import { useCallback } from 'react'
import { useAssistantContext } from '../../providers/AssistantProvider'

export function useAdminFetch() {
  const { runtimeUrl, getAuthToken } = useAssistantContext()

  const fetchWithAuth = useCallback(async (path: string, options?: RequestInit): Promise<Response> => {
    const headers: Record<string, string> = {
      ...(options?.headers as Record<string, string> || {}),
    }

    if (getAuthToken) {
      try {
        const token = await getAuthToken()
        headers.Authorization = `Bearer ${token}`
      } catch {
        // Continue without auth
      }
    }

    return fetch(`${runtimeUrl}${path}`, { ...options, headers })
  }, [runtimeUrl, getAuthToken])

  return { fetchWithAuth, runtimeUrl }
}
