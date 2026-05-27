import { useEffect, useState } from 'react'
import { useCopilotMessagesContext } from '@copilotkit/react-core'
import {
  TextMessage,
  ActionExecutionMessage,
  ResultMessage,
} from '@copilotkit/runtime-client-gql'
import { useAssistantContext } from '../providers/AssistantProvider'

/**
 * Loads chat history from the persistence API when the thread changes.
 * Clears messages for new threads, loads existing messages for saved threads.
 */
export function useThreadLoader() {
  const { threadId, threadVersion, runtimeUrl, getAuthToken } = useAssistantContext()
  const { setMessages } = useCopilotMessagesContext()
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    let cancelled = false

    const fetchMessages = async () => {
      setIsLoading(true)
      try {
        const headers: HeadersInit = {}
        if (getAuthToken) {
          try {
            const token = await getAuthToken()
            headers.Authorization = `Bearer ${token}`
          } catch { /* continue without auth */ }
        }

        const response = await fetch(`${runtimeUrl}/threads/${threadId}/messages`, { headers })

        if (cancelled) return

        if (!response.ok) {
          setMessages([])
          return
        }

        const data = await response.json()

        if (cancelled) return

        if (!data || data.length === 0) {
          setMessages([])
          return
        }

        const formattedMessages = data
          .map((msg: any) => {
            const rawMsg = msg.rawMessage
            if (!rawMsg?.type) return null

            try {
              switch (rawMsg.type) {
                case 'TextMessage':
                  return new TextMessage(rawMsg)
                case 'ActionExecutionMessage': {
                  const actionData = { ...rawMsg }
                  if (actionData.arguments && typeof actionData.arguments !== 'string') {
                    actionData.arguments = JSON.stringify(actionData.arguments)
                  }
                  return new ActionExecutionMessage(actionData)
                }
                case 'ResultMessage': {
                  const resultData = { ...rawMsg }
                  if (resultData.result && typeof resultData.result !== 'string') {
                    resultData.result = JSON.stringify(resultData.result)
                  }
                  return new ResultMessage(resultData)
                }
                default:
                  return null
              }
            } catch {
              return null
            }
          })
          .filter(Boolean)

        if (!cancelled) {
          setMessages(formattedMessages)
        }
      } catch (error) {
        console.error('[useThreadLoader] Failed to load thread:', error)
        if (!cancelled) setMessages([])
      } finally {
        if (!cancelled) setIsLoading(false)
      }
    }

    fetchMessages()

    return () => { cancelled = true }
  }, [threadVersion]) // eslint-disable-line react-hooks/exhaustive-deps

  return { isLoading }
}
