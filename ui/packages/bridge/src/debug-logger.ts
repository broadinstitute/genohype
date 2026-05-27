const DEBUG_TOKEN_COUNTING = process.env.DEBUG_TOKEN_COUNTING === 'true'

export const debugLog = (data: any) => {
  if (!DEBUG_TOKEN_COUNTING) return
  const timestamp = new Date().toISOString()
  console.debug(`[${timestamp}] token-debug:`, JSON.stringify(data, null, 2))
}

export interface ContentExtractionResult {
  content: string
  method: string
  parsedFromJson: boolean
}

export const extractTextContent = (result: any, messageId?: string): ContentExtractionResult => {
  let parsedResult = result
  let parsedFromJson = false

  if (typeof result === 'string') {
    try {
      parsedResult = JSON.parse(result)
      parsedFromJson = true
    } catch {
      return { content: result, method: 'plain-string', parsedFromJson: false }
    }
  }

  let content = ''
  let method = 'unknown'

  if (typeof parsedResult === 'string') {
    content = parsedResult
    method = 'plain-string'
  } else if (Array.isArray(parsedResult)) {
    content = parsedResult
      .filter((item: any) => item.type === 'text' && item.text)
      .map((item: any) => item.text)
      .join('\n')
    method = 'MCP-array'
  } else if (parsedResult?.content && Array.isArray(parsedResult.content)) {
    content = parsedResult.content
      .filter((item: any) => item.type === 'text' && item.text)
      .map((item: any) => item.text)
      .join('\n')
    method = 'content-array'
  } else if (parsedResult?.textContent) {
    content = Array.isArray(parsedResult.textContent)
      ? parsedResult.textContent.map((item: any) => item.text || '').join('\n')
      : String(parsedResult.textContent)
    method = 'textContent'
  } else {
    content = '[Tool result with no text representation]'
    method = 'fallback-no-text'
    if (messageId) {
      console.warn('Tool result has no extractable text content', { messageId, resultKeys: Object.keys(parsedResult || {}) })
    }
  }

  return { content, method, parsedFromJson }
}

export const isDebugEnabled = () => DEBUG_TOKEN_COUNTING
