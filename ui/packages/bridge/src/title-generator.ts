import { GoogleGenerativeAI } from '@google/generative-ai'

const TITLING_MODEL = process.env.COPILOT_TITLING_MODEL || 'gemini-2.5-flash'

const cleanTitle = (title: string): string => {
  return title.replace(/["*]/g, '').trim()
}

interface MessageLike {
  role: string
  content: string | object
}

export const generateTitleForChat = async (messages: MessageLike[]): Promise<string | null> => {
  if (!process.env.GOOGLE_GENERATIVE_AI_API_KEY) {
    console.warn('GOOGLE_GENERATIVE_AI_API_KEY is not set, skipping title generation.')
    return null
  }

  try {
    const genAI = new GoogleGenerativeAI(process.env.GOOGLE_GENERATIVE_AI_API_KEY)
    const model = genAI.getGenerativeModel({ model: TITLING_MODEL })

    const history = messages
      .map((msg) => `${msg.role}: ${typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content)}`)
      .join('\n')

    const prompt = `Generate a very short, concise title (max 5 words) for the following conversation. The title should summarize the main topic.

IMPORTANT:
- If gene names (e.g., BRCA1, TP53) or variant IDs (e.g., 1-55516888-G-GA) are discussed, include the most frequently mentioned ones in the title.
- If multiple genes/variants are discussed, prioritize the most frequently mentioned.
- If no specific genes or variants can be identified, use "multiple" as appropriate.
- Do not use quotes or asterisks in the title.

Conversation:
---
${history}
---
Title:`

    const result = await model.generateContent(prompt)
    const response = result.response
    const text = response.text()

    if (!text) return null
    return cleanTitle(text)
  } catch (error: any) {
    console.error('Failed to generate title from AI model', error.message)
    return null
  }
}
