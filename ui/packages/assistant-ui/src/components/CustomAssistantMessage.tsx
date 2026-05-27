import React, { useState, useEffect, useRef } from 'react'
import { AssistantMessage, AssistantMessageProps } from '@copilotkit/react-ui'
import styled from 'styled-components'
import { ChatModal } from './ChatModal'
import { useAssistantContext } from '../providers/AssistantProvider'

const TextArea = styled.textarea`
  width: 100%;
  min-height: 100px;
  padding: 8px 12px 8px 8px;
  border: 1px solid #e0e0e0;
  border-radius: 4px;
  font-family: inherit;
  font-size: 14px;
  resize: vertical;
  box-sizing: border-box;

  &:focus {
    outline: none;
    border-color: #0d79d0;
  }
`

const MessageWrapper = styled.div`
  position: relative;
`

const StyledButton = styled.button`
  padding: 8px 16px;
  border: 1px solid #e0e0e0;
  border-radius: 4px;
  background: white;
  cursor: pointer;
  font-size: 14px;

  &:hover {
    background: #f7f7f7;
  }

  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`

const PrimaryStyledButton = styled(StyledButton)`
  background: #0d79d0;
  color: white;
  border-color: #0d79d0;

  &:hover {
    background: #0b6ab8;
  }
`

interface CustomAssistantMessageProps extends AssistantMessageProps {
  threadId?: string
}

export const CustomAssistantMessage: React.FC<CustomAssistantMessageProps> = (props) => {
  const [isFeedbackModalOpen, setIsFeedbackModalOpen] = useState(false)
  const [feedbackText, setFeedbackText] = useState('')
  const [isSubmitting, setIsSubmitting] = useState(false)
  const { runtimeUrl, getAuthToken } = useAssistantContext()
  const wrapperRef = useRef<HTMLDivElement>(null)

  const handleFeedbackSubmit = async () => {
    if (!feedbackText.trim()) return

    setIsSubmitting(true)
    try {
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
      }

      if (getAuthToken) {
        try {
          const token = await getAuthToken()
          headers.Authorization = `Bearer ${token}`
        } catch {
          // Submit as anonymous
        }
      }

      await fetch(`${runtimeUrl}/feedback`, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          messageId: props.message?.id,
          threadId: props.threadId,
          source: 'message',
          feedbackText,
        }),
      })
      setIsFeedbackModalOpen(false)
      setFeedbackText('')
    } catch (error) {
      console.error('Failed to submit feedback:', error)
    } finally {
      setIsSubmitting(false)
    }
  }

  useEffect(() => {
    if (!wrapperRef.current) return

    const thumbsUpButton = wrapperRef.current.querySelector('button[aria-label="Thumbs up"]')
    const thumbsDownButton = wrapperRef.current.querySelector('button[aria-label="Thumbs down"]')

    const handleThumbsClick = () => {
      setTimeout(() => {
        setIsFeedbackModalOpen(true)
      }, 0)
    }

    if (thumbsUpButton) {
      thumbsUpButton.addEventListener('click', handleThumbsClick)
    }
    if (thumbsDownButton) {
      thumbsDownButton.addEventListener('click', handleThumbsClick)
    }

    return () => {
      if (thumbsUpButton) {
        thumbsUpButton.removeEventListener('click', handleThumbsClick)
      }
      if (thumbsDownButton) {
        thumbsDownButton.removeEventListener('click', handleThumbsClick)
      }
    }
  }, [props.message?.id])

  return (
    <MessageWrapper ref={wrapperRef}>
      <AssistantMessage {...props} />

      {isFeedbackModalOpen && (
        <ChatModal
          title="Provide Feedback"
          onRequestClose={() => setIsFeedbackModalOpen(false)}
          footer={
            <>
              <StyledButton onClick={() => setIsFeedbackModalOpen(false)} disabled={isSubmitting}>
                Cancel
              </StyledButton>
              <PrimaryStyledButton onClick={handleFeedbackSubmit} disabled={isSubmitting || !feedbackText.trim()}>
                {isSubmitting ? 'Submitting...' : 'Submit'}
              </PrimaryStyledButton>
            </>
          }
        >
          <TextArea
            aria-label="Feedback input"
            value={feedbackText}
            onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setFeedbackText(e.target.value)}
            placeholder="Tell us what you think about this response..."
            autoFocus
          />
        </ChatModal>
      )}
    </MessageWrapper>
  )
}
