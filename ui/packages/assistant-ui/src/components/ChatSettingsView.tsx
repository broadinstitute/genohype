import React, { useState } from 'react'
import styled from 'styled-components'
import { ChatModal } from './ChatModal'

const SettingsContainer = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
  background: #f7f7f7;
  overflow: hidden;
`

const SettingsBody = styled.div`
  display: flex;
  flex: 1;
  overflow: hidden;
`

const SettingsNav = styled.nav`
  width: 180px;
  background: white;
  border-right: 1px solid #e0e0e0;
  padding: 12px 0;
  overflow-y: auto;
`

const NavItem = styled.button<{ active: boolean }>`
  width: 100%;
  padding: 10px 20px;
  border: none;
  background: ${props => props.active ? '#f0f7fd' : 'transparent'};
  color: ${props => props.active ? '#0d79d0' : '#333'};
  font-size: 14px;
  font-weight: ${props => props.active ? '600' : '400'};
  text-align: left;
  cursor: pointer;
  border-left: 3px solid ${props => props.active ? '#0d79d0' : 'transparent'};
  transition: all 0.2s;

  &:hover { background: #f0f7fd; }
`

const SettingsContent = styled.div`
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 20px;
  padding: 20px;
  overflow-y: auto;
  background: #f7f7f7;
`

const SectionTitle = styled.h3`
  margin: 0 0 16px 0;
  font-size: 18px;
  font-weight: 600;
  color: #333;
`

const SettingItem = styled.div`
  display: flex;
  flex-direction: column;
  gap: 6px;
`

const SettingLabel = styled.label`
  font-size: 13px;
  font-weight: 500;
  color: #666;
`

const Select = styled.select`
  padding: 8px 12px;
  border: 1px solid #e0e0e0;
  border-radius: 6px;
  font-size: 14px;
  background: white;
  cursor: pointer;

  &:hover { border-color: #0d79d0; }
  &:focus {
    outline: none;
    border-color: #0d79d0;
    box-shadow: 0 0 0 3px rgba(13, 121, 208, 0.1);
  }
`

const TextArea = styled.textarea`
  padding: 8px 12px;
  border: 1px solid #e0e0e0;
  border-radius: 6px;
  font-size: 14px;
  font-family: inherit;
  resize: vertical;
  min-height: 80px;

  &:hover { border-color: #0d79d0; }
  &:focus {
    outline: none;
    border-color: #0d79d0;
    box-shadow: 0 0 0 3px rgba(13, 121, 208, 0.1);
  }
`

const FeedbackContainer = styled.div`
  margin-top: 20px;
  padding-top: 20px;
  border-top: 1px solid #e0e0e0;
`

const Button = styled.button`
  padding: 8px 16px;
  border: 1px solid #e0e0e0;
  border-radius: 4px;
  background: white;
  cursor: pointer;
  font-size: 14px;

  &:hover { border-color: #0d79d0; background: #f0f7fd; }
  &:disabled { opacity: 0.5; cursor: not-allowed; }
`

const PrimaryButton = styled(Button)`
  background: #0d79d0;
  color: white;
  border-color: #0d79d0;

  &:hover { background: #0a5fa3; }
`

export interface SavedPrompt {
  id: string
  name: string
  prompt: string
}

export interface ModelOption {
  value: string
  label: string
}

export interface ChatSettingsViewProps {
  selectedModel: string
  onModelChange: (model: string) => void
  modelOptions?: ModelOption[]
  customPrompt: string
  onCustomPromptChange: (prompt: string) => void
  savedPrompts: SavedPrompt[]
  activePromptId: string | null
  onPromptSelect: (promptId: string) => void
  onSavePrompt: (promptName: string) => void
  onDeletePrompt: (promptId: string) => void
  activeSection?: string
  onSectionChange?: (section: string) => void
  onSubmitFeedback?: (text: string) => Promise<void>
  /** Optional extra content rendered at the top of the general settings */
  extraSettingsContent?: React.ReactNode
}

const DEFAULT_MODELS: ModelOption[] = [
  { value: 'gemini-2.5-flash', label: 'Gemini 2.5 Flash' },
  { value: 'gemini-2.5-pro', label: 'Gemini 2.5 Pro' },
]

export const ChatSettingsView: React.FC<ChatSettingsViewProps> = ({
  selectedModel,
  onModelChange,
  modelOptions = DEFAULT_MODELS,
  customPrompt,
  onCustomPromptChange,
  savedPrompts,
  activePromptId,
  onPromptSelect,
  onSavePrompt,
  onDeletePrompt,
  activeSection: activeSectionProp = 'general',
  onSectionChange,
  onSubmitFeedback,
  extraSettingsContent,
}) => {
  const [promptName, setPromptName] = useState('')
  const [isFeedbackModalOpen, setIsFeedbackModalOpen] = useState(false)
  const [feedbackText, setFeedbackText] = useState('')
  const [isSubmittingFeedback, setIsSubmittingFeedback] = useState(false)

  const handleSaveClick = () => {
    onSavePrompt(promptName)
    setPromptName('')
  }

  const handleFeedbackSubmit = async () => {
    if (!feedbackText.trim() || !onSubmitFeedback) return
    setIsSubmittingFeedback(true)
    try {
      await onSubmitFeedback(feedbackText)
      setIsFeedbackModalOpen(false)
      setFeedbackText('')
    } catch {
      // Consumer handles error display
    } finally {
      setIsSubmittingFeedback(false)
    }
  }

  const renderGeneralSection = () => (
    <>
      <SectionTitle>General Settings</SectionTitle>
      {extraSettingsContent}
      <SettingItem>
        <SettingLabel htmlFor="model-select">Model</SettingLabel>
        <Select id="model-select" value={selectedModel} onChange={(e) => onModelChange(e.target.value)}>
          {modelOptions.map((m) => (
            <option key={m.value} value={m.value}>{m.label}</option>
          ))}
        </Select>
      </SettingItem>

      <SettingItem>
        <SettingLabel htmlFor="saved-prompts">Saved Prompts</SettingLabel>
        <Select id="saved-prompts" value={activePromptId || ''} onChange={(e) => onPromptSelect(e.target.value)}>
          <option value="">None</option>
          {savedPrompts.map(p => (
            <option key={p.id} value={p.id}>{p.name}</option>
          ))}
        </Select>
      </SettingItem>

      <SettingItem>
        <SettingLabel htmlFor="custom-prompt">Custom System Prompt</SettingLabel>
        <TextArea
          id="custom-prompt"
          value={customPrompt}
          onChange={(e) => onCustomPromptChange(e.target.value)}
          placeholder="Add additional instructions for the assistant (optional)..."
        />
      </SettingItem>

      <SettingItem>
        <SettingLabel htmlFor="prompt-name">Save Current Prompt As</SettingLabel>
        <div style={{ display: 'flex', gap: '8px' }}>
          <input
            id="prompt-name"
            type="text"
            value={promptName}
            onChange={(e) => setPromptName(e.target.value)}
            placeholder="e.g., Rare Disease Focus"
            style={{ flex: 1, padding: '8px 12px', border: '1px solid #e0e0e0', borderRadius: '6px', fontSize: '14px' }}
          />
          <PrimaryButton onClick={handleSaveClick} disabled={!promptName.trim() || !customPrompt.trim()}>
            Save
          </PrimaryButton>
        </div>
      </SettingItem>

      {activePromptId && (
        <SettingItem>
          <Button onClick={() => onDeletePrompt(activePromptId)}>Delete Current Prompt</Button>
        </SettingItem>
      )}

      {onSubmitFeedback && (
        <FeedbackContainer>
          <SettingLabel>Feedback</SettingLabel>
          <p style={{ fontSize: '13px', color: '#666', margin: '4px 0 12px' }}>
            Have feedback about the assistant? We'd love to hear it!
          </p>
          <Button onClick={() => setIsFeedbackModalOpen(true)}>Provide General Feedback</Button>
        </FeedbackContainer>
      )}
    </>
  )

  return (
    <SettingsContainer>
      <SettingsBody>
        {onSectionChange && (
          <SettingsNav>
            <NavItem active={activeSectionProp === 'general'} onClick={() => onSectionChange('general')}>
              General
            </NavItem>
          </SettingsNav>
        )}
        <SettingsContent>
          {renderGeneralSection()}
        </SettingsContent>
      </SettingsBody>
      {isFeedbackModalOpen && (
        <ChatModal
          title="Provide General Feedback"
          onRequestClose={() => setIsFeedbackModalOpen(false)}
          footer={
            <>
              <Button onClick={() => setIsFeedbackModalOpen(false)} disabled={isSubmittingFeedback}>Cancel</Button>
              <PrimaryButton onClick={handleFeedbackSubmit} disabled={isSubmittingFeedback || !feedbackText.trim()}>
                {isSubmittingFeedback ? 'Submitting...' : 'Submit'}
              </PrimaryButton>
            </>
          }
        >
          <TextArea
            aria-label="Feedback input"
            value={feedbackText}
            onChange={(e) => setFeedbackText(e.target.value)}
            placeholder="Tell us about your experience with the assistant..."
            autoFocus
          />
        </ChatModal>
      )}
    </SettingsContainer>
  )
}
