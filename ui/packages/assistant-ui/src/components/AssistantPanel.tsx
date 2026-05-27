import React, { useState, useRef, useCallback, useMemo } from 'react'
import styled, { css } from 'styled-components'
import { CopilotChat } from '@copilotkit/react-ui'
import { useCopilotAction, useCopilotAdditionalInstructions } from '@copilotkit/react-core'
import '@copilotkit/react-ui/styles.css'
import { useAssistantContext } from '../providers/AssistantProvider'
import { useMCPStateRender } from '../hooks/useMCPStateRender'
import { useThreadLoader } from '../hooks/useThreadLoader'
import { NavigationBar, type AssistantView } from './NavigationBar'
import { ChatSettingsView, type SavedPrompt, type ModelOption } from './ChatSettingsView'
import { CustomAssistantMessage } from './CustomAssistantMessage'
import { AdminView } from './admin/AdminView'

// ── Styled components ──────────────────────────────────────────────

const PageContainer = styled.div`
  display: flex;
  height: 100vh;
  width: 100%;
  overflow: hidden;
  position: relative;
`

const MainContent = styled.div`
  flex: 1;
  overflow: auto;
  display: flex;
  flex-direction: column;
  min-width: 300px;
`

const ChatPanel = styled.div<{ width: number; mode: 'side' | 'fullscreen' }>`
  display: flex;
  flex-direction: column;
  background: white;
  min-width: 300px;
  position: relative;
  box-sizing: border-box;

  ${(props) =>
    props.mode === 'side' &&
    css`
      width: ${props.width}px;
      max-width: 80%;
      overflow: hidden;
      padding-right: 8px;
    `}

  ${(props) =>
    props.mode === 'fullscreen' &&
    css`
      position: fixed;
      top: 0;
      right: 0;
      width: 100vw;
      height: 100vh;
      z-index: 1000;
      max-width: 100%;
      overflow: hidden;
    `}
`

const ResizeHandle = styled.div`
  width: 4px;
  background-color: #e0e0e0;
  cursor: col-resize;
  flex-shrink: 0;
  transition: background-color 0.2s;

  &:hover, &:active { background-color: #0d79d0; }
`

const ContextUpdateBanner = styled.div`
  position: absolute;
  top: 60px;
  left: 20px;
  right: 20px;
  z-index: 100;
  padding: 8px 12px;
  background: rgba(227, 242, 253, 0.95);
  border: 1px solid #90caf9;
  border-radius: 4px;
  font-size: 13px;
  font-weight: 500;
  color: #1976d2;
  text-align: center;
`

const IconButton = styled.button`
  position: absolute;
  top: 10px;
  z-index: 99999;
  padding: 8px;
  background: white;
  border: 1px solid #e0e0e0;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.2s;
  pointer-events: auto;
  font-size: 14px;
  line-height: 1;

  &:hover {
    background: #f7f7f7;
    border-color: #0d79d0;
  }
`

const CloseButton = styled(IconButton)`
  right: 20px;
  &:hover { border-color: #d32f2f; }
`

const FullscreenButton = styled(IconButton)`
  right: 60px;
`

const BadgeContainer = styled.div`
  display: flex;
  gap: 8px;
  padding: 8px 12px;
  flex-wrap: wrap;
  background: white;
  border-top: 1px solid #e0e0e0;
  flex-shrink: 0;
`

const ModelBadge = styled.div`
  padding: 6px 12px;
  background: #f7f7f7;
  border: 1px solid #e0e0e0;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 500;
  color: #666;
`

const ContextBadge = styled.div`
  padding: 6px 12px;
  background: #e3f2fd;
  border: 1px solid #90caf9;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 500;
  color: #1976d2;
  display: flex;
  align-items: center;
  gap: 6px;
  max-width: calc(100% - 200px);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;

  .context-type {
    font-weight: 600;
    text-transform: uppercase;
    font-size: 11px;
  }

  .context-id {
    font-family: monospace;
    opacity: 0.9;
  }
`

const FullscreenContainer = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  z-index: 1000;
  background: white;
`

const FullscreenChatArea = styled.div`
  flex: 1;
  display: flex;
  flex-direction: column;
  position: relative;
`

const ToggleButton = styled.button`
  position: fixed;
  bottom: 24px;
  right: 24px;
  z-index: 1000;
  padding: 12px 24px;
  border-radius: 8px;
  border: 1px solid #ddd;
  background-color: #fff;
  color: #333;
  font-size: 16px;
  font-weight: 500;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
  cursor: pointer;
  transition: all 0.2s ease-in-out;

  &:hover {
    background-color: #f7f7f7;
    box-shadow: 0 6px 16px rgba(0, 0, 0, 0.12);
  }
`

const StyledCopilotChat = styled(CopilotChat)`
  height: 100%;
  position: relative;
  z-index: 1;
  overflow: hidden;

  --copilot-kit-primary-color: #0d79d0;
  --copilot-kit-background-color: white;
  --copilot-kit-header-background: #f7f7f7;
  --copilot-kit-separator-color: rgba(0, 0, 0, 0.08);
  --copilot-kit-border-radius: 0.5rem;

  .copilotKitMessages {
    padding: 1rem;
    padding-top: calc(1rem + 40px);
    overflow-x: hidden;
    overflow-y: auto;
  }

  .copilotKitMessages::-webkit-scrollbar { width: 8px; }
  .copilotKitMessages::-webkit-scrollbar-track { background: transparent; }
  .copilotKitMessages::-webkit-scrollbar-thumb { background: #d0d0d0; border-radius: 4px; }
  .copilotKitMessages::-webkit-scrollbar-thumb:hover { background: #a0a0a0; }

  .copilotKitMessage { border-radius: 0.75rem; }

  .copilotKitInputContainer {
    width: calc(100% - 48px) !important;
    margin: 0 auto !important;
    padding: 0 8px !important;
    box-sizing: border-box !important;
  }

  .copilotKitInput {
    border-radius: 0.75rem;
    border: 1px solid var(--copilot-kit-separator-color) !important;
  }

  .copilotKitMessages footer .suggestions .suggestion {
    font-size: 14px !important;
    border-radius: 0.5rem;
  }

  .copilotKitMessages footer .suggestions button:not(:disabled):hover {
    background-color: #f0f9ff;
    border-color: var(--copilot-kit-primary-color);
    transform: scale(1.03);
  }
`

// ── Types ──────────────────────────────────────────────────────────

export interface PageContext {
  type: string
  id: string
  detail?: string
}

export type AssistantDisplayMode = 'closed' | 'side' | 'fullscreen'

export interface AssistantPanelProps {
  /** How the panel starts — sidebar, fullscreen, or closed. */
  defaultMode?: AssistantDisplayMode

  /** Panel title shown in the chat header. */
  title?: string

  /** Greeting message shown at the start. */
  initialMessage?: string

  /** Placeholder for the chat input. */
  placeholder?: string

  /** Suggestions displayed in the chat. Domain-specific — consumer provides. */
  suggestions?: Array<{ title: string; message: string }>

  /** Current page context (e.g., which gene/variant/region is being viewed). */
  pageContext?: PageContext | null

  /** Toggle button label when chat is closed. */
  toggleLabel?: string

  /** Whether to show the admin tab. Consumer determines access. */
  allowAdmin?: boolean

  /** Custom admin view component rendered in the Admin tab. */
  adminView?: React.ReactNode

  /** Render the sidebar in fullscreen mode (e.g., chat history). */
  fullscreenSidebar?: React.ReactNode

  /** Model options for the settings view. */
  modelOptions?: ModelOption[]

  /** Additional instructions injected into the conversation. */
  customPrompt?: string

  /** Callback when custom prompt changes (for persistence). */
  onCustomPromptChange?: (prompt: string) => void

  /** Selected model ID. */
  selectedModel?: string

  /** Callback when model changes (for persistence). */
  onModelChange?: (model: string) => void

  /** Callback for submitting general feedback. */
  onSubmitFeedback?: (text: string) => Promise<void>

  /** Register a navigation action so the LLM can navigate pages. */
  onNavigate?: (url: string) => void

  /** CSS class name for the outer container. */
  className?: string

  /** The main application content to wrap (only needed for side mode). */
  children?: React.ReactNode
}

// ── Component ──────────────────────────────────────────────────────

export function AssistantPanel({
  defaultMode = 'side',
  title = 'Genomic Assistant',
  initialMessage = "Hello! I can help you understand genomic data or answer questions about what you're viewing.",
  suggestions = [],
  pageContext,
  toggleLabel = 'Ask Assistant',
  allowAdmin = false,
  adminView,
  fullscreenSidebar,
  modelOptions,
  customPrompt: controlledPrompt,
  onCustomPromptChange,
  selectedModel: controlledModel,
  onModelChange,
  onSubmitFeedback,
  onNavigate,
  className,
  children,
}: AssistantPanelProps) {
  // Use provider's display mode so it survives CopilotKit remounts (new chat)
  const { displayMode: chatDisplayMode, setDisplayMode: setChatDisplayMode, persistentSidebar } = useAssistantContext()
  const [activeView, setActiveView] = useState<AssistantView | null>(null)
  const [chatWidth, setChatWidth] = useState(typeof window !== 'undefined' ? window.innerWidth / 3 : 400)
  const [contextNotification, setContextNotification] = useState<string | null>(null)

  // Settings state (uncontrolled fallbacks)
  const [internalModel, setInternalModel] = useState(controlledModel || 'gemini-3.1-flash')
  const [internalPrompt, setInternalPrompt] = useState(controlledPrompt || '')
  const [savedPrompts, setSavedPrompts] = useState<SavedPrompt[]>([])
  const [activePromptId, setActivePromptId] = useState<string | null>(null)
  const [adminSection, setAdminSection] = useState('feedback')

  const model = controlledModel ?? internalModel
  const prompt = controlledPrompt ?? internalPrompt
  const handleModelChange = onModelChange ?? setInternalModel
  const handlePromptChange = onCustomPromptChange ?? setInternalPrompt

  const isResizing = useRef(false)
  const containerRef = useRef<HTMLDivElement>(null)
  const lastSentContext = useRef<string | null>(null)

  const isChatOpen = chatDisplayMode !== 'closed'

  // ── Hooks ────────────────────────────────────────────────────────

  // Register custom instructions
  useCopilotAdditionalInstructions(
    { instructions: prompt, available: prompt ? 'enabled' : 'disabled' },
    [prompt]
  )

  // Register MCP state rendering
  useMCPStateRender()

  // Load thread history when switching threads
  useThreadLoader()

  // Register navigation action if provided
  useCopilotAction({
    name: 'navigateToPage',
    description: 'Navigate to a page in the browser (variant, gene, or region).',
    parameters: [
      { name: 'url', type: 'string' as const, description: 'The URL path to navigate to.', required: true },
    ],
    handler: async ({ url }: { url: string }) => {
      if (onNavigate) {
        onNavigate(url)
        return { message: `Navigating to ${url}` }
      }
      return { error: 'Navigation not available' }
    },
    available: onNavigate ? 'enabled' : 'disabled',
  })

  // ── Context notification ─────────────────────────────────────────

  React.useEffect(() => {
    if (!pageContext) return
    const contextId = `${pageContext.type}:${pageContext.id}`
    if (lastSentContext.current === contextId) return

    const isNavigation = lastSentContext.current !== null
    lastSentContext.current = contextId

    if (isNavigation) {
      setContextNotification(`Context updated to ${pageContext.type}: ${pageContext.id}`)
      setTimeout(() => setContextNotification(null), 5000)
    }
  }, [pageContext])

  // ── Settings handlers ────────────────────────────────────────────

  const handlePromptSelect = useCallback((promptId: string) => {
    if (!promptId) {
      setActivePromptId(null)
      handlePromptChange('')
    } else {
      const p = savedPrompts.find(s => s.id === promptId)
      if (p) {
        setActivePromptId(promptId)
        handlePromptChange(p.prompt)
      }
    }
  }, [savedPrompts, handlePromptChange])

  const handleSavePrompt = useCallback((name: string) => {
    if (!name.trim() || !prompt.trim()) return
    const newPrompt: SavedPrompt = { id: Date.now().toString(), name: name.trim(), prompt }
    setSavedPrompts(prev => [...prev, newPrompt])
    setActivePromptId(newPrompt.id)
  }, [prompt])

  const handleDeletePrompt = useCallback((promptId: string) => {
    setSavedPrompts(prev => prev.filter(p => p.id !== promptId))
    if (activePromptId === promptId) {
      setActivePromptId(null)
      handlePromptChange('')
    }
  }, [activePromptId, handlePromptChange])

  // ── View navigation ──────────────────────────────────────────────

  const handleNavigate = useCallback((view: AssistantView) => {
    setActiveView(view === 'chat' ? null : view)
  }, [])

  // ── Resize handling ──────────────────────────────────────────────

  const handleMouseDown = useCallback(() => {
    isResizing.current = true
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'
  }, [])

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (!isResizing.current || !containerRef.current) return
    const rect = containerRef.current.getBoundingClientRect()
    const newWidth = rect.right - e.clientX
    if (newWidth >= 300 && newWidth <= rect.width * 0.8) setChatWidth(newWidth)
  }, [])

  const handleMouseUp = useCallback(() => {
    isResizing.current = false
    document.body.style.cursor = ''
    document.body.style.userSelect = ''
  }, [])

  React.useEffect(() => {
    if (chatDisplayMode !== 'side') return
    document.addEventListener('mousemove', handleMouseMove)
    document.addEventListener('mouseup', handleMouseUp)
    return () => {
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }
  }, [chatDisplayMode, handleMouseMove, handleMouseUp])

  // ── Model display name ───────────────────────────────────────────

  const modelDisplayName = useMemo(() => {
    const opt = modelOptions?.find(m => m.value === model)
    return opt?.label || model
  }, [model, modelOptions])

  // ── Shared chat content ──────────────────────────────────────────

  const renderChatContent = () => (
    <>
      <StyledCopilotChat
        labels={{ title, initial: initialMessage }}
        suggestions={suggestions}
        AssistantMessage={CustomAssistantMessage}
      />
      {contextNotification && <ContextUpdateBanner>{contextNotification}</ContextUpdateBanner>}
      <BadgeContainer>
        <ModelBadge title={`Current model: ${model}`}>{modelDisplayName}</ModelBadge>
        {pageContext && (
          <ContextBadge title={`Current context: ${pageContext.type} - ${pageContext.id}`}>
            <span className="context-type">{pageContext.type}</span>
            <span className="context-id">{pageContext.id}</span>
          </ContextBadge>
        )}
      </BadgeContainer>
    </>
  )

  const renderSettings = () => (
    <ChatSettingsView
      selectedModel={model}
      onModelChange={handleModelChange}
      modelOptions={modelOptions}
      customPrompt={prompt}
      onCustomPromptChange={handlePromptChange}
      savedPrompts={savedPrompts}
      activePromptId={activePromptId}
      onPromptSelect={handlePromptSelect}
      onSavePrompt={handleSavePrompt}
      onDeletePrompt={handleDeletePrompt}
      onSubmitFeedback={onSubmitFeedback}
    />
  )

  const renderPanelBody = () => (
    <>
      <NavigationBar activeView={activeView} onNavigate={handleNavigate} allowAdmin={allowAdmin} />
      {activeView === 'settings' && renderSettings()}
      {activeView === 'admin' && (adminView || <AdminView activeSection={adminSection} onSectionChange={setAdminSection} />)}
      {!activeView && renderChatContent()}
      <CloseButton onClick={() => setChatDisplayMode('closed')} title="Close Assistant">
        &times;
      </CloseButton>
      <FullscreenButton
        onClick={() => setChatDisplayMode(chatDisplayMode === 'fullscreen' ? 'side' : 'fullscreen')}
        title={chatDisplayMode === 'fullscreen' ? 'Exit fullscreen' : 'Enter fullscreen'}
      >
        {chatDisplayMode === 'fullscreen' ? '\u2198' : '\u2197'}
      </FullscreenButton>
    </>
  )

  // ── Render ───────────────────────────────────────────────────────

  return (
    <>
      <PageContainer ref={containerRef} className={className}>
        <MainContent>{children}</MainContent>
        {isChatOpen && chatDisplayMode === 'side' && (
          <>
            <ResizeHandle onMouseDown={handleMouseDown} />
            <ChatPanel width={chatWidth} mode="side">
              {renderPanelBody()}
            </ChatPanel>
          </>
        )}
      </PageContainer>

      {chatDisplayMode === 'fullscreen' && (
        <FullscreenContainer>
          {persistentSidebar || fullscreenSidebar}
          <FullscreenChatArea>
            {renderPanelBody()}
          </FullscreenChatArea>
        </FullscreenContainer>
      )}

      {!isChatOpen && (
        <ToggleButton onClick={() => setChatDisplayMode('side')}>
          {toggleLabel}
        </ToggleButton>
      )}
    </>
  )
}
