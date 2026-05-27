import React from 'react'
import styled from 'styled-components'

const NavBarContainer = styled.div`
  display: flex;
  background: white;
  border-bottom: 2px solid #e0e0e0;
  flex-shrink: 0;
`

const NavTab = styled.button<{ active: boolean }>`
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 14px 20px;
  border: none;
  background: ${props => props.active ? '#f0f7fd' : 'transparent'};
  color: ${props => props.active ? '#0d79d0' : '#666'};
  font-size: 14px;
  font-weight: ${props => props.active ? '600' : '500'};
  cursor: pointer;
  border-bottom: 3px solid ${props => props.active ? '#0d79d0' : 'transparent'};
  transition: all 0.2s;
  position: relative;
  top: 2px;

  &:hover {
    background: #f0f7fd;
    color: #0d79d0;
  }
`

export type AssistantView = 'chat' | 'settings' | 'admin'

export interface NavigationBarProps {
  activeView: AssistantView | null
  onNavigate: (view: AssistantView) => void
  allowAdmin?: boolean
}

export const NavigationBar: React.FC<NavigationBarProps> = ({
  activeView,
  onNavigate,
  allowAdmin = false,
}) => {
  const currentView = activeView || 'chat'

  return (
    <NavBarContainer>
      <NavTab active={currentView === 'chat'} onClick={() => onNavigate('chat')}>
        Chat
      </NavTab>
      <NavTab active={currentView === 'settings'} onClick={() => onNavigate('settings')}>
        Settings
      </NavTab>
      {allowAdmin && (
        <NavTab active={currentView === 'admin'} onClick={() => onNavigate('admin')}>
          Admin
        </NavTab>
      )}
    </NavBarContainer>
  )
}
