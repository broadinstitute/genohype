-- Chat threads (conversations)
CREATE TABLE chat_threads (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  thread_id VARCHAR(255) UNIQUE NOT NULL,
  title VARCHAR(500),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  message_count INTEGER DEFAULT 0,
  total_input_tokens INTEGER DEFAULT 0,
  total_output_tokens INTEGER DEFAULT 0,
  total_request_tokens BIGINT DEFAULT 0,
  model VARCHAR(100),
  user_id VARCHAR(255),
  session_id VARCHAR(255),
  contexts JSONB DEFAULT '[]'::jsonb NOT NULL,
  title_generated_at_message_count INTEGER DEFAULT 0
);

CREATE TABLE chat_messages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  thread_id VARCHAR(255) NOT NULL REFERENCES chat_threads(thread_id) ON DELETE CASCADE,
  role VARCHAR(50),
  content TEXT,
  copilot_message_id VARCHAR(255),
  message_type VARCHAR(100),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  sequence_number SERIAL,
  input_tokens INTEGER,
  output_tokens INTEGER,
  system_prompt_tokens INTEGER,
  tool_definition_tokens INTEGER,
  history_tokens INTEGER,
  user_message_tokens INTEGER,
  tool_result_tokens INTEGER,
  raw_message JSONB,
  tool_result_id UUID,
  CONSTRAINT unique_copilot_message UNIQUE (thread_id, copilot_message_id)
);

CREATE TABLE tool_results (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  thread_id VARCHAR(255) NOT NULL,
  message_id VARCHAR(255) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  tool_name VARCHAR(255) NOT NULL,
  result_data JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  CONSTRAINT fk_thread FOREIGN KEY (thread_id) REFERENCES chat_threads(thread_id) ON DELETE CASCADE,
  UNIQUE(thread_id, message_id)
);

ALTER TABLE chat_messages ADD CONSTRAINT fk_tool_result FOREIGN KEY (tool_result_id) REFERENCES tool_results(id) ON DELETE SET NULL;

CREATE INDEX idx_threads_updated ON chat_threads(updated_at DESC);
CREATE INDEX idx_threads_user ON chat_threads(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_messages_thread ON chat_messages(thread_id, created_at);
CREATE INDEX idx_messages_created ON chat_messages(created_at DESC);
CREATE INDEX idx_threads_contexts ON chat_threads USING gin(contexts);
CREATE INDEX idx_tool_results_user ON tool_results(user_id);

CREATE TYPE user_role AS ENUM ('user', 'viewer', 'admin');

CREATE TABLE users (
  user_id VARCHAR(255) PRIMARY KEY,
  email VARCHAR(255),
  name VARCHAR(255),
  role user_role NOT NULL DEFAULT 'user',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ DEFAULT NOW(),
  allow_admin_viewing BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_users_email ON users(email);

CREATE TABLE chat_feedback (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  user_id VARCHAR(255) REFERENCES users(user_id) ON DELETE SET NULL,
  thread_id VARCHAR(255) REFERENCES chat_threads(thread_id) ON DELETE SET NULL,
  message_id VARCHAR(255),
  source VARCHAR(50) NOT NULL,
  rating INT,
  feedback_text TEXT,
  metadata JSONB
);

CREATE INDEX idx_feedback_thread ON chat_feedback(thread_id);
CREATE INDEX idx_feedback_user ON chat_feedback(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_feedback_source ON chat_feedback(source);

CREATE TABLE tool_invocations (
  id SERIAL PRIMARY KEY,
  thread_id VARCHAR(255) REFERENCES chat_threads(thread_id) ON DELETE CASCADE,
  message_id UUID REFERENCES chat_messages(id) ON DELETE CASCADE,
  tool_name TEXT NOT NULL,
  result_tokens INTEGER,
  execution_time_ms INTEGER,
  arguments JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_tool_invocations_thread ON tool_invocations(thread_id);
CREATE INDEX idx_tool_invocations_tool_name ON tool_invocations(tool_name);
CREATE INDEX idx_tool_invocations_created_at ON tool_invocations(created_at);

CREATE TABLE analytics_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  user_id VARCHAR(255) REFERENCES users(user_id) ON DELETE SET NULL,
  thread_id VARCHAR(255) REFERENCES chat_threads(thread_id) ON DELETE SET NULL,
  event_type VARCHAR(100) NOT NULL,
  payload JSONB,
  session_id VARCHAR(255)
);

CREATE INDEX idx_analytics_event_type ON analytics_events(event_type);
CREATE INDEX idx_analytics_user ON analytics_events(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_analytics_created_at ON analytics_events(created_at DESC);
