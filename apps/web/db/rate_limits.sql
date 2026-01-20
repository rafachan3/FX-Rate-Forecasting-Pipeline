-- Rate limits table for tracking API request limits
-- This table is created automatically by the rate-limit.ts utility,
-- but you can run this script manually if needed.

CREATE TABLE IF NOT EXISTS rate_limits (
  id SERIAL PRIMARY KEY,
  identifier VARCHAR(100) NOT NULL,
  ip_address VARCHAR(45) NOT NULL,
  request_count INTEGER NOT NULL DEFAULT 1,
  window_start TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
  UNIQUE(identifier, ip_address, window_start)
);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_rate_limits_lookup 
ON rate_limits(identifier, ip_address, window_start);

-- Index for cleanup operations
CREATE INDEX IF NOT EXISTS idx_rate_limits_cleanup 
ON rate_limits(window_start);

-- Optional: Add a comment
COMMENT ON TABLE rate_limits IS 'Tracks rate limits for API endpoints by IP address and identifier';
