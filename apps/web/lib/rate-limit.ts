/**
 * Rate limiting utility for Next.js API routes.
 * Uses Postgres for rate limit tracking (works in serverless/Vercel).
 */

import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';

export interface RateLimitConfig {
  /** Maximum number of requests allowed */
  maxRequests: number;
  /** Time window in seconds */
  windowSeconds: number;
  /** Identifier for the rate limit (e.g., 'subscribe', 'unsubscribe') */
  identifier: string;
}

export interface RateLimitResult {
  /** Whether the request is allowed */
  allowed: boolean;
  /** Number of requests remaining in the current window */
  remaining: number;
  /** Unix timestamp when the rate limit resets */
  resetAt: number;
}

/**
 * Get client IP address from request headers.
 * Handles Vercel's proxy headers.
 */
function getClientIP(request: NextRequest): string {
  // Vercel provides IP in x-forwarded-for or x-real-ip
  const forwardedFor = request.headers.get('x-forwarded-for');
  if (forwardedFor) {
    // x-forwarded-for can contain multiple IPs, take the first one
    return forwardedFor.split(',')[0].trim();
  }
  
  const realIP = request.headers.get('x-real-ip');
  if (realIP) {
    return realIP.trim();
  }
  
  // Fallback: use a default identifier if IP cannot be determined
  // In production, this should rarely happen
  return 'unknown';
}

/**
 * Initialize rate limit table if it doesn't exist.
 * This is idempotent and safe to call multiple times.
 */
async function ensureRateLimitTable(): Promise<void> {
  try {
    await sql`
      CREATE TABLE IF NOT EXISTS rate_limits (
        id SERIAL PRIMARY KEY,
        identifier VARCHAR(100) NOT NULL,
        ip_address VARCHAR(45) NOT NULL,
        request_count INTEGER NOT NULL DEFAULT 1,
        window_start TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        UNIQUE(identifier, ip_address, window_start)
      )
    `;
    
    // Create index for faster lookups
    await sql`
      CREATE INDEX IF NOT EXISTS idx_rate_limits_lookup 
      ON rate_limits(identifier, ip_address, window_start)
    `;
    
    // Create index for cleanup
    await sql`
      CREATE INDEX IF NOT EXISTS idx_rate_limits_cleanup 
      ON rate_limits(window_start)
    `;
  } catch (error) {
    // Table might already exist, ignore error
    console.warn('Rate limit table creation warning:', error);
  }
}

/**
 * Clean up old rate limit records (older than 24 hours).
 * This prevents the table from growing indefinitely.
 */
async function cleanupOldRecords(): Promise<void> {
  try {
    await sql`
      DELETE FROM rate_limits
      WHERE window_start < NOW() - INTERVAL '24 hours'
    `;
  } catch (error) {
    // Ignore cleanup errors, not critical
    console.warn('Rate limit cleanup warning:', error);
  }
}

/**
 * Check and enforce rate limit for a request.
 * 
 * @param request - Next.js request object
 * @param config - Rate limit configuration
 * @returns Rate limit result with allowed status and remaining requests
 */
export async function checkRateLimit(
  request: NextRequest,
  config: RateLimitConfig
): Promise<RateLimitResult> {
  const ip = getClientIP(request);
  const now = new Date();
  const windowStart = new Date(now.getTime() - (config.windowSeconds * 1000));
  const windowStartIso = windowStart.toISOString(); // Pass as primitive string
  
  // Ensure table exists (idempotent)
  await ensureRateLimitTable();
  
  // Clean up old records periodically (10% chance to avoid overhead)
  if (Math.random() < 0.1) {
    await cleanupOldRecords();
  }
  
  try {
    // Try to get or create rate limit record
    const result = await sql`
      INSERT INTO rate_limits (identifier, ip_address, request_count, window_start)
      VALUES (${config.identifier}, ${ip}, 1, ${windowStartIso})
      ON CONFLICT (identifier, ip_address, window_start)
      DO UPDATE SET
        request_count = rate_limits.request_count + 1
      RETURNING request_count, window_start
    `;
    
    const record = result.rows[0];
    const currentCount = parseInt(record.request_count, 10);
    const windowStartTime = new Date(record.window_start);
    
    // Calculate reset time (window start + window duration)
    const resetAt = Math.floor((windowStartTime.getTime() + (config.windowSeconds * 1000)) / 1000);
    
    const allowed = currentCount <= config.maxRequests;
    const remaining = Math.max(0, config.maxRequests - currentCount);
    
    return {
      allowed,
      remaining,
      resetAt,
    };
  } catch (error) {
    // If rate limiting fails, log but allow the request (fail open)
    // This prevents rate limiting from breaking the service
    console.error('Rate limit check error:', error);
    return {
      allowed: true,
      remaining: config.maxRequests,
      resetAt: Math.floor((now.getTime() + (config.windowSeconds * 1000)) / 1000),
    };
  }
}

/**
 * Create a rate limit error response.
 */
export function rateLimitErrorResponse(
  result: RateLimitResult,
  config: RateLimitConfig
): NextResponse {
  const resetDate = new Date(result.resetAt * 1000);
  const resetSeconds = Math.ceil((resetDate.getTime() - Date.now()) / 1000);
  
  return NextResponse.json(
    {
      ok: false,
      error: 'Rate limit exceeded',
      message: `Too many requests. Please try again in ${resetSeconds} seconds.`,
      retry_after: resetSeconds,
    },
    {
      status: 429,
      headers: {
        'X-RateLimit-Limit': config.maxRequests.toString(),
        'X-RateLimit-Remaining': result.remaining.toString(),
        'X-RateLimit-Reset': result.resetAt.toString(),
        'Retry-After': resetSeconds.toString(),
      },
    }
  );
}
