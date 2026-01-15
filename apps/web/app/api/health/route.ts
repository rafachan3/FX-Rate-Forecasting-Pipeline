import { NextResponse } from 'next/server';
import { sql } from '@/lib/db';

/**
 * Health check endpoint.
 * Returns simple OK status for monitoring.
 * Includes database connectivity check.
 */
export async function GET() {
  let dbOk = false;
  
  try {
    // Quick DB connectivity check
    await sql`SELECT 1`;
    dbOk = true;
  } catch (error) {
    // Log error but don't expose connection details
    console.error('Database health check failed');
    dbOk = false;
  }

  return NextResponse.json({
    ok: dbOk,
    service: 'northbound-web',
    env: process.env.NODE_ENV || 'development',
    time_utc: new Date().toISOString(),
    db_connected: dbOk,
  });
}
