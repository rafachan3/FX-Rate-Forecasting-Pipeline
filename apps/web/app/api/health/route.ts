import { NextResponse } from 'next/server';

/**
 * Health check endpoint.
 * Returns simple OK status for monitoring.
 */
export async function GET() {
  return NextResponse.json({
    ok: true,
    service: 'northbound-web',
    env: process.env.NODE_ENV || 'development',
    time_utc: new Date().toISOString(),
  });
}
