import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const token = searchParams.get('token');

  if (!token) {
    return NextResponse.json(
      { ok: false, error: 'Missing verification token' },
      { status: 400 }
    );
  }

  try {
    const result = await sql`
      SELECT id, email, verified_at, verification_expires_at
      FROM subscriptions
      WHERE verification_token = ${token}
    `;

    if (result.rows.length === 0) {
      return NextResponse.json(
        { ok: false, error: 'Invalid verification token' },
        { status: 400 }
      );
    }

    const row = result.rows[0];
    const expiresAt = row.verification_expires_at
      ? new Date(row.verification_expires_at)
      : null;

    if (expiresAt && expiresAt.getTime() < Date.now()) {
      return NextResponse.json(
        { ok: false, error: 'Verification token has expired' },
        { status: 400 }
      );
    }

    // Idempotent: if already verified, return success
    if (row.verified_at) {
      return NextResponse.json({
        ok: true,
        status: 'already_verified',
        email: row.email,
        message: 'Email already verified.',
      });
    }

    await sql`
      UPDATE subscriptions
      SET 
        verified_at = NOW(),
        verification_token = NULL,
        verification_expires_at = NULL,
        updated_at = NOW()
      WHERE id = ${row.id}
    `;

    return NextResponse.json({
      ok: true,
      status: 'verified',
      email: row.email,
      message: 'Email verified successfully.',
    });
  } catch (error) {
    console.error('Verification error:', error);
    return NextResponse.json(
      { ok: false, error: 'Server error. Please try again later.' },
      { status: 500 }
    );
  }
}
