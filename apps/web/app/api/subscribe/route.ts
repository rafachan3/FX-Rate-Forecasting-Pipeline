import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { checkRateLimit, rateLimitErrorResponse, RateLimitConfig } from '@/lib/rate-limit';

function isValidEmail(email: string): boolean {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}

// Rate limit configuration for subscriptions
const SUBSCRIBE_RATE_LIMIT: RateLimitConfig = {
  maxRequests: 5, // 5 subscriptions per hour per IP
  windowSeconds: 3600, // 1 hour
  identifier: 'subscribe',
};

export async function POST(request: NextRequest) {
  // Check rate limit before processing
  const rateLimitResult = await checkRateLimit(request, SUBSCRIBE_RATE_LIMIT);
  if (!rateLimitResult.allowed) {
    return rateLimitErrorResponse(rateLimitResult, SUBSCRIBE_RATE_LIMIT);
  }

  try {
    const body = await request.json();
    const { email, pairs, frequency, weekly_day, monthly_timing, timezone } = body;

    // Validate email
    if (!email || typeof email !== 'string' || !isValidEmail(email)) {
      return NextResponse.json(
        { ok: false, error: 'Invalid email address' },
        { status: 400 }
      );
    }

    // Validate pairs
    if (!pairs || !Array.isArray(pairs) || pairs.length === 0) {
      return NextResponse.json(
        { ok: false, error: 'At least one FX pair must be selected' },
        { status: 400 }
      );
    }

    // Validate frequency
    const validFrequencies = ['DAILY', 'WEEKLY', 'MONTHLY'];
    if (!frequency || !validFrequencies.includes(frequency)) {
      return NextResponse.json(
        { ok: false, error: `Frequency must be one of: ${validFrequencies.join(', ')}` },
        { status: 400 }
      );
    }

    // Validate weekly_day if frequency is WEEKLY
    if (frequency === 'WEEKLY') {
      const validDays = ['MON', 'TUE', 'WED', 'THU', 'FRI'];
      if (!weekly_day || !validDays.includes(weekly_day)) {
        return NextResponse.json(
          { ok: false, error: `Weekly day must be one of: ${validDays.join(', ')}` },
          { status: 400 }
        );
      }
    }

    // Validate monthly_timing if frequency is MONTHLY
    if (frequency === 'MONTHLY') {
      const validTimings = ['FIRST_BUSINESS_DAY', 'LAST_BUSINESS_DAY'];
      if (!monthly_timing || !validTimings.includes(monthly_timing)) {
        return NextResponse.json(
          { ok: false, error: `Monthly timing must be one of: ${validTimings.join(', ')}` },
          { status: 400 }
        );
      }
    }

    const emailLower = email.toLowerCase();
    const timezoneValue = timezone || 'America/Toronto';

    // Convert pairs array to CSV string for SQL parameterization
    const pairsArr = Array.isArray(pairs) ? (pairs as string[]) : [];
    const pairsCsv: string | null = pairsArr.length > 0 ? pairsArr.join(',') : null;

    // Upsert subscription (insert or update, set is_active = true)
    const subscriptionResult = await sql`
      INSERT INTO subscriptions (email, is_active)
      VALUES (${emailLower}, true)
      ON CONFLICT (email) 
      DO UPDATE SET is_active = true
      RETURNING id
    `;

    const subscriptionId = subscriptionResult.rows[0].id;

    // Upsert subscription preferences
    await sql`
      INSERT INTO subscription_preferences (
        subscription_id,
        frequency,
        weekly_day,
        monthly_timing,
        pairs,
        timezone,
        updated_at
      )
      VALUES (
        ${subscriptionId},
        ${frequency},
        ${frequency === 'WEEKLY' ? weekly_day : null},
        ${frequency === 'MONTHLY' ? monthly_timing : null},
        COALESCE(string_to_array(${pairsCsv}, ','), '{}'::text[]),
        ${timezoneValue},
        NOW()
      )
      ON CONFLICT (subscription_id)
      DO UPDATE SET
        frequency = ${frequency},
        weekly_day = ${frequency === 'WEEKLY' ? weekly_day : null},
        monthly_timing = ${frequency === 'MONTHLY' ? monthly_timing : null},
        pairs = COALESCE(string_to_array(${pairsCsv}, ','), '{}'::text[]),
        timezone = ${timezoneValue},
        updated_at = NOW()
    `;

    return NextResponse.json({
      ok: true,
      status: 'created_or_updated',
      email: emailLower,
      subscription_id: subscriptionId.toString(),
      email_enabled: true,
      message: 'Subscription saved successfully.',
    });
  } catch (error) {
    console.error('Subscription error:', error);
    return NextResponse.json(
      { ok: false, error: 'Server error. Please try again later.' },
      { status: 500 }
    );
  }
}

// Rate limit configuration for unsubscribes
const UNSUBSCRIBE_RATE_LIMIT: RateLimitConfig = {
  maxRequests: 10, // 10 unsubscribe requests per hour per IP
  windowSeconds: 3600, // 1 hour
  identifier: 'unsubscribe',
};

export async function DELETE(request: NextRequest) {
  // Check rate limit before processing
  const rateLimitResult = await checkRateLimit(request, UNSUBSCRIBE_RATE_LIMIT);
  if (!rateLimitResult.allowed) {
    return rateLimitErrorResponse(rateLimitResult, UNSUBSCRIBE_RATE_LIMIT);
  }

  try {
    const body = await request.json();
    const { email } = body;

    // Validate email
    if (!email || typeof email !== 'string' || !isValidEmail(email)) {
      return NextResponse.json(
        { ok: false, error: 'Invalid email address' },
        { status: 400 }
      );
    }

    const emailLower = email.toLowerCase();

    // Soft delete: set is_active = false
    const result = await sql`
      UPDATE subscriptions
      SET is_active = false
      WHERE email = ${emailLower}
      RETURNING id
    `;

    // Return success even if email not found (idempotency)
    return NextResponse.json({
      ok: true,
      status: 'unsubscribed',
      email: emailLower,
    });
  } catch (error) {
    console.error('Unsubscribe error:', error);
    return NextResponse.json(
      { ok: false, error: 'Server error. Please try again later.' },
      { status: 500 }
    );
  }
}
