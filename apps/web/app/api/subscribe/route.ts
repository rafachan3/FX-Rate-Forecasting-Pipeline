import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { checkRateLimit, rateLimitErrorResponse, RateLimitConfig } from '@/lib/rate-limit';
import { randomUUID } from 'crypto';

function isValidEmail(email: string): boolean {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}

function generateUnsubscribeToken(): string {
  // Generate a secure random token (64 hex characters)
  return randomUUID().replace(/-/g, '') + randomUUID().replace(/-/g, '');
}

function generateVerificationToken(): string {
  // Generate a secure random token (64 hex characters)
  return randomUUID().replace(/-/g, '') + randomUUID().replace(/-/g, '');
}

const VERIFICATION_TTL_HOURS = 24;

function getVerificationExpiry(): string {
  const expiresAt = new Date(Date.now() + VERIFICATION_TTL_HOURS * 60 * 60 * 1000);
  return expiresAt.toISOString();
}

async function sendVerificationEmail(email: string, token: string) {
  const apiKey = process.env.SENDGRID_API_KEY;
  if (!apiKey) {
    console.warn('SENDGRID_API_KEY not set, skipping verification email');
    return;
  }
  const websiteUrl = (process.env.WEBSITE_URL || 'https://northbound-fx.com').replace(/\/$/, '');
  const fromEmail = process.env.EMAIL_FROM || 'noreply@northbound-fx.com';
  const fromName = process.env.EMAIL_FROM_NAME || 'NorthBound FX';
  const verifyUrl = `${websiteUrl}/verify?token=${encodeURIComponent(token)}`;

  const subject = 'Verify your email for NorthBound FX';
  const textBody = [
    'Welcome to NorthBound FX!',
    '',
    'Please verify your email to start receiving FX signals:',
    verifyUrl,
    '',
    'If you did not request this, you can ignore this email.',
  ].join('\n');

  const htmlBody = `
    <p>Welcome to NorthBound FX!</p>
    <p>Please verify your email to start receiving FX signals:</p>
    <p><a href="${verifyUrl}">${verifyUrl}</a></p>
    <p>If you did not request this, you can ignore this email.</p>
  `;

  try {
    await fetch('https://api.sendgrid.com/v3/mail/send', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        personalizations: [
          {
            to: [{ email }],
          },
        ],
        from: { email: fromEmail, name: fromName },
        subject,
        content: [
          { type: 'text/plain', value: textBody },
          { type: 'text/html', value: htmlBody },
        ],
      }),
    });
  } catch (err) {
    console.error('Failed to send verification email', err);
  }
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

    const emailLower = email.toLowerCase().trim();
    const timezoneValue = timezone || 'America/Toronto';

    // Convert pairs array to CSV string for SQL parameterization
    const pairsArr = Array.isArray(pairs) ? (pairs as string[]) : [];
    const pairsCsv: string | null = pairsArr.length > 0 ? pairsArr.join(',') : null;

    // Check if subscriber already exists
    const existingResult = await sql`
      SELECT id, unsubscribe_token, verified_at, verification_token, verification_expires_at
      FROM subscriptions 
      WHERE email = ${emailLower}
    `;

    let subscriptionId: number;
    let unsubscribeToken: string;
    let verificationToken: string | null = null;
    let verificationExpiresAt: string | null = null;
    const isNewSubscriber = existingResult.rows.length === 0;
    const isVerified = !isNewSubscriber && existingResult.rows[0].verified_at !== null;

    if (!isVerified) {
      verificationToken = generateVerificationToken();
      verificationExpiresAt = getVerificationExpiry();
    }

    if (existingResult.rows.length > 0) {
      // Update existing subscription (preserve unsubscribe_token, refresh verification token if not verified)
      subscriptionId = existingResult.rows[0].id;
      unsubscribeToken = existingResult.rows[0].unsubscribe_token;

      await sql`
        UPDATE subscriptions 
        SET 
          verification_token = ${verificationToken},
          verification_expires_at = ${verificationExpiresAt},
          updated_at = NOW()
        WHERE id = ${subscriptionId}
      `;

      // Update preferences
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
    } else {
      // Create new subscription
      unsubscribeToken = generateUnsubscribeToken();
      verificationToken = verificationToken || generateVerificationToken();
      verificationExpiresAt = verificationExpiresAt || getVerificationExpiry();

      const insertResult = await sql`
        INSERT INTO subscriptions (email, unsubscribe_token, verification_token, verification_expires_at, created_at, updated_at)
        VALUES (${emailLower}, ${unsubscribeToken}, ${verificationToken}, ${verificationExpiresAt}, NOW(), NOW())
        RETURNING id
      `;
      subscriptionId = insertResult.rows[0].id;

      // Create preferences
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
      `;
    }

    // Send verification email if not verified yet and we have a token
    if (!isVerified && verificationToken) {
      void sendVerificationEmail(emailLower, verificationToken);
    }

    return NextResponse.json({
      ok: true,
      status: 'created_or_updated',
      email: emailLower,
      subscription_id: subscriptionId,
      verification_required: !isVerified,
      message: !isVerified
        ? 'Subscription saved. Please verify your email via the link we sent you.'
        : 'Subscription saved successfully.',
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
    const { email, token } = body;

    // Can unsubscribe by email or by token
    if (token) {
      // Unsubscribe by token (from email link) - delete the subscription entirely
      const result = await sql`
        DELETE FROM subscriptions 
        WHERE unsubscribe_token = ${token}
        RETURNING email
      `;

      if (result.rows.length === 0) {
        return NextResponse.json(
          { ok: false, error: 'Invalid unsubscribe token' },
          { status: 400 }
        );
      }

      return NextResponse.json({
        ok: true,
        status: 'unsubscribed',
        email: result.rows[0].email,
        message: 'You have been successfully unsubscribed.',
      });
    }

    // Unsubscribe by email - delete the subscription entirely
    if (!email || typeof email !== 'string' || !isValidEmail(email)) {
      return NextResponse.json(
        { ok: false, error: 'Invalid email address' },
        { status: 400 }
      );
    }

    const emailLower = email.toLowerCase().trim();

    await sql`
      DELETE FROM subscriptions 
      WHERE email = ${emailLower}
    `;

    return NextResponse.json({
      ok: true,
      status: 'unsubscribed',
      email: emailLower,
      message: 'You have been successfully unsubscribed.',
    });
  } catch (error) {
    console.error('Unsubscribe error:', error);
    return NextResponse.json(
      { ok: false, error: 'Server error. Please try again later.' },
      { status: 500 }
    );
  }
}
