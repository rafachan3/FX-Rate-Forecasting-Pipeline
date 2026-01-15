import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { randomUUID } from 'crypto';

function isValidEmail(email: string): boolean {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}

function generateUnsubscribeToken(): string {
  // Generate a secure random token (64 hex characters)
  return randomUUID().replace(/-/g, '') + randomUUID().replace(/-/g, '');
}

export async function POST(request: NextRequest) {
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
      SELECT id, unsubscribe_token FROM subscriptions 
      WHERE email = ${emailLower}
    `;

    let subscriptionId: number;
    let unsubscribeToken: string;

    if (existingResult.rows.length > 0) {
      // Update existing subscription
      subscriptionId = existingResult.rows[0].id;
      unsubscribeToken = existingResult.rows[0].unsubscribe_token;

      await sql`
        UPDATE subscriptions 
        SET verified_at = NOW()
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

      const insertResult = await sql`
        INSERT INTO subscriptions (email, unsubscribe_token, created_at)
        VALUES (${emailLower}, ${unsubscribeToken}, NOW())
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

    return NextResponse.json({
      ok: true,
      status: 'created_or_updated',
      email: emailLower,
      subscription_id: subscriptionId,
      message: 'Subscription saved successfully. You will start receiving FX signals based on your preferences.',
    });
  } catch (error) {
    console.error('Subscription error:', error);
    return NextResponse.json(
      { ok: false, error: 'Server error. Please try again later.' },
      { status: 500 }
    );
  }
}

export async function DELETE(request: NextRequest) {
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
