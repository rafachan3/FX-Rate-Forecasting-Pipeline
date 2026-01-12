import { NextRequest, NextResponse } from 'next/server';
import { promises as fs } from 'fs';
import { join } from 'path';
import { randomUUID } from 'crypto';

const SUBSCRIBERS_FILE = join(process.cwd(), '.local', 'subscribers.json');

interface Subscriber {
  id: string;
  email: string;
  pair: string;
  frequency: string;
  consent: boolean;
  status: string;
  created_at: string;
  unsubscribe_token: string;
}

async function ensureSubscribersFile() {
  const dir = join(process.cwd(), '.local');
  try {
    await fs.access(dir);
  } catch {
    await fs.mkdir(dir, { recursive: true });
  }
  
  try {
    await fs.access(SUBSCRIBERS_FILE);
  } catch {
    await fs.writeFile(SUBSCRIBERS_FILE, JSON.stringify([], null, 2));
  }
}

async function readSubscribers(): Promise<Subscriber[]> {
  await ensureSubscribersFile();
  const content = await fs.readFile(SUBSCRIBERS_FILE, 'utf-8');
  return JSON.parse(content);
}

async function writeSubscribers(subscribers: Subscriber[]) {
  await ensureSubscribersFile();
  await fs.writeFile(SUBSCRIBERS_FILE, JSON.stringify(subscribers, null, 2));
}

function isValidEmail(email: string): boolean {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { email, pairs, frequency, weekly_day, monthly_timing } = body;

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
    if (frequency === 'WEEKLY' && weekly_day) {
      const validDays = ['MON', 'TUE', 'WED', 'THU', 'FRI'];
      if (!validDays.includes(weekly_day)) {
        return NextResponse.json(
          { ok: false, error: `Weekly day must be one of: ${validDays.join(', ')}` },
          { status: 400 }
        );
      }
    }

    // Validate monthly_timing if frequency is MONTHLY
    if (frequency === 'MONTHLY' && monthly_timing) {
      const validTimings = ['FIRST_BUSINESS_DAY', 'LAST_BUSINESS_DAY'];
      if (!validTimings.includes(monthly_timing)) {
        return NextResponse.json(
          { ok: false, error: `Monthly timing must be one of: ${validTimings.join(', ')}` },
          { status: 400 }
        );
      }
    }

    // Read existing subscribers
    const subscribers = await readSubscribers();

    // Check if email already exists
    const existingSubscriber = subscribers.find((s) => s.email.toLowerCase() === email.toLowerCase());
    if (existingSubscriber) {
      // Update existing subscriber
      existingSubscriber.frequency = frequency;
      existingSubscriber.status = 'active';
      existingSubscriber.created_at = new Date().toISOString();
      // Update pairs (store as comma-separated string for compatibility)
      existingSubscriber.pair = pairs.join(',');
    } else {
      // Create new subscriber
      const newSubscriber: Subscriber = {
        id: randomUUID(),
        email: email.toLowerCase(),
        pair: pairs.join(','), // Store pairs as comma-separated string
        frequency,
        consent: true, // Implicit consent by submitting form
        status: 'active',
        created_at: new Date().toISOString(),
        unsubscribe_token: randomUUID(),
      };
      subscribers.push(newSubscriber);
    }

    // Write back to file
    await writeSubscribers(subscribers);

    return NextResponse.json({
      status: 'created_or_updated',
      email: email.toLowerCase(),
      subscription_id: existingSubscriber?.id || subscribers[subscribers.length - 1].id,
      email_enabled: false, // File-based storage doesn't support email yet
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

export async function DELETE(request: NextRequest) {
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

    // Read existing subscribers
    const subscribers = await readSubscribers();

    // Find and mark as inactive
    const subscriber = subscribers.find((s) => s.email.toLowerCase() === email.toLowerCase());
    if (subscriber) {
      subscriber.status = 'inactive';
      await writeSubscribers(subscribers);
      return NextResponse.json({
        status: 'unsubscribed',
        email: subscriber.email,
      });
    }

    // Email not found - return success for idempotency
    return NextResponse.json({
      status: 'unsubscribed',
      email: email.toLowerCase(),
    });
  } catch (error) {
    console.error('Unsubscribe error:', error);
    return NextResponse.json(
      { ok: false, error: 'Server error. Please try again later.' },
      { status: 500 }
    );
  }
}

