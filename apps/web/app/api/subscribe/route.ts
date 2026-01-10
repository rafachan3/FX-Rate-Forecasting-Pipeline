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
    const { email, frequency, consent } = body;

    // Validate email
    if (!email || typeof email !== 'string' || !isValidEmail(email)) {
      return NextResponse.json(
        { ok: false, error: 'Invalid email address' },
        { status: 400 }
      );
    }

    // Validate consent
    if (!consent || consent !== true) {
      return NextResponse.json(
        { ok: false, error: 'Consent checkbox must be checked' },
        { status: 400 }
      );
    }

    // Validate frequency (default to 'WED' if not provided)
    const validFrequency = frequency || 'WED';
    if (!['WED', 'FRI'].includes(validFrequency)) {
      return NextResponse.json(
        { ok: false, error: 'Invalid frequency' },
        { status: 400 }
      );
    }

    // Read existing subscribers
    const subscribers = await readSubscribers();

    // Check if email already exists
    const existingSubscriber = subscribers.find((s) => s.email === email);
    if (existingSubscriber) {
      // Update existing subscriber
      existingSubscriber.frequency = validFrequency;
      existingSubscriber.status = 'active';
      existingSubscriber.created_at = new Date().toISOString();
    } else {
      // Create new subscriber
      const newSubscriber: Subscriber = {
        id: randomUUID(),
        email,
        pair: 'USDCAD',
        frequency: validFrequency,
        consent: true,
        status: 'active',
        created_at: new Date().toISOString(),
        unsubscribe_token: randomUUID(),
      };
      subscribers.push(newSubscriber);
    }

    // Write back to file
    await writeSubscribers(subscribers);

    return NextResponse.json({ ok: true });
  } catch (error) {
    console.error('Subscription error:', error);
    return NextResponse.json(
      { ok: false, error: 'Server error. Please try again later.' },
      { status: 500 }
    );
  }
}

