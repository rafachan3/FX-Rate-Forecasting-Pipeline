-- Vercel Postgres schema for NorthBound FX subscriptions
-- Run this in Vercel SQL Editor after creating the database

-- Subscriptions table
CREATE TABLE IF NOT EXISTS subscriptions (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
  verified_at TIMESTAMP WITH TIME ZONE,
  unsubscribe_token VARCHAR(64) UNIQUE NOT NULL,
  verification_token VARCHAR(255),
  verification_expires_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_email ON subscriptions(email);
CREATE INDEX IF NOT EXISTS idx_subscriptions_unsubscribe_token ON subscriptions(unsubscribe_token);
CREATE INDEX IF NOT EXISTS idx_subscriptions_verification_token ON subscriptions(verification_token);

-- Subscription preferences table
CREATE TABLE IF NOT EXISTS subscription_preferences (
  subscription_id INTEGER PRIMARY KEY REFERENCES subscriptions(id) ON DELETE CASCADE,
  frequency VARCHAR(20) NOT NULL CHECK (frequency IN ('DAILY', 'WEEKLY', 'MONTHLY')),
  weekly_day VARCHAR(3) CHECK (weekly_day IS NULL OR weekly_day IN ('MON', 'TUE', 'WED', 'THU', 'FRI')),
  monthly_timing VARCHAR(20) CHECK (monthly_timing IS NULL OR monthly_timing IN ('FIRST_BUSINESS_DAY', 'LAST_BUSINESS_DAY')),
  pairs TEXT[] DEFAULT '{}' NOT NULL,
  timezone VARCHAR(50) DEFAULT 'America/Toronto' NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_subscription_preferences_subscription_id ON subscription_preferences(subscription_id);
