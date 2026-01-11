/**
 * Database connection using Vercel Postgres.
 * Automatically uses POSTGRES_URL from Vercel environment.
 */
import { sql } from '@vercel/postgres';

export { sql };
