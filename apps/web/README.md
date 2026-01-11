This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## S3-backed API (Vercel Production)

The production deployment fetches latest FX signals directly from S3 using a Next.js API route. **No separate backend server is required.**

### Required Environment Variables (Vercel)

Set these in your Vercel project settings (**Settings** → **Environment Variables**):

1. **AWS_ACCESS_KEY_ID** - AWS access key with S3 read permissions
2. **AWS_SECRET_ACCESS_KEY** - AWS secret key
3. **AWS_REGION** - AWS region (default: `us-east-1`)
4. **S3_BUCKET** - S3 bucket name (default: `fx-rate-pipeline-dev`)
5. **S3_PREDICTIONS_LATEST_PREFIX** - S3 prefix for latest JSON files (default: `predictions/h7/latest/`)

### Local Development

For local development, you can either:

**Option A: Use AWS credentials**
```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=us-east-2
export S3_BUCKET=fx-rate-pipeline-dev
```

**Option B: Use Vercel CLI to pull environment variables**
```bash
vercel env pull .env.local
```

The Next.js API route at `/api/predictions/h7/latest` will fetch JSON files from S3:
- Format: `s3://{S3_BUCKET}/{S3_PREDICTIONS_LATEST_PREFIX}latest_{PAIR}_h7.json`
- Example: `s3://fx-rate-pipeline-dev/predictions/h7/latest/latest_USD_CAD_h7.json`

### Legacy Backend API (Optional)

If you have a separate backend API, you can still use it by setting `NEXT_PUBLIC_API_BASE_URL`. However, the S3-backed route is the default for production.

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
