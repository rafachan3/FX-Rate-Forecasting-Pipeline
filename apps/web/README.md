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

## Backend API Configuration

This app requires a backend API to fetch FX signals. Configure the API base URL via environment variable.

### Local Development

The app defaults to `http://127.0.0.1:8000` for local development. If your backend runs on a different port, set:

```bash
export NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8000
```

### Production (Vercel)

**Required:** Set the `NEXT_PUBLIC_API_BASE_URL` environment variable in your Vercel project settings:

1. Go to your Vercel project dashboard
2. Navigate to **Settings** → **Environment Variables**
3. Add a new variable:
   - **Name:** `NEXT_PUBLIC_API_BASE_URL`
   - **Value:** Your deployed backend API URL (e.g., `https://api.example.com`)
   - **Environment:** Production (and Preview if needed)
4. Redeploy your application

**Important:** Without this environment variable, production builds will fail to fetch signals and show "Network error: Unable to reach API" messages.

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
