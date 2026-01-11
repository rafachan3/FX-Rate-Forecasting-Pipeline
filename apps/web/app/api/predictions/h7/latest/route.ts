import { NextRequest, NextResponse } from 'next/server';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';

// Environment variables with defaults
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';
const S3_BUCKET = process.env.S3_BUCKET || 'fx-rate-pipeline-dev';
const S3_PREDICTIONS_LATEST_PREFIX = process.env.S3_PREDICTIONS_LATEST_PREFIX || 'predictions/h7/latest/';

// Initialize S3 client
const s3Client = new S3Client({
  region: AWS_REGION,
  credentials: process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY
    ? {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      }
    : undefined, // Use IAM role if credentials not provided
});

interface LatestRow {
  obs_date: string;
  pair: string;
  p_up_logreg?: number | null;
  p_up_tree?: number | null;
  action_logreg?: string | null;
  action_tree?: string | null;
  decision?: string | null;
  confidence?: number | null;
}

interface LatestArtifact {
  sha: string;
  pair: string;
  horizon: string;
  generated_at: string;
  rows: LatestRow[];
}

/**
 * Map action to direction for UI compatibility.
 */
function mapActionToDirection(action: string | null | undefined): "UP" | "DOWN" | "SIDEWAYS" {
  if (!action) return "SIDEWAYS";
  const upper = action.toUpperCase();
  if (upper === "UP") return "UP";
  if (upper === "DOWN") return "DOWN";
  return "SIDEWAYS";
}

/**
 * Compute confidence from probability and threshold.
 */
function computeConfidence(pUp: number | null | undefined, threshold: number = 0.6): number {
  if (pUp === null || pUp === undefined) return 0;
  if (pUp >= threshold) return pUp;
  if (pUp <= 1 - threshold) return 1 - pUp;
  return 0; // SIDEWAYS
}

/**
 * Format pair label (e.g., "USD_CAD" -> "USD/CAD").
 */
function formatPairLabel(pair: string): string {
  return pair.replace(/_/g, '/');
}

/**
 * Fetch and parse a single latest JSON file from S3.
 */
async function fetchLatestJson(pair: string): Promise<LatestArtifact | null> {
  const key = `${S3_PREDICTIONS_LATEST_PREFIX}latest_${pair}_h7.json`;
  
  try {
    const command = new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
    });
    
    const response = await s3Client.send(command);
    
    if (!response.Body) {
      return null;
    }
    
    // Read and parse JSON
    const bodyString = await response.Body.transformToString();
    const data = JSON.parse(bodyString) as LatestArtifact;
    
    return data;
  } catch (error: any) {
    // Handle NoSuchKey or other S3 errors
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      return null;
    }
    throw error;
  }
}

/**
 * Transform LatestArtifact to PredictionItem format expected by frontend.
 */
function transformToPredictionItem(artifact: LatestArtifact): {
  pair: string;
  pair_label: string;
  generated_at: string;
  obs_date: string | null;
  direction: "UP" | "DOWN" | "SIDEWAYS";
  confidence: number;
  model: string;
  raw: { p_up?: number };
} | null {
  if (!artifact.rows || artifact.rows.length === 0) {
    return null;
  }
  
  // Get latest row by obs_date
  const sortedRows = [...artifact.rows].sort((a, b) => {
    const dateA = a.obs_date || '';
    const dateB = b.obs_date || '';
    return dateB.localeCompare(dateA);
  });
  
  const latestRow = sortedRows[0];
  
  // Use decision/confidence if available, otherwise derive from action_logreg/p_up_logreg
  let direction: "UP" | "DOWN" | "SIDEWAYS";
  let confidence: number;
  
  if (latestRow.decision !== null && latestRow.decision !== undefined) {
    direction = mapActionToDirection(latestRow.decision);
    confidence = latestRow.confidence ?? computeConfidence(latestRow.p_up_logreg);
  } else if (latestRow.action_logreg) {
    direction = mapActionToDirection(latestRow.action_logreg);
    confidence = computeConfidence(latestRow.p_up_logreg);
  } else {
    // Fallback: derive from probability
    const pUp = latestRow.p_up_logreg ?? 0.5;
    if (pUp >= 0.6) {
      direction = "UP";
      confidence = pUp;
    } else if (pUp <= 0.4) {
      direction = "DOWN";
      confidence = 1 - pUp;
    } else {
      direction = "SIDEWAYS";
      confidence = 0;
    }
  }
  
  return {
    pair: artifact.pair,
    pair_label: formatPairLabel(artifact.pair),
    generated_at: artifact.generated_at,
    obs_date: latestRow.obs_date || null,
    direction,
    confidence,
    model: "logreg",
    raw: {
      p_up: latestRow.p_up_logreg ?? undefined,
    },
  };
}

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const pairsParam = searchParams.get('pairs');
    
    if (!pairsParam) {
      return NextResponse.json(
        { ok: false, error: 'Missing required query parameter: pairs' },
        { status: 400 }
      );
    }
    
    const pairs = pairsParam.split(',').map(p => p.trim().toUpperCase().replace('/', '_'));
    
    if (pairs.length === 0) {
      return NextResponse.json(
        { ok: false, error: 'At least one pair must be provided' },
        { status: 400 }
      );
    }
    
    // Fetch all pairs in parallel
    const fetchPromises = pairs.map(async (pair) => {
      try {
        const artifact = await fetchLatestJson(pair);
        if (!artifact) {
          return { pair, data: null, error: 'File not found in S3' };
        }
        const item = transformToPredictionItem(artifact);
        return { pair, data: item, error: null };
      } catch (error: any) {
        return {
          pair,
          data: null,
          error: error.message || 'Failed to fetch from S3',
        };
      }
    });
    
    const results = await Promise.all(fetchPromises);
    
    // Separate successful and failed fetches
    const successful: Record<string, any> = {};
    const errors: Record<string, string> = {};
    
    results.forEach((result) => {
      if (result.data) {
        successful[result.pair] = result.data;
      } else {
        errors[result.pair] = result.error || 'Unknown error';
      }
    });
    
    // Determine response status
    const hasSuccess = Object.keys(successful).length > 0;
    const allFailed = Object.keys(successful).length === 0;
    
    if (allFailed) {
      return NextResponse.json(
        {
          ok: false,
          error: 'All pairs failed to load',
          errors,
        },
        { status: 502 }
      );
    }
    
    // Build response matching LatestResponse interface
    const items = Object.values(successful);
    const latestGeneratedAt = items.reduce((latest, item) => {
      if (!latest || !item.generated_at) return latest || item.generated_at;
      return item.generated_at > latest ? item.generated_at : latest;
    }, null as string | null);
    
    // Find latest obs_date
    const latestObsDate = items.reduce((latest, item) => {
      if (!latest || !item.obs_date) return latest || item.obs_date;
      return item.obs_date > latest ? item.obs_date : latest;
    }, null as string | null);
    
    const response = {
      ok: true,
      horizon: 'h7',
      as_of_utc: latestGeneratedAt,
      run_date: latestObsDate || new Date().toISOString().split('T')[0],
      items,
      ...(Object.keys(errors).length > 0 && { errors }),
    };
    
    return NextResponse.json(response);
  } catch (error: any) {
    console.error('API route error:', error);
    return NextResponse.json(
      {
        ok: false,
        error: error.message || 'Internal server error',
      },
      { status: 500 }
    );
  }
}
