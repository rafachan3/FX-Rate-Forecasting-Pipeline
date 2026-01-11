/**
 * API client for NorthBound backend.
 * Handles retries, timeouts, and error formatting.
 */

declare const process: {
  env: {
    [key: string]: string | undefined;
  };
};

/**
 * API base URL from environment variable.
 * Falls back to localhost for local development.
 * Must be set in Vercel production environment variables.
 */
export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8000";

export interface HealthResponse {
  ok: boolean;
  service: string;
  env: string;
  time_utc: string;
  email_enabled: boolean;
}

export interface PredictionItem {
  pair: string;
  pair_label?: string;
  generated_at?: string;
  obs_date?: string | null;
  direction: "UP" | "DOWN" | "ABSTAIN" | "SIDEWAYS"; // API may return ABSTAIN, UI maps to SIDEWAYS
  confidence: number;
  model?: string;
  raw?: {
    p_up?: number;
  };
}

/**
 * Maps API direction values to UI display values.
 * API returns "ABSTAIN", UI displays "SIDEWAYS".
 */
export function mapDirection(direction: string): "UP" | "DOWN" | "SIDEWAYS" {
  if (direction === "ABSTAIN" || direction === "SIDEWAYS") {
    return "SIDEWAYS";
  }
  if (direction === "UP") {
    return "UP";
  }
  if (direction === "DOWN") {
    return "DOWN";
  }
  // Default fallback
  return "SIDEWAYS";
}

export interface LatestResponse {
  horizon: string;
  as_of_utc: string | null;
  run_date: string;
  timezone?: string;
  git_sha?: string;
  items: PredictionItem[];
}

export interface SubscriptionRequest {
  email: string;
  pairs: string[];
  frequency: "DAILY" | "WEEKLY" | "MONTHLY";
  weekly_day?: "MON" | "TUE" | "WED" | "THU" | "FRI";
  monthly_timing?: "FIRST_BUSINESS_DAY" | "LAST_BUSINESS_DAY";
}

export interface SubscriptionResponse {
  status: string;
  email: string;
  subscription_id: string;
  email_enabled: boolean;
  message: string;
}

export interface UnsubscribeRequest {
  email: string;
}

export interface UnsubscribeResponse {
  status: string;
  email: string;
}

export interface ApiError {
  code: string;
  message: string;
  request_id?: string;
}

export class ApiClientError extends Error {
  status?: number;
  code?: string;

  constructor(message: string, status?: number, code?: string) {
    super(message);
    this.name = "ApiClientError";
    this.status = status;
    this.code = code;
  }
}


/**
 * Fetch with timeout, retry, and error handling.
 */
async function apiFetch<T>(
  path: string,
  opts?: RequestInit & { timeoutMs?: number; retry?: number },
): Promise<T> {
  const timeoutMs = opts?.timeoutMs ?? 6000;
  const retry = opts?.retry ?? 1;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

  const fetchWithRetry = async (attempt: number): Promise<Response> => {
    try {
      // Use URL constructor to properly handle query params and path joining
      const url = new URL(path, API_BASE_URL);
      const response = await fetch(url.toString(), {
        ...opts,
        signal: controller.signal,
        headers: {
          "Content-Type": "application/json",
          ...opts?.headers,
        },
      });
      clearTimeout(timeoutId);
      
      // Retry on 5xx errors
      if (!response.ok && response.status >= 500 && attempt < retry) {
        await new Promise((resolve) => setTimeout(resolve, 300));
        return fetchWithRetry(attempt + 1);
      }
      
      return response;
    } catch (error) {
      clearTimeout(timeoutId);
      
      // Retry on network errors
      if (attempt < retry && (error instanceof TypeError || error instanceof DOMException)) {
        await new Promise((resolve) => setTimeout(resolve, 300));
        return fetchWithRetry(attempt + 1);
      }
      throw error;
    }
  };

  try {
    const response = await fetchWithRetry(0);

    if (!response.ok) {
      let errorDetail: ApiError;
      try {
        const errorData = await response.json();
        errorDetail = errorData.error || { code: "HTTP_ERROR", message: response.statusText };
      } catch {
        errorDetail = { code: "HTTP_ERROR", message: response.statusText };
      }

      throw new ApiClientError(
        errorDetail.message || `HTTP ${response.status}: ${response.statusText}`,
        response.status,
        errorDetail.code,
      );
    }

    return await response.json();
  } catch (error) {
    if (error instanceof ApiClientError) {
      throw error;
    }
    if (error instanceof TypeError || error instanceof DOMException) {
      const apiUrl = API_BASE_URL || "API endpoint";
      throw new ApiClientError(
        `Network error: Unable to reach API at ${apiUrl}. Check NEXT_PUBLIC_API_BASE_URL environment variable.`,
        undefined,
        "NETWORK_ERROR",
      );
    }
    throw new ApiClientError("Unexpected error", undefined, "UNKNOWN_ERROR");
  }
}

/**
 * Health check endpoint.
 */
export async function getHealth(): Promise<HealthResponse> {
  return apiFetch<HealthResponse>("/v1/health");
}

/**
 * Get latest predictions for pairs.
 */
export async function getLatestH7(pairs: string[]): Promise<LatestResponse> {
  // Use URL constructor to properly encode query parameters
  const url = new URL("/v1/predictions/h7/latest", API_BASE_URL);
  url.searchParams.set("pairs", pairs.join(","));
  return apiFetch<LatestResponse>(url.pathname + url.search);
}

/**
 * Create or update subscription.
 */
export async function createSubscription(
  payload: SubscriptionRequest,
): Promise<SubscriptionResponse> {
  return apiFetch<SubscriptionResponse>("/v1/subscriptions", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

/**
 * Unsubscribe email.
 */
export async function unsubscribe(email: string): Promise<UnsubscribeResponse> {
  return apiFetch<UnsubscribeResponse>("/v1/subscriptions/unsubscribe", {
    method: "POST",
    body: JSON.stringify({ email }),
  });
}

