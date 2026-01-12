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
 * All API calls use same-origin Next.js API routes.
 * No external backend required.
 */

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
 * Uses same-origin Next.js API routes (no external backend).
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
      // Use relative path for same-origin Next.js API routes
      const response = await fetch(path, {
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
      throw new ApiClientError(
        `Network error: Unable to reach API endpoint.`,
        undefined,
        "NETWORK_ERROR",
      );
    }
    throw new ApiClientError("Unexpected error", undefined, "UNKNOWN_ERROR");
  }
}

/**
 * Health check endpoint.
 * Uses same-origin Next.js API route.
 */
export async function getHealth(): Promise<HealthResponse> {
  return apiFetch<HealthResponse>("/api/health");
}

/**
 * Get latest predictions for pairs.
 * Uses Next.js API route that fetches from S3 (no external backend required).
 */
export async function getLatestH7(pairs: string[]): Promise<LatestResponse> {
  // Use relative URL to call Next.js API route
  const url = new URL("/api/predictions/h7/latest", typeof window !== 'undefined' ? window.location.origin : 'http://localhost:3000');
  url.searchParams.set("pairs", pairs.join(","));
  
  // Use fetch with timeout and error handling
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 10000); // 10s timeout
  
  try {
    const response = await fetch(url.pathname + url.search, {
      headers: {
        "Content-Type": "application/json",
      },
      signal: controller.signal,
    });
    
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new ApiClientError(
        errorData.error || `HTTP ${response.status}: ${response.statusText}`,
        response.status,
        errorData.code || "HTTP_ERROR",
      );
    }
    
    const data = await response.json();
    
    // Transform response to match LatestResponse interface
    if (data.ok && data.items) {
      return {
        horizon: data.horizon || "h7",
        as_of_utc: data.as_of_utc || null,
        run_date: data.run_date || new Date().toISOString().split('T')[0],
        items: data.items,
      };
    }
    
    throw new ApiClientError(
      data.error || "Invalid response format",
      response.status,
      "INVALID_RESPONSE",
    );
  } catch (error) {
    clearTimeout(timeoutId);
    
    if (error instanceof ApiClientError) {
      throw error;
    }
    
    if (error instanceof TypeError || error instanceof DOMException) {
      throw new ApiClientError(
        "Network error: Unable to reach API",
        undefined,
        "NETWORK_ERROR",
      );
    }
    
    throw new ApiClientError(
      "Unexpected error while fetching predictions",
      undefined,
      "UNKNOWN_ERROR",
    );
  }
}

/**
 * Create or update subscription.
 * Uses same-origin Next.js API route.
 */
export async function createSubscription(
  payload: SubscriptionRequest,
): Promise<SubscriptionResponse> {
  return apiFetch<SubscriptionResponse>("/api/subscribe", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

/**
 * Unsubscribe email.
 * Uses same-origin Next.js API route.
 */
export async function unsubscribe(email: string): Promise<UnsubscribeResponse> {
  return apiFetch<UnsubscribeResponse>("/api/subscribe", {
    method: "DELETE",
    body: JSON.stringify({ email }),
  });
}

