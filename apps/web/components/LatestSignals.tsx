'use client';

import { useEffect, useState } from 'react';
import { ApiClientError, getLatestH7, PredictionItem } from '@/lib/api';
import SignalBar from './SignalBar';

interface LatestSignalsProps {
  pairs?: string[];
}

export default function LatestSignals({ pairs = ['USD_CAD', 'EUR_CAD', 'GBP_CAD'] }: LatestSignalsProps) {
  const [signals, setSignals] = useState<PredictionItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);

  const fetchSignals = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await getLatestH7(pairs);
      setSignals(response.items);
      setLastUpdated(response.as_of_utc || response.run_date);
    } catch (err) {
      if (err instanceof ApiClientError) {
        setError(err.message || 'Signals temporarily unavailable.');
      } else {
        setError('Signals temporarily unavailable.');
      }
      setSignals([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    // Initial fetch
    fetchSignals();

    // Set up interval for revalidation (60s)
    const interval = setInterval(() => {
      fetchSignals();
    }, 60000);

    return () => clearInterval(interval);
  }, [pairs.join(',')]); // Re-fetch if pairs change

  return (
    <div className="rounded-lg border border-[#334155] bg-[#111827] p-6 sm:p-8 transition-all duration-150 ease-out shadow-sm">
      <div className="space-y-6">
        <div>
          <h2 className="text-lg font-semibold text-[#E5E7EB] mb-1">Latest signals</h2>
          <p className="text-xs text-[#94A3B8]">Updated daily · Research-only</p>
        </div>

        {loading && signals.length === 0 && (
          <div className="space-y-3">
            {[1, 2, 3].map((i) => (
              <div key={i} className="flex items-center gap-4 py-3">
                <div className="h-4 bg-[#1e293b] rounded w-24 animate-pulse" />
                <div className="flex-1 h-2 bg-[#1e293b] rounded-full animate-pulse" />
                <div className="h-4 bg-[#1e293b] rounded w-16 animate-pulse" />
                <div className="h-4 bg-[#1e293b] rounded w-14 animate-pulse" />
              </div>
            ))}
          </div>
        )}

        {error && !loading && (
          <div className="space-y-3">
            <p className="text-sm text-[#94A3B8]">{error}</p>
            <button
              onClick={fetchSignals}
              className="text-sm text-[#3b82f6] hover:text-[#2563eb] transition-colors duration-150 ease-out focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-2 focus:ring-offset-[#111827] rounded px-3 py-1.5"
            >
              Retry
            </button>
          </div>
        )}

        {!loading && !error && signals.length > 0 && (
          <div className="space-y-0">
            {/* Signal Bars */}
            <div className="space-y-0">
              {signals.map((item) => (
                <SignalBar
                  key={item.pair}
                  pair={item.pair}
                  pairLabel={item.pair_label}
                  direction={item.direction}
                  confidence={item.confidence}
                />
              ))}
            </div>

            {lastUpdated && (
              <p className="text-xs text-[#94A3B8] mt-4 pt-3 border-t border-[#334155]">
                Last update: {lastUpdated}
              </p>
            )}
          </div>
        )}

        {!loading && !error && signals.length === 0 && (
          <p className="text-sm text-[#94A3B8]">No signals available.</p>
        )}
      </div>
    </div>
  );
}

