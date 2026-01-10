'use client';

import { useEffect, useState } from 'react';
import { ApiClientError, getLatestH7, PredictionItem } from '@/lib/api';

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

  const getDirectionColor = (direction: string) => {
    switch (direction) {
      case 'UP':
        return 'text-[#22C55E]';
      case 'DOWN':
        return 'text-[#EF4444]';
      case 'ABSTAIN':
        return 'text-[#94A3B8]';
      default:
        return 'text-[#94A3B8]';
    }
  };

  const getDirectionBadgeBg = (direction: string) => {
    switch (direction) {
      case 'UP':
        return 'bg-[#22C55E]/10 border-[#22C55E]/20 text-[#22C55E]';
      case 'DOWN':
        return 'bg-[#EF4444]/10 border-[#EF4444]/20 text-[#EF4444]';
      case 'ABSTAIN':
        return 'bg-[#94A3B8]/10 border-[#94A3B8]/20 text-[#94A3B8]';
      default:
        return 'bg-[#94A3B8]/10 border-[#94A3B8]/20 text-[#94A3B8]';
    }
  };

  return (
    <div className="rounded-lg border border-[#334155] bg-[#111827] p-6 sm:p-8 transition-all duration-150 ease-out shadow-sm">
      <div className="space-y-4">
        <div>
          <h2 className="text-lg font-semibold text-[#E5E7EB] mb-1">Latest signals</h2>
          <p className="text-xs text-[#94A3B8]">Updated daily · Research-only</p>
        </div>

        {loading && signals.length === 0 && (
          <div className="space-y-3">
            {[1, 2, 3].map((i) => (
              <div key={i} className="flex items-center justify-between py-2">
                <div className="h-4 bg-[#1e293b] rounded w-24 animate-pulse" />
                <div className="h-4 bg-[#1e293b] rounded w-16 animate-pulse" />
                <div className="h-4 bg-[#1e293b] rounded w-20 animate-pulse" />
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
            {/* Table header */}
            <div className="grid grid-cols-3 gap-4 pb-2 border-b border-[#334155] text-xs font-medium text-[#94A3B8]">
              <div>Pair</div>
              <div>Signal</div>
              <div>Confidence</div>
            </div>

            {/* Table rows */}
            <div className="space-y-0">
              {signals.map((item) => (
                <div
                  key={item.pair}
                  className="grid grid-cols-3 gap-4 py-3 border-b border-[#334155]/50 last:border-b-0"
                >
                  <div className="text-sm font-medium text-[#E5E7EB]">
                    {item.pair_label || item.pair.replace('_', '/')}
                  </div>
                  <div>
                    <span
                      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${getDirectionBadgeBg(item.direction)}`}
                    >
                      {item.direction}
                    </span>
                  </div>
                  <div className="text-sm text-[#E5E7EB]">
                    {Math.round(item.confidence * 100)}%
                  </div>
                </div>
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

