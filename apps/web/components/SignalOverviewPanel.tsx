'use client';

import { useEffect, useState, useCallback } from 'react';
import { ApiClientError, getLatestH7, PredictionItem, getDisplayDirection } from '@/lib/api';

interface SignalOverviewPanelProps {
  pairs?: string[];
}

interface SignalRow {
  pair: string;
  pairLabel: string;
  direction: 'UP' | 'DOWN' | 'SIDEWAYS';
  confidence: number;
  trendDots: Array<'UP' | 'DOWN' | 'SIDEWAYS'>; // Last 7 days
}

export default function SignalOverviewPanel({ pairs = ['USD_CAD', 'EUR_CAD', 'GBP_CAD'] }: SignalOverviewPanelProps) {
  const [signals, setSignals] = useState<SignalRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);

  const fetchSignals = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      // Request history to get last 7 days of data
      const response = await getLatestH7(pairs, true);
      
      // Transform PredictionItem to SignalRow
      const signalRows: SignalRow[] = response.items.map((item) => {
        const mappedDirection = item.direction === 'ABSTAIN' ? 'SIDEWAYS' : 
                                item.direction === 'UP' ? 'UP' : 
                                item.direction === 'DOWN' ? 'DOWN' : 'SIDEWAYS';
        
        // Use real history if available, otherwise fallback to repeating latest direction
        let trendDots: Array<'UP' | 'DOWN' | 'SIDEWAYS'>;
        if (item.history && item.history.length > 0) {
          // History comes sorted newest first, reverse to get oldest first for display
          const sortedHistory = [...item.history].reverse();
          
          // Take last 7 days from history (oldest to newest)
          const historyDirections = sortedHistory
            .slice(-7)
            .map(h => {
              const dir = h.direction === 'ABSTAIN' ? 'SIDEWAYS' : h.direction;
              return dir as 'UP' | 'DOWN' | 'SIDEWAYS';
            });
          
          // Pad to 7 if we have fewer than 7 days (pad at the beginning with latest direction)
          while (historyDirections.length < 7) {
            historyDirections.unshift(mappedDirection);
          }
          
          trendDots = historyDirections.slice(0, 7);
        } else {
          // Fallback: repeat latest direction for 7 dots
          // TODO: This fallback is used when API doesn't return history
          // Consider enhancing API to always return history when available
          trendDots = Array(7).fill(mappedDirection) as Array<'UP' | 'DOWN' | 'SIDEWAYS'>;
        }
        
        return {
          pair: item.pair,
          pairLabel: item.pair_label || item.pair.replace('_', '/'),
          direction: mappedDirection,
          confidence: item.confidence,
          trendDots,
        };
      });
      
      setSignals(signalRows);
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
  }, [pairs]);

  useEffect(() => {
    // Initial fetch
    fetchSignals();

    // Set up interval for revalidation (60s)
    const interval = setInterval(() => {
      fetchSignals();
    }, 60000);

    return () => clearInterval(interval);
  }, [fetchSignals]); // Re-fetch if fetchSignals changes (which depends on pairs)

  const getDirectionColor = (direction: 'UP' | 'DOWN' | 'SIDEWAYS') => {
    switch (direction) {
      case 'UP':
        return 'text-[#22C55E]'; // green
      case 'DOWN':
        return 'text-[#EF4444]'; // red
      case 'SIDEWAYS':
        return 'text-[#F59E0B]'; // amber
      default:
        return 'text-[#94A3B8]'; // gray
    }
  };

  const getDirectionBgColor = (direction: 'UP' | 'DOWN' | 'SIDEWAYS') => {
    switch (direction) {
      case 'UP':
        return 'bg-[#22C55E]'; // green
      case 'DOWN':
        return 'bg-[#EF4444]'; // red
      case 'SIDEWAYS':
        return 'bg-[#F59E0B]'; // amber
      default:
        return 'bg-[#94A3B8]'; // gray
    }
  };

  return (
    <div className="rounded-lg border border-[#334155] bg-[#111827] p-6 sm:p-8 transition-all duration-150 ease-out shadow-sm">
      <div className="space-y-6">
        {/* Header */}
        <div>
          <div className="flex items-center justify-between mb-1">
            <h2 className="text-lg font-semibold text-[#E5E7EB]">Signal Overview</h2>
            <span className="text-xs text-[#94A3B8]">Last 7 days</span>
          </div>
          <p className="text-xs text-[#94A3B8]">Updated daily · Research-only</p>
        </div>

        {/* Loading State */}
        {loading && signals.length === 0 && (
          <div className="space-y-4">
            {[1, 2, 3].map((i) => (
              <div key={i} className="flex items-center gap-4 py-3 border-b border-[#334155]/50 last:border-b-0">
                <div className="h-4 bg-[#1e293b] rounded w-20 animate-pulse" />
                <div className="flex gap-1">
                  {[1, 2, 3, 4, 5, 6, 7].map((j) => (
                    <div key={j} className="w-2 h-2 bg-[#1e293b] rounded-full animate-pulse" />
                  ))}
                </div>
                <div className="flex-1 h-2 bg-[#1e293b] rounded-full animate-pulse" />
                <div className="h-4 bg-[#1e293b] rounded w-16 animate-pulse" />
              </div>
            ))}
          </div>
        )}

        {/* Error State */}
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

        {/* Signal Rows */}
        {!loading && !error && signals.length > 0 && (
          <div className="space-y-0">
            {signals.map((signal, index) => {
              const confidencePercent = Math.round(signal.confidence * 100);
              const displayDirection = getDisplayDirection(signal.direction);
              
              return (
                <div
                  key={signal.pair}
                  className={`flex items-center gap-3 sm:gap-4 py-3 border-b border-[#334155]/50 last:border-b-0 ${
                    index === 0 ? 'pt-0' : ''
                  }`}
                >
                  {/* Pair Name */}
                  <div className="flex-shrink-0 w-16 sm:w-20 md:w-24">
                    <span className="text-sm font-medium text-[#E5E7EB] whitespace-nowrap">
                      {signal.pairLabel}
                    </span>
                  </div>

                  {/* Trend Dots (7 days) */}
                  <div className="flex gap-1.5 flex-shrink-0">
                    {signal.trendDots.map((dotDirection, dotIndex) => {
                      // Opacity gradient: oldest (faint, 0.3) to newest (solid, 1.0)
                      // dotIndex 0 = oldest, dotIndex 6 = newest
                      const opacity = 0.3 + (dotIndex / (signal.trendDots.length - 1)) * 0.7;
                      const color = dotDirection === 'UP' ? '#22C55E' : 
                                   dotDirection === 'DOWN' ? '#EF4444' : '#F59E0B';
                      
                      return (
                        <div
                          key={dotIndex}
                          className="w-2 h-2 rounded-full transition-opacity duration-200"
                          style={{
                            backgroundColor: color,
                            opacity: signal.trendDots.length === 1 ? 1 : opacity,
                          }}
                          title={`Day ${dotIndex + 1}: ${getDisplayDirection(dotDirection)}`}
                        />
                      );
                    })}
                  </div>

                  {/* Confidence Bar */}
                  <div className="flex-1 flex items-center gap-2 min-w-0 sm:min-w-[120px]">
                    <div className="flex-1 h-2 bg-[#1e293b] rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all duration-300 ${getDirectionBgColor(signal.direction)}`}
                        style={{ width: `${Math.max(confidencePercent, 4)}%` }}
                      />
                    </div>
                  </div>

                  {/* Direction Label */}
                  <div className="flex-shrink-0 w-16 sm:w-20 md:w-24 text-right">
                    <span className={`text-sm font-medium ${getDirectionColor(signal.direction)} whitespace-nowrap`}>
                      {displayDirection}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* Empty State */}
        {!loading && !error && signals.length === 0 && (
          <p className="text-sm text-[#94A3B8]">No signals available.</p>
        )}

        {/* Legend */}
        {!loading && !error && signals.length > 0 && (
          <div className="pt-4 border-t border-[#334155]">
            <div className="flex flex-wrap items-center gap-4 text-xs text-[#94A3B8]">
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-[#22C55E]" />
                <span>Bullish</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-[#EF4444]" />
                <span>Bearish</span>
              </div>
              <div className="flex items-center gap-2">
                <div className="w-2 h-2 rounded-full bg-[#F59E0B]" />
                <span>Sideways</span>
              </div>
            </div>
          </div>
        )}

        {/* Footer */}
        {lastUpdated && (
          <p className="text-xs text-[#94A3B8] pt-2 border-t border-[#334155]">
            Updated daily · Research-only
          </p>
        )}
      </div>
    </div>
  );
}
