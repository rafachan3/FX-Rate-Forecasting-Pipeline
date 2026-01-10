'use client';

import { mapDirection } from '@/lib/api';

interface SignalBarProps {
  pair: string;
  pairLabel?: string;
  direction: string; // API may return "ABSTAIN", UI maps to "SIDEWAYS"
  confidence: number;
}

export default function SignalBar({ pair, pairLabel, direction, confidence }: SignalBarProps) {
  const mappedDirection = mapDirection(direction);
  const confidencePercent = Math.round(confidence * 100);

  const getBarColor = () => {
    switch (mappedDirection) {
      case 'UP':
        return 'bg-[#22C55E]';
      case 'DOWN':
        return 'bg-[#EF4444]';
      case 'SIDEWAYS':
        return 'bg-[#64748B]';
      default:
        return 'bg-[#64748B]';
    }
  };

  const getDirectionLabel = () => {
    switch (mappedDirection) {
      case 'UP':
        return 'UP';
      case 'DOWN':
        return 'DOWN';
      case 'SIDEWAYS':
        return 'SIDEWAYS';
      default:
        return 'SIDEWAYS';
    }
  };

  const getDirectionColor = () => {
    switch (mappedDirection) {
      case 'UP':
        return 'text-[#22C55E]';
      case 'DOWN':
        return 'text-[#EF4444]';
      case 'SIDEWAYS':
        return 'text-[#94A3B8]';
      default:
        return 'text-[#94A3B8]';
    }
  };

  // Calculate bar position based on direction
  // LEFT = DOWN, CENTER = SIDEWAYS, RIGHT = UP
  const getBarStyle = () => {
    const minWidth = mappedDirection === 'SIDEWAYS' ? 20 : 8;
    const width = Math.max(confidencePercent, minWidth);
    
    switch (mappedDirection) {
      case 'UP':
        // Bar on the right
        return {
          width: `${width}%`,
          marginLeft: 'auto',
        };
      case 'DOWN':
        // Bar on the left
        return {
          width: `${width}%`,
          marginLeft: '0',
        };
      case 'SIDEWAYS':
        // Bar in the center
        return {
          width: `${width}%`,
          marginLeft: 'auto',
          marginRight: 'auto',
        };
      default:
        return {
          width: `${width}%`,
          marginLeft: 'auto',
          marginRight: 'auto',
        };
    }
  };

  return (
    <div className="flex items-center gap-4 py-3 border-b border-[#334155]/50 last:border-b-0">
      {/* Pair Label */}
      <div className="flex-shrink-0 w-24 sm:w-28">
        <span className="text-sm font-medium text-[#E5E7EB]">
          {pairLabel || pair.replace('_', '/')}
        </span>
      </div>

      {/* Signal Bar Container */}
      <div className="flex-1 flex items-center gap-3">
        <div className="flex-1 h-2 bg-[#1e293b] rounded-full overflow-hidden relative flex">
          <div
            className={`h-full ${getBarColor()} rounded-full transition-all duration-500 ease-out`}
            style={getBarStyle()}
          />
        </div>
        
        {/* Direction Label */}
        <div className="flex-shrink-0 w-20 sm:w-24 text-right">
          <span className={`text-sm font-medium ${getDirectionColor()}`}>
            {getDirectionLabel()}
          </span>
        </div>
      </div>

      {/* Confidence Percentage */}
      <div className="flex-shrink-0 w-14 text-right">
        <span className="text-sm text-[#E5E7EB] font-medium">
          {confidencePercent}%
        </span>
      </div>
    </div>
  );
}
