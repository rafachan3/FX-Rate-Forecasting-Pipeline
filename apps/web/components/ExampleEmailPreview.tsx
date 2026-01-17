import { getDisplayDirection } from '@/lib/api';

export default function ExampleEmailPreview() {
  const signals = [
    { pair: 'EUR/CAD', direction: 'SIDEWAYS', confidence: 0.75 },
    { pair: 'USD/CAD', direction: 'UP', confidence: 0.82 },
    { pair: 'GBP/CAD', direction: 'DOWN', confidence: 0.68 },
  ];

  const getDirectionColor = (direction: string) => {
    switch (direction) {
      case 'UP':
        return 'text-[#22C55E]';
      case 'DOWN':
        return 'text-[#EF4444]';
      case 'SIDEWAYS':
        return 'text-[#94A3B8]';
      default:
        return 'text-[#E5E7EB]';
    }
  };

  return (
    <div className="mt-6 pt-4 border-t border-[#334155]">
      <div className="space-y-3">
        <div>
          <h3 className="text-sm font-semibold text-[#E5E7EB] mb-1">Example signal email</h3>
          <p className="text-xs text-[#94A3B8]">Dec 30 · 6:00pm ET</p>
        </div>
        
        <div className="space-y-2.5">
          {signals.map((signal, index) => (
            <div
              key={signal.pair}
              className={`pb-2.5 ${index < signals.length - 1 ? 'border-b border-[#334155]/50' : ''}`}
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1 space-y-1">
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-[#94A3B8]">Pair:</span>
                    <span className="text-xs font-medium text-[#E5E7EB]">{signal.pair}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-[#94A3B8]">Direction:</span>
                    <span className={`text-xs font-medium ${getDirectionColor(signal.direction)}`}>
                      {getDisplayDirection(signal.direction)}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-[#94A3B8]">Confidence:</span>
                    <span className="text-xs font-medium text-[#E5E7EB]">{signal.confidence}</span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
        
        <p className="text-xs text-[#94A3B8] italic mt-3">
          Format shown for illustration. Delivered by email only.
        </p>
      </div>
    </div>
  );
}

