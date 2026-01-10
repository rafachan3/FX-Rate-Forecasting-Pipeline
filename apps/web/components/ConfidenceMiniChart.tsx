'use client';

interface ConfidenceMiniChartProps {
  items: Array<{ confidence: number; direction: string }>;
}

export default function ConfidenceMiniChart({ items }: ConfidenceMiniChartProps) {
  // Filter out SIDEWAYS/ABSTAIN signals (confidence 0) for the distribution
  const confidentSignals = items.filter(item => {
    const confidencePercent = Math.round(item.confidence * 100);
    return confidencePercent > 0;
  });

  // If no confident signals, show helper text
  if (confidentSignals.length === 0) {
    return (
      <div className="mt-4 pt-4 border-t border-[#334155]">
        <p className="text-xs text-[#94A3B8]">No confident signals today.</p>
      </div>
    );
  }

  // Define buckets: 0-20, 20-40, 40-60, 60-80, 80-100
  const buckets = [
    { min: 0, max: 20, label: '0-20%' },
    { min: 20, max: 40, label: '20-40%' },
    { min: 40, max: 60, label: '40-60%' },
    { min: 60, max: 80, label: '60-80%' },
    { min: 80, max: 100, label: '80-100%' },
  ];

  // Count signals in each bucket
  const bucketCounts = buckets.map(bucket => {
    return confidentSignals.filter(item => {
      const confidencePercent = Math.round(item.confidence * 100);
      // Since we filtered out 0% confidence, all values are > 0
      // Buckets: 0-20 (1-20%), 20-40 (21-40%), 40-60 (41-60%), 60-80 (61-80%), 80-100 (81-100%)
      return confidencePercent > bucket.min && confidencePercent <= bucket.max;
    }).length;
  });

  const maxCount = Math.max(...bucketCounts, 1); // Avoid division by zero

  return (
    <div className="mt-4 pt-4 border-t border-[#334155]">
      <div className="space-y-2">
        <div className="flex items-center justify-between">
          <p className="text-xs font-medium text-[#94A3B8]">Confidence distribution</p>
          <p className="text-xs text-[#64748B]">{confidentSignals.length} signal{confidentSignals.length !== 1 ? 's' : ''}</p>
        </div>
        
        {/* Histogram bars */}
        <div className="flex items-end gap-1.5 h-12">
          {buckets.map((bucket, index) => {
            const count = bucketCounts[index];
            const height = maxCount > 0 ? (count / maxCount) * 100 : 0;
            
            return (
              <div key={index} className="flex-1 flex flex-col items-center gap-1">
                {/* Bar */}
                <div className="w-full flex items-end" style={{ height: '32px' }}>
                  <div
                    className="w-full bg-[#3b82f6] rounded-t transition-all duration-300 ease-out"
                    style={{
                      height: `${Math.max(height, count > 0 ? 8 : 0)}%`,
                      opacity: count > 0 ? 0.6 + (height / 100) * 0.4 : 0,
                      minHeight: count > 0 ? '4px' : '0',
                    }}
                    title={`${bucket.label}: ${count} signal${count !== 1 ? 's' : ''}`}
                  />
                </div>
                
                {/* Bucket label */}
                <span className="text-[10px] text-[#64748B] leading-none">{bucket.label}</span>
                
                {/* Count (if > 0) */}
                {count > 0 && (
                  <span className="text-[10px] text-[#94A3B8] font-medium leading-none">{count}</span>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
