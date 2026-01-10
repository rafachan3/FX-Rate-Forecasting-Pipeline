'use client';

interface ConfidenceTrendChartProps {
  series: Array<{ t: number; confidence: number }>;
  label: string;
}

export default function ConfidenceTrendChart({ series, label }: ConfidenceTrendChartProps) {
  if (series.length === 0) {
    return (
      <div className="mt-4 pt-4 border-t border-[#334155]">
        <p className="text-xs text-[#94A3B8]">No data available yet.</p>
      </div>
    );
  }

  // Chart dimensions
  const width = 100;
  const height = 60;
  const padding = { top: 8, right: 8, bottom: 16, left: 32 };
  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;

  // Convert confidence to percentage (0-100)
  const data = series.map(d => ({
    t: d.t,
    confidence: Math.round(d.confidence * 100),
  }));

  // Find min/max for scaling
  const minConfidence = Math.min(...data.map(d => d.confidence), 0);
  const maxConfidence = Math.max(...data.map(d => d.confidence), 100);
  const confidenceRange = maxConfidence - minConfidence || 100;

  // Scale function: map confidence (0-100) to chart Y coordinate
  const scaleY = (confidence: number) => {
    if (confidenceRange === 0) {
      // All values are the same - center the line
      return padding.top + chartHeight / 2;
    }
    const normalized = (confidence - minConfidence) / confidenceRange;
    return padding.top + chartHeight - (normalized * chartHeight);
  };

  // Scale function: map index to chart X coordinate
  const scaleX = (index: number) => {
    if (data.length === 1) return padding.left + chartWidth / 2;
    return padding.left + (index / (data.length - 1)) * chartWidth;
  };

  // Generate path for the line
  const pathData = data
    .map((d, i) => {
      const x = scaleX(i);
      const y = scaleY(d.confidence);
      return i === 0 ? `M ${x} ${y}` : `L ${x} ${y}`;
    })
    .join(' ');

  // Generate path for the area (closed path)
  const areaPath = [
    pathData,
    `L ${scaleX(data.length - 1)} ${padding.top + chartHeight}`,
    `L ${scaleX(0)} ${padding.top + chartHeight}`,
    'Z',
  ].join(' ');

  // Latest point
  const latestPoint = data[data.length - 1];
  const latestX = scaleX(data.length - 1);
  const latestY = scaleY(latestPoint.confidence);

  // Y-axis ticks
  const yTicks = [0, 25, 50, 75, 100].filter(
    tick => tick >= minConfidence && tick <= maxConfidence
  );

  return (
    <div className="mt-4 pt-4 border-t border-[#334155]">
      <div className="space-y-3">
        <div className="flex items-center justify-between">
          <div>
            <p className="text-xs font-medium text-[#94A3B8]">Live confidence trend</p>
            <p className="text-[10px] text-[#64748B] mt-0.5">{label}</p>
          </div>
          {latestPoint && (
            <div className="text-right">
              <p className="text-xs font-semibold text-[#E5E7EB]">{latestPoint.confidence}%</p>
              <p className="text-[10px] text-[#64748B]">latest</p>
            </div>
          )}
        </div>

        {/* SVG Chart */}
        <div className="relative">
          <svg
            viewBox={`0 0 ${width} ${height}`}
            className="w-full h-auto"
            style={{ minHeight: '120px' }}
            preserveAspectRatio="none"
          >
            {/* Gradient definition */}
            <defs>
              <linearGradient id={`gradient-${label.replace(/[^a-zA-Z0-9]/g, '-')}`} x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" stopColor="#3b82f6" stopOpacity="0.3" />
                <stop offset="100%" stopColor="#3b82f6" stopOpacity="0.05" />
              </linearGradient>
            </defs>

            {/* Grid lines */}
            {yTicks.map((tick, i) => {
              const y = scaleY(tick);
              return (
                <g key={i}>
                  <line
                    x1={padding.left}
                    y1={y}
                    x2={width - padding.right}
                    y2={y}
                    stroke="#334155"
                    strokeWidth="0.5"
                    strokeDasharray="2,2"
                    opacity="0.5"
                  />
                  {/* Y-axis label */}
                  <text
                    x={padding.left - 6}
                    y={y + 3}
                    fill="#64748B"
                    fontSize="8"
                    textAnchor="end"
                    alignmentBaseline="middle"
                  >
                    {tick}%
                  </text>
                </g>
              );
            })}

            {/* Area fill */}
            <path
              d={areaPath}
              fill={`url(#gradient-${label.replace(/[^a-zA-Z0-9]/g, '-')})`}
              opacity="0.6"
            />

            {/* Line */}
            <path
              d={pathData}
              fill="none"
              stroke="#3b82f6"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />

            {/* Latest point dot */}
            <circle
              cx={latestX}
              cy={latestY}
              r="3"
              fill="#3b82f6"
              stroke="#0f172a"
              strokeWidth="1.5"
            />
          </svg>
        </div>

        {/* X-axis info */}
        <div className="flex items-center justify-between text-[10px] text-[#64748B]">
          <span>{data.length} point{data.length !== 1 ? 's' : ''}</span>
          <span>Session history</span>
        </div>
      </div>
    </div>
  );
}
