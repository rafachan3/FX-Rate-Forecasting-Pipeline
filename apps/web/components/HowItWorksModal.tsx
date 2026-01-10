'use client';

interface HowItWorksModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function HowItWorksModal({ isOpen, onClose }: HowItWorksModalProps) {
  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      onClick={onClose}
    >
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/60" />
      
      {/* Modal */}
      <div
        className="relative z-10 w-full max-w-md rounded-lg border border-[#334155] bg-[#1e293b] p-6 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-xl font-semibold text-white">How it works</h2>
          <button
            onClick={onClose}
            className="text-[#94A3B8] hover:text-white transition-colors focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-2 focus:ring-offset-[#1e293b] rounded p-1"
            aria-label="Close"
          >
            <svg
              className="h-6 w-6"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M6 18L18 6M6 6l12 12"
              />
            </svg>
          </button>
        </div>
        
        <div className="space-y-4 text-[#94A3B8]">
          <p>
            Receive machine learning-based probabilistic research signals for major FX pairs against the Canadian Dollar.
          </p>
          <div className="space-y-3">
            <p className="font-medium text-[#E5E7EB]">Each signal includes a directional output:</p>
            <div className="space-y-2 pl-2 border-l-2 border-[#334155]">
              <p>
                <span className="text-[#22C55E] font-medium">UP</span> — The model predicts the exchange rate will increase (the first currency will strengthen against CAD).
              </p>
              <p>
                <span className="text-[#EF4444] font-medium">DOWN</span> — The model predicts the exchange rate will decrease (the first currency will weaken against CAD).
              </p>
              <p>
                <span className="text-[#94A3B8] font-medium">SIDEWAYS</span> — The model has low confidence and recommends no directional position.
              </p>
            </div>
          </div>
          <p>
            Signals are delivered via email only. This service does not provide trading or execution capabilities.
          </p>
        </div>
      </div>
    </div>
  );
}

