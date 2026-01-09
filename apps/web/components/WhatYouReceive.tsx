import ExampleEmailPreview from './ExampleEmailPreview';

export default function WhatYouReceive() {
  return (
    <div className="rounded-lg border border-[#334155] bg-[#1e293b] p-6 sm:p-8 transition-all duration-150 ease-out shadow-sm lg:hover:shadow-md lg:hover:shadow-[#3b82f6]/10 lg:hover:-translate-y-1">
      <div className="space-y-6">
        <div>
          <h2 className="text-lg font-semibold text-[#E5E7EB] mb-4">What you&apos;ll receive</h2>
          <div className="space-y-3 text-sm text-[#94A3B8] leading-snug">
            <div className="flex items-start gap-2.5">
              <svg
                className="w-4 h-4 text-[#3b82f6] mt-0.5 flex-shrink-0"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2.5}
                  d="M5 13l4 4L19 7"
                />
              </svg>
              <p>Directional signal (UP / DOWN / ABSTAIN)</p>
            </div>
            <div className="flex items-start gap-2.5">
              <svg
                className="w-4 h-4 text-[#3b82f6] mt-0.5 flex-shrink-0"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2.5}
                  d="M5 13l4 4L19 7"
                />
              </svg>
              <p>Model confidence level</p>
            </div>
            <div className="flex items-start gap-2.5">
              <svg
                className="w-4 h-4 text-[#3b82f6] mt-0.5 flex-shrink-0"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2.5}
                  d="M5 13l4 4L19 7"
                />
              </svg>
              <p>Email-only delivery</p>
            </div>
          </div>
          
          {/* Example Email Preview */}
          <ExampleEmailPreview />
          
          <div className="mt-6 pt-4 border-t border-[#334155]">
            <p className="text-xs text-[#94A3B8]">
              Last update: Dec 31, 5:30pm ET
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
