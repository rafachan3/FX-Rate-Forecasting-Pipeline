'use client';

import { useEffect, useRef, useState } from 'react';
import SignupForm from '@/components/SignupForm';
import Nav from '@/components/Nav';
import WhatYouReceive from '@/components/WhatYouReceive';
import SignalOverviewPanel from '@/components/SignalOverviewPanel';
import UnsubscribeModal from '@/components/UnsubscribeModal';

export default function Home() {
  const heroRef = useRef<HTMLDivElement>(null);
  const formRef = useRef<HTMLDivElement>(null);
  const rightRef = useRef<HTMLDivElement>(null);
  const mobileHeaderRef = useRef<HTMLDivElement>(null);
  const [isUnsubscribeModalOpen, setIsUnsubscribeModalOpen] = useState(false);

  useEffect(() => {
    // Subtle entrance animation
    const elements = [heroRef.current, formRef.current, rightRef.current, mobileHeaderRef.current].filter(Boolean);
    elements.forEach((el, index) => {
      if (el) {
        el.style.opacity = '0';
        el.style.transform = 'translateY(4px)';
        setTimeout(() => {
          el.style.transition = 'opacity 200ms ease-out, transform 200ms ease-out';
          el.style.opacity = '1';
          el.style.transform = 'translateY(0)';
        }, index * 40);
      }
    });
  }, []);

  return (
    <div className="min-h-screen bg-[#0f172a] relative">
      {/* Subtle radial glow behind hero */}
      <div className="absolute inset-0 pointer-events-none overflow-hidden">
        <div className="absolute top-0 left-0 w-[600px] h-[600px] bg-[#3b82f6] opacity-[0.02] rounded-full blur-3xl -translate-x-1/4 -translate-y-1/4" />
      </div>

      {/* Top Nav */}
      <Nav />

      {/* Main Content */}
      <section className="relative mx-auto max-w-6xl px-4 py-10 sm:px-6 sm:py-12 lg:px-8 lg:py-16">
        {/* Mobile: Stacked, centered */}
        <div className="lg:hidden space-y-8">
          {/* Mobile Context Header */}
          <div ref={mobileHeaderRef} className="max-w-md mx-auto space-y-2">
            <h1 className="text-2xl font-bold leading-tight text-[#E5E7EB]">
              FX research signals by email
            </h1>
            <p className="text-sm text-[#94A3B8] leading-relaxed">
              Probabilistic direction for major FX pairs
            </p>
          </div>
          <div ref={formRef} className="max-w-md mx-auto">
            <SignupForm />
          </div>
          <div className="max-w-md mx-auto space-y-6" ref={rightRef}>
            <SignalOverviewPanel pairs={['USD_CAD', 'EUR_CAD', 'GBP_CAD']} />
            <WhatYouReceive />
          </div>
        </div>

        {/* Desktop: Two-column, aligned layout */}
        <div className="hidden lg:grid lg:grid-cols-[1fr_1.15fr] lg:gap-8 xl:gap-10 lg:items-start">
          {/* Left Column: Headline + Form */}
          <div className="space-y-8">
            <div ref={heroRef} className="space-y-3">
              <h1 className="text-[42px] font-bold leading-tight text-[#E5E7EB]">
                Get probabilistic FX direction before the market moves.
              </h1>
              <p className="text-[18px] text-[#94A3B8] leading-relaxed">
                Research signals sent Mon–Fri at 5:30pm ET. 
              </p>
            </div>
            <div ref={formRef} className="pt-0">
              <SignupForm />
            </div>
          </div>

          {/* Right Column: Signal Overview + What You'll Receive */}
          <div className="space-y-6" ref={rightRef}>
            <SignalOverviewPanel pairs={['USD_CAD', 'EUR_CAD', 'GBP_CAD']} />
            <WhatYouReceive />
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="relative border-t border-[#334155] bg-[#0f172a]">
        <div className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
          <div className="text-xs text-[#94A3B8] flex items-center justify-between">
            <p>Free. Cancel anytime.</p>
            <button
              onClick={() => setIsUnsubscribeModalOpen(true)}
              className="text-[#3b82f6] hover:text-[#2563eb] transition-colors duration-150 ease-out underline focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-2 focus:ring-offset-[#0f172a] rounded"
            >
              Unsubscribe
            </button>
          </div>
        </div>
      </footer>

      <UnsubscribeModal isOpen={isUnsubscribeModalOpen} onClose={() => setIsUnsubscribeModalOpen(false)} />
    </div>
  );
}
