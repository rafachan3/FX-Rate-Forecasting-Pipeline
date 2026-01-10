'use client';

import { useState, useEffect } from 'react';
import Image from 'next/image';
import HowItWorksModal from './HowItWorksModal';

export default function Nav() {
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [scrolled, setScrolled] = useState(false);

  useEffect(() => {
    const handleScroll = () => {
      setScrolled(window.scrollY > 10);
    };
    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  return (
    <>
      <nav className={`sticky top-0 z-40 transition-shadow duration-200 ${
        scrolled ? 'bg-[#0B1220] shadow-lg shadow-black/10' : 'bg-[#0B1220]'
      } border-b border-[#1e293b]/40`}>
        <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
          <div className="flex h-16 items-center justify-between">
            <div className="flex items-center gap-2.5">
              {/* Logo mark - standalone, no container */}
              <Image
                src="/brand/northbound-logo.png"
                alt="NorthBound"
                width={32}
                height={32}
                className="opacity-95 sm:w-9 sm:h-9"
                priority
                sizes="(max-width: 640px) 32px, 36px"
              />
              {/* Wordmark */}
              <div className="text-[15px] sm:text-[16px] font-semibold text-white tracking-[0.02em]">NorthBound</div>
            </div>
            <button
              onClick={() => setIsModalOpen(true)}
              className="text-sm text-[#94A3B8] hover:text-white transition-colors duration-150 ease-out focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-2 focus:ring-offset-[#0B1220] rounded px-2 py-1"
            >
              How it works
            </button>
          </div>
        </div>
      </nav>
      <HowItWorksModal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} />
    </>
  );
}
