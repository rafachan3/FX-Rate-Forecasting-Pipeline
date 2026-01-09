'use client';

import { useState, useEffect, useRef } from 'react';
import FXPairsInfoModal from './FXPairsInfoModal';

// All FX pairs from Bank of Canada (against CAD)
type FXPair = 
  | 'AUDCAD' | 'BRLCAD' | 'CNYCAD' | 'EURCAD' | 'HKDCAD' | 'INRCAD' 
  | 'IDRCAD' | 'JPYCAD' | 'MXNCAD' | 'NZDCAD' | 'NOKCAD' | 'PENCAD' 
  | 'RUBCAD' | 'SARCAD' | 'SGDCAD' | 'ZARCAD' | 'KRWCAD' | 'SEKCAD' 
  | 'CHFCAD' | 'TWDCAD' | 'TRYCAD' | 'GBPCAD' | 'USDCAD';

const FX_PAIRS: { code: FXPair; label: string }[] = [
  { code: 'USDCAD', label: 'USD/CAD' },
  { code: 'EURCAD', label: 'EUR/CAD' },
  { code: 'GBPCAD', label: 'GBP/CAD' },
  { code: 'JPYCAD', label: 'JPY/CAD' },
  { code: 'AUDCAD', label: 'AUD/CAD' },
  { code: 'CHFCAD', label: 'CHF/CAD' },
  { code: 'CNYCAD', label: 'CNY/CAD' },
  { code: 'HKDCAD', label: 'HKD/CAD' },
  { code: 'SGDCAD', label: 'SGD/CAD' },
  { code: 'NOKCAD', label: 'NOK/CAD' },
  { code: 'SEKCAD', label: 'SEK/CAD' },
  { code: 'NZDCAD', label: 'NZD/CAD' },
  { code: 'MXNCAD', label: 'MXN/CAD' },
  { code: 'BRLCAD', label: 'BRL/CAD' },
  { code: 'INRCAD', label: 'INR/CAD' },
  { code: 'ZARCAD', label: 'ZAR/CAD' },
  { code: 'KRWCAD', label: 'KRW/CAD' },
  { code: 'TWDCAD', label: 'TWD/CAD' },
  { code: 'TRYCAD', label: 'TRY/CAD' },
  { code: 'IDRCAD', label: 'IDR/CAD' },
  { code: 'PENCAD', label: 'PEN/CAD' },
  { code: 'RUBCAD', label: 'RUB/CAD' },
  { code: 'SARCAD', label: 'SAR/CAD' },
];

export default function SignupForm() {
  const [email, setEmail] = useState('');
  const [step, setStep] = useState<1 | 2>(1);
  const [selectedPairs, setSelectedPairs] = useState<FXPair[]>(['USDCAD', 'EURCAD']);
  const [showPairSelector, setShowPairSelector] = useState(false);
  const [frequency, setFrequency] = useState<'daily' | 'weekly' | 'monthly'>('weekly');
  const [weeklyDay, setWeeklyDay] = useState<'mon' | 'tue' | 'wed' | 'thu' | 'fri'>('wed');
  const [monthlyRule, setMonthlyRule] = useState<'first_business_day' | 'last_business_day'>('first_business_day');
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [emailError, setEmailError] = useState<string | null>(null);
  const [isValidating, setIsValidating] = useState(false);
  const [isInfoModalOpen, setIsInfoModalOpen] = useState(false);
  const step2Ref = useRef<HTMLDivElement>(null);

  const validateEmail = (email: string): boolean => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  };

  const handleEmailChange = (value: string) => {
    setEmail(value);
    setEmailError(null);
    
    // Real-time validation
    if (value && !validateEmail(value)) {
      setIsValidating(true);
    } else {
      setIsValidating(false);
    }
  };

  const handleEmailSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setEmailError(null);
    
    if (!email) {
      setEmailError('Email is required');
      return;
    }
    
    if (!validateEmail(email)) {
      setEmailError('Please enter a valid email address');
      return;
    }
    
    setStep(2);
  };

  const togglePair = (pair: FXPair) => {
    setSelectedPairs(prev => 
      prev.includes(pair)
        ? prev.filter(p => p !== pair)
        : [...prev, pair]
    );
  };

  useEffect(() => {
    // Animate step 2 entrance
    if (step === 2 && step2Ref.current) {
      step2Ref.current.style.opacity = '0';
      step2Ref.current.style.transform = 'translateY(6px)';
      setTimeout(() => {
        step2Ref.current!.style.transition = 'opacity 200ms ease-out, transform 200ms ease-out';
        step2Ref.current!.style.opacity = '1';
        step2Ref.current!.style.transform = 'translateY(0)';
      }, 10);
    }
  }, [step]);

  const handleFinalSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setLoading(true);

    // Mock submission
    const formData = {
      email,
      pairs: selectedPairs,
      frequency,
      ...(frequency === 'weekly' && { weekly_day: weeklyDay }),
      ...(frequency === 'monthly' && { monthly_rule: monthlyRule }),
    };
    
    console.log('Form submission:', formData);
    
    setTimeout(() => {
      setLoading(false);
      setSuccess(true);
    }, 1500);
  };

  if (success) {
    return (
      <div className="rounded-lg border border-[#334155] bg-[#111827] p-8 transition-all duration-200">
        <div className="flex flex-col items-center gap-4 text-center">
          <div className="flex items-center justify-center w-16 h-16 rounded-full bg-[#22C55E]/10 border border-[#22C55E]/20">
            <svg
              className="h-8 w-8 text-[#22C55E]"
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
          </div>
          <div>
            <p className="text-lg font-semibold text-[#E5E7EB] mb-1">Check your email to confirm</p>
            <p className="text-sm text-[#94A3B8]">We&apos;ve sent a confirmation link to {email}</p>
          </div>
        </div>
      </div>
    );
  }

      // Step 1: Email only
  if (step === 1) {
    const isEmailValid = email && validateEmail(email);
    
    return (
      <>
        <form onSubmit={handleEmailSubmit} className="space-y-4">
        <div className="rounded-lg border border-[#334155] bg-[#111827] p-6 sm:p-8 transition-all duration-150 ease-out shadow-sm lg:hover:shadow-md lg:hover:shadow-[#3b82f6]/10 lg:hover:-translate-y-1">
          <div className="space-y-4">
            {/* Email Input */}
            <div>
              <label htmlFor="email" className="block text-xs font-medium text-[#94A3B8] mb-1.5">
                Email address
              </label>
              <input
                id="email"
                type="email"
                value={email}
                onChange={(e) => handleEmailChange(e.target.value)}
                onBlur={() => {
                  if (email && !validateEmail(email)) {
                    setEmailError('Please enter a valid email address');
                  }
                }}
                placeholder="you@example.com"
                required
                className={`w-full rounded-md border px-4 py-3 text-base text-[#E5E7EB] placeholder:text-[#6b7280] focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#111827] focus:shadow-[0_0_0_3px_rgba(59,130,246,0.15)] transition-all duration-150 ease-out ${
                  emailError
                    ? 'border-[#ef4444] bg-[#0f172a]'
                    : 'border-[#475569] bg-[#0f172a] focus:border-[#3b82f6]'
                }`}
                aria-invalid={emailError ? 'true' : 'false'}
                aria-describedby={emailError ? 'email-error' : undefined}
              />
              {emailError && (
                <p id="email-error" className="mt-1.5 text-sm text-[#ef4444]" role="alert">
                  {emailError}
                </p>
              )}
            </div>

            {/* Trust Text */}
            <p className="text-xs text-[#94A3B8]">
              No spam. Unsubscribe anytime.
            </p>

            {/* Submit Button */}
            <button
              type="submit"
              disabled={!isEmailValid}
              className="w-full rounded-md bg-[#3b82f6] px-6 py-3.5 text-base font-medium text-white transition-all duration-150 ease-out hover:bg-[#2563eb] focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-2 focus:ring-offset-[#111827] active:scale-[0.98] min-h-[48px] disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:bg-[#3b82f6]"
            >
              Continue
            </button>

            {/* Trust Text Below CTA */}
            <p className="text-xs text-center text-[#94A3B8]">
              Free. Cancel anytime.
            </p>

            {/* FX Pair Selector */}
            <div className="pt-2 border-t border-[#334155]">
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => setShowPairSelector(!showPairSelector)}
                    className="flex-1 flex items-center justify-between min-h-[48px] px-3 py-2.5 rounded-md border border-[#334155] bg-[#0f172a] text-sm font-semibold text-[#94A3B8] hover:text-[#E5E7EB] hover:bg-[#1e293b] hover:border-[#475569] transition-all duration-150 ease-out focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#111827]"
                  >
                    <span>FX pairs (optional)</span>
                    <svg
                      className={`w-4 h-4 transition-transform duration-150 ease-out ${showPairSelector ? 'rotate-180' : ''}`}
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                    </svg>
                  </button>
                  <button
                    type="button"
                    onClick={() => setIsInfoModalOpen(true)}
                    className="flex items-center justify-center w-10 h-10 rounded-md border border-[#334155] bg-[#0f172a] text-[#94A3B8] hover:text-[#E5E7EB] hover:bg-[#1e293b] hover:border-[#475569] transition-all duration-150 ease-out focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#111827]"
                    aria-label="Learn more about FX pairs"
                  >
                    <svg
                      className="w-4 h-4"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                      />
                    </svg>
                  </button>
                </div>
                <p className="text-xs text-[#94A3B8] px-1">
                  Start with USD/CAD and EUR/CAD. Add more anytime.
                </p>
                
                {/* Selected pairs chips */}
                {selectedPairs.length > 0 && (
                  <div className="flex flex-wrap gap-2">
                    {selectedPairs.map((pair) => {
                      const pairInfo = FX_PAIRS.find(p => p.code === pair);
                      return (
                        <span
                          key={pair}
                          className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-[#1e293b] border border-[#334155] text-xs text-[#E5E7EB]"
                        >
                          {pairInfo?.label}
                          <button
                            type="button"
                            onClick={() => togglePair(pair)}
                            className="hover:text-[#94A3B8] transition-colors"
                            aria-label={`Remove ${pairInfo?.label}`}
                          >
                            <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                            </svg>
                          </button>
                        </span>
                      );
                    })}
                  </div>
                )}
              </div>
              
              {showPairSelector && (
                <div className="mt-3 space-y-2 transition-all duration-200">
                  <div className="flex flex-wrap gap-2">
                    {FX_PAIRS.map((pair) => (
                      <label
                        key={pair.code}
                        className="flex items-center gap-2 px-3 py-2 rounded-md border border-[#334155] bg-[#0f172a] cursor-pointer hover:bg-[#1e293b] hover:border-[#475569] transition-all duration-150 ease-out min-h-[44px] focus-within:ring-2 focus-within:ring-[#3b82f6] focus-within:ring-offset-0"
                      >
                        <input
                          type="checkbox"
                          checked={selectedPairs.includes(pair.code)}
                          onChange={() => togglePair(pair.code)}
                          className="w-4 h-4 rounded border-[#334155] bg-[#0f172a] text-[#3b82f6] focus:ring-[#3b82f6] focus:ring-offset-0"
                        />
                        <span className="text-sm text-[#E5E7EB]">{pair.label}</span>
                      </label>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </form>
      <FXPairsInfoModal isOpen={isInfoModalOpen} onClose={() => setIsInfoModalOpen(false)} />
      </>
    );
  }

  // Step 2: Delivery preferences
  return (
    <>
    <form onSubmit={handleFinalSubmit} className="space-y-4">
      <div ref={step2Ref} className="rounded-lg border border-[#334155] bg-[#111827] p-6 sm:p-8 transition-all duration-150 ease-out shadow-sm lg:hover:shadow-md lg:hover:shadow-[#3b82f6]/10 lg:hover:-translate-y-1">
        <div className="space-y-6">
          {/* Email Display (read-only) */}
          <div>
            <label className="block text-xs font-medium text-[#94A3B8] mb-1.5">
              Email
            </label>
            <div className="rounded-md border border-[#334155] bg-[#0f172a] px-4 py-3 text-base text-[#E5E7EB]">
              {email}
            </div>
            <button
              type="button"
              onClick={() => setStep(1)}
              className="mt-1.5 text-xs text-[#3b82f6] hover:text-[#2563eb] transition-colors duration-150 ease-out focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-2 focus:ring-offset-[#111827] rounded"
            >
              Change email
            </button>
          </div>

          {/* FX Pair Selector */}
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <label className="block text-sm font-medium text-[#E5E7EB] mb-1.5">
                FX pairs
              </label>
              <button
                type="button"
                onClick={() => setIsInfoModalOpen(true)}
                className="flex items-center justify-center w-6 h-6 rounded-full text-[#94A3B8] hover:text-[#E5E7EB] hover:bg-[#1e293b] transition-all duration-150 ease-out focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#111827]"
                aria-label="Learn more about FX pairs"
              >
                <svg
                  className="w-4 h-4"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                  />
                </svg>
              </button>
            </div>
            <div className="flex flex-wrap gap-2">
              {FX_PAIRS.map((pair) => (
                <label
                  key={pair.code}
                  className="flex items-center gap-2 px-3 py-2 rounded-md border border-[#334155] bg-[#0f172a] cursor-pointer hover:bg-[#1e293b] hover:border-[#475569] transition-all duration-150 ease-out min-h-[44px] focus-within:ring-2 focus-within:ring-[#3b82f6] focus-within:ring-offset-0"
                >
                  <input
                    type="checkbox"
                    checked={selectedPairs.includes(pair.code)}
                    onChange={() => togglePair(pair.code)}
                    className="w-4 h-4 rounded border-[#334155] bg-[#0f172a] text-[#3b82f6] focus:ring-[#3b82f6] focus:ring-offset-0"
                  />
                  <span className="text-sm text-[#E5E7EB]">{pair.label}</span>
                </label>
              ))}
            </div>
            <p className="text-xs text-[#94A3B8]">
              You can change pairs anytime.
            </p>
          </div>

          {/* Delivery Schedule Selector */}
          <div className="space-y-3">
            <div>
              <label className="block text-sm font-medium text-[#E5E7EB] mb-1.5">
                Delivery frequency
              </label>
              <p className="text-xs text-[#94A3B8] leading-relaxed">
                {frequency === 'daily' && 'Sent every business day after the market close.'}
                {frequency === 'weekly' && 'Sent once per week after the final update.'}
                {frequency === 'monthly' && 'Sent on the first or last business day.'}
              </p>
            </div>
            
            {/* Frequency Selector */}
            <div 
              className="flex flex-col sm:flex-row rounded-lg border border-[#334155] bg-[#0f172a] p-1.5 gap-1.5"
              role="group"
              aria-label="Delivery frequency options"
            >
              <button
                type="button"
                onClick={() => setFrequency('daily')}
                className={`flex-1 rounded-md px-4 py-3.5 text-sm font-medium transition-all duration-150 ease-out min-h-[48px] flex items-center justify-center focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#0f172a] ${
                  frequency === 'daily'
                    ? 'bg-[#3b82f6] text-white'
                    : 'text-[#94A3B8] hover:text-[#E5E7EB] hover:bg-[#1e293b] active:bg-[#1e293b] active:scale-[0.98]'
                }`}
                aria-pressed={frequency === 'daily'}
                aria-label="Daily"
              >
                Daily
              </button>
              <button
                type="button"
                onClick={() => setFrequency('weekly')}
                className={`flex-1 rounded-md px-4 py-3.5 text-sm font-medium transition-all duration-150 ease-out min-h-[48px] flex items-center justify-center focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#0f172a] ${
                  frequency === 'weekly'
                    ? 'bg-[#3b82f6] text-white'
                    : 'text-[#94A3B8] hover:text-[#E5E7EB] hover:bg-[#1e293b] active:bg-[#1e293b] active:scale-[0.98]'
                }`}
                aria-pressed={frequency === 'weekly'}
                aria-label="Weekly"
              >
                Weekly
              </button>
              <button
                type="button"
                onClick={() => setFrequency('monthly')}
                className={`flex-1 rounded-md px-4 py-3.5 text-sm font-medium transition-all duration-150 ease-out min-h-[48px] flex items-center justify-center focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#0f172a] ${
                  frequency === 'monthly'
                    ? 'bg-[#3b82f6] text-white'
                    : 'text-[#94A3B8] hover:text-[#E5E7EB] hover:bg-[#1e293b] active:bg-[#1e293b] active:scale-[0.98]'
                }`}
                aria-pressed={frequency === 'monthly'}
                aria-label="Monthly"
              >
                Monthly
              </button>
            </div>

            {/* Weekly Day Picker */}
            {frequency === 'weekly' && (
              <div className="space-y-2.5">
                <label className="block text-xs font-medium text-[#94A3B8]">
                  Day of week
                </label>
                <div 
                  className="flex flex-wrap gap-2"
                  role="group"
                  aria-label="Weekday options"
                >
                  {(['mon', 'tue', 'wed', 'thu', 'fri'] as const).map((day) => (
                    <button
                      key={day}
                      type="button"
                      onClick={() => setWeeklyDay(day)}
                      className={`rounded-md px-4 py-2.5 text-sm font-medium transition-all duration-150 ease-out min-h-[48px] min-w-[80px] flex items-center justify-center focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#111827] ${
                        weeklyDay === day
                          ? 'bg-[#3b82f6] text-white'
                          : 'border border-[#334155] bg-[#0f172a] text-[#94A3B8] hover:text-[#E5E7EB] hover:bg-[#1e293b] hover:border-[#475569] active:bg-[#1e293b] active:scale-[0.98]'
                      }`}
                      aria-pressed={weeklyDay === day}
                      aria-label={day === 'mon' ? 'Monday' : day === 'tue' ? 'Tuesday' : day === 'wed' ? 'Wednesday' : day === 'thu' ? 'Thursday' : 'Friday'}
                    >
                      {day === 'mon' ? 'Mon' : day === 'tue' ? 'Tue' : day === 'wed' ? 'Wed' : day === 'thu' ? 'Thu' : 'Fri'}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {/* Monthly Rule Selector */}
            {frequency === 'monthly' && (
              <div className="space-y-2.5">
                <label className="block text-xs font-medium text-[#94A3B8]">
                  Timing
                </label>
                <div 
                  className="flex flex-col sm:flex-row gap-2"
                  role="group"
                  aria-label="Monthly delivery options"
                >
                  <button
                    type="button"
                    onClick={() => setMonthlyRule('first_business_day')}
                    className={`flex-1 rounded-md px-4 py-3 text-sm font-medium transition-all duration-150 ease-out min-h-[48px] flex items-center justify-center focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#111827] ${
                      monthlyRule === 'first_business_day'
                        ? 'bg-[#3b82f6] text-white'
                        : 'border border-[#334155] bg-[#0f172a] text-[#94A3B8] hover:text-[#E5E7EB] hover:bg-[#1e293b] hover:border-[#475569] active:bg-[#1e293b] active:scale-[0.98]'
                    }`}
                    aria-pressed={monthlyRule === 'first_business_day'}
                    aria-label="First business day"
                  >
                    First business day
                  </button>
                  <button
                    type="button"
                    onClick={() => setMonthlyRule('last_business_day')}
                    className={`flex-1 rounded-md px-4 py-3 text-sm font-medium transition-all duration-150 ease-out min-h-[48px] flex items-center justify-center focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#111827] ${
                      monthlyRule === 'last_business_day'
                        ? 'bg-[#3b82f6] text-white'
                        : 'border border-[#334155] bg-[#0f172a] text-[#94A3B8] hover:text-[#E5E7EB] hover:bg-[#1e293b] hover:border-[#475569] active:bg-[#1e293b] active:scale-[0.98]'
                    }`}
                    aria-pressed={monthlyRule === 'last_business_day'}
                    aria-label="Last business day"
                  >
                    Last business day
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Disclaimer */}
          <div className="pt-4 border-t border-[#334155]">
            <p className="text-xs text-[#94A3B8] leading-relaxed">
              Research-only. Not trading or financial advice.
            </p>
          </div>

          {/* Error Message */}
          {error && (
            <div className="rounded-md bg-[#7F1D1D]/30 border border-[#EF4444]/30 p-3 text-sm text-[#EF4444]" role="alert">
              {error}
            </div>
          )}

          {/* Submit Button */}
          <button
            type="submit"
            disabled={loading}
            className="w-full rounded-md bg-[#3b82f6] px-6 py-3.5 text-base font-medium text-white transition-all duration-150 ease-out hover:bg-[#2563eb] focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-2 focus:ring-offset-[#111827] disabled:cursor-not-allowed disabled:opacity-50 active:scale-[0.98] min-h-[48px]"
          >
            {loading ? (
              <span className="flex items-center justify-center gap-2">
                <svg
                  className="animate-spin h-5 w-5 text-white"
                  xmlns="http://www.w3.org/2000/svg"
                  fill="none"
                  viewBox="0 0 24 24"
                >
                  <circle
                    className="opacity-25"
                    cx="12"
                    cy="12"
                    r="10"
                    stroke="currentColor"
                    strokeWidth="4"
                  />
                  <path
                    className="opacity-75"
                    fill="currentColor"
                    d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                  />
                </svg>
                <span>Sending…</span>
              </span>
            ) : (
              'Start free updates'
            )}
          </button>
        </div>
      </div>
    </form>
    <FXPairsInfoModal isOpen={isInfoModalOpen} onClose={() => setIsInfoModalOpen(false)} />
    </>
  );
}
