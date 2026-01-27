'use client';

import { useSearchParams } from 'next/navigation';
import { useEffect, useState, Suspense } from 'react';

function VerifyContent() {
  const searchParams = useSearchParams();
  const token = searchParams.get('token');
  
  const [status, setStatus] = useState<'loading' | 'success' | 'already_verified' | 'error' | 'invalid'>('loading');
  const [email, setEmail] = useState<string>('');
  const [errorMessage, setErrorMessage] = useState<string>('');

  useEffect(() => {
    if (!token) {
      setStatus('invalid');
      return;
    }

    const verify = async () => {
      try {
        const response = await fetch(`/api/verify?token=${encodeURIComponent(token)}`, {
          method: 'GET',
          headers: { 'Content-Type': 'application/json' },
        });

        const data = await response.json();

        if (data.ok) {
          if (data.status === 'already_verified') {
            setStatus('already_verified');
          } else {
            setStatus('success');
          }
          setEmail(data.email || '');
        } else {
          setStatus('error');
          setErrorMessage(data.error || 'Failed to verify email');
        }
      } catch {
        setStatus('error');
        setErrorMessage('Network error. Please try again later.');
      }
    };

    verify();
  }, [token]);

  return (
    <div className="min-h-screen bg-[#0f172a] flex items-center justify-center p-4">
      <div className="max-w-md w-full">
        <div className="bg-[#111827] border border-[#334155] rounded-xl p-8 text-center">
          {status === 'loading' && (
            <>
              <div className="w-12 h-12 border-4 border-[#3b82f6] border-t-transparent rounded-full animate-spin mx-auto mb-4" />
              <h1 className="text-xl font-semibold text-[#E5E7EB] mb-2">Verifying...</h1>
              <p className="text-[#94A3B8]">Please wait while we verify your email.</p>
            </>
          )}

          {status === 'success' && (
            <>
              <div className="w-16 h-16 bg-[#22C55E]/20 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg className="w-8 h-8 text-[#22C55E]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              </div>
              <h1 className="text-xl font-semibold text-[#E5E7EB] mb-2">Email Verified</h1>
              <p className="text-[#94A3B8] mb-4">
                {email ? `${email} has been` : 'Your email has been'} verified successfully.
              </p>
              <p className="text-[#64748B] text-sm mb-6">
                You will now receive FX signal emails from NorthBound FX based on your preferences.
              </p>
              <a 
                href="/"
                className="inline-block px-6 py-2 bg-[#3b82f6] hover:bg-[#2563eb] text-white rounded-lg transition-colors"
              >
                Return to Home
              </a>
            </>
          )}

          {status === 'already_verified' && (
            <>
              <div className="w-16 h-16 bg-[#3b82f6]/20 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg className="w-8 h-8 text-[#3b82f6]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </div>
              <h1 className="text-xl font-semibold text-[#E5E7EB] mb-2">Already Verified</h1>
              <p className="text-[#94A3B8] mb-4">
                {email ? `${email} is already` : 'Your email is already'} verified.
              </p>
              <p className="text-[#64748B] text-sm mb-6">
                You're all set to receive FX signal emails from NorthBound FX.
              </p>
              <a 
                href="/"
                className="inline-block px-6 py-2 bg-[#3b82f6] hover:bg-[#2563eb] text-white rounded-lg transition-colors"
              >
                Return to Home
              </a>
            </>
          )}

          {status === 'error' && (
            <>
              <div className="w-16 h-16 bg-[#EF4444]/20 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg className="w-8 h-8 text-[#EF4444]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </div>
              <h1 className="text-xl font-semibold text-[#E5E7EB] mb-2">Verification Failed</h1>
              <p className="text-[#94A3B8] mb-4">{errorMessage}</p>
              <p className="text-[#64748B] text-sm mb-6">
                The verification link may be invalid or expired. Please try subscribing again or contact support if the problem persists.
              </p>
              <a 
                href="/"
                className="inline-block px-6 py-2 bg-[#334155] hover:bg-[#475569] text-white rounded-lg transition-colors"
              >
                Return to Home
              </a>
            </>
          )}

          {status === 'invalid' && (
            <>
              <div className="w-16 h-16 bg-[#F59E0B]/20 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg className="w-8 h-8 text-[#F59E0B]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
              </div>
              <h1 className="text-xl font-semibold text-[#E5E7EB] mb-2">Invalid Link</h1>
              <p className="text-[#94A3B8] mb-4">
                This verification link is invalid or missing a token.
              </p>
              <p className="text-[#64748B] text-sm mb-6">
                Please use the verification link from your email or try subscribing again.
              </p>
              <a 
                href="/"
                className="inline-block px-6 py-2 bg-[#334155] hover:bg-[#475569] text-white rounded-lg transition-colors"
              >
                Return to Home
              </a>
            </>
          )}
        </div>

        <p className="text-center text-[#64748B] text-sm mt-6">
          NorthBound FX - FX Signal Forecasting
        </p>
      </div>
    </div>
  );
}

export default function VerifyPage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-[#0f172a] flex items-center justify-center">
        <div className="w-12 h-12 border-4 border-[#3b82f6] border-t-transparent rounded-full animate-spin" />
      </div>
    }>
      <VerifyContent />
    </Suspense>
  );
}
