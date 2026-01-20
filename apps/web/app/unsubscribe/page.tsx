'use client';

import { useSearchParams } from 'next/navigation';
import { useEffect, useState, Suspense } from 'react';

function UnsubscribeContent() {
  const searchParams = useSearchParams();
  const token = searchParams.get('token');
  
  const [status, setStatus] = useState<'loading' | 'success' | 'error' | 'invalid'>('loading');
  const [email, setEmail] = useState<string>('');
  const [errorMessage, setErrorMessage] = useState<string>('');

  useEffect(() => {
    if (!token) {
      setStatus('invalid');
      return;
    }

    const unsubscribe = async () => {
      try {
        const response = await fetch('/api/subscribe', {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token }),
        });

        const data = await response.json();

        if (data.ok) {
          setStatus('success');
          setEmail(data.email || '');
        } else {
          setStatus('error');
          setErrorMessage(data.error || 'Failed to unsubscribe');
        }
      } catch {
        setStatus('error');
        setErrorMessage('Network error. Please try again later.');
      }
    };

    unsubscribe();
  }, [token]);

  return (
    <div className="min-h-screen bg-slate-950 flex items-center justify-center p-4">
      <div className="max-w-md w-full">
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-8 text-center">
          {status === 'loading' && (
            <>
              <div className="w-12 h-12 border-4 border-teal-500 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
              <h1 className="text-xl font-semibold text-white mb-2">Processing...</h1>
              <p className="text-slate-400">Please wait while we process your request.</p>
            </>
          )}

          {status === 'success' && (
            <>
              <div className="w-16 h-16 bg-teal-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg className="w-8 h-8 text-teal-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              </div>
              <h1 className="text-xl font-semibold text-white mb-2">Unsubscribed Successfully</h1>
              <p className="text-slate-400 mb-4">
                {email ? `${email} has been` : 'You have been'} removed from our mailing list.
              </p>
              <p className="text-slate-500 text-sm">
                You will no longer receive FX signal emails from NorthBound FX.
              </p>
              <a 
                href="/"
                className="inline-block mt-6 px-6 py-2 bg-slate-800 hover:bg-slate-700 text-white rounded-lg transition-colors"
              >
                Return to Home
              </a>
            </>
          )}

          {status === 'error' && (
            <>
              <div className="w-16 h-16 bg-red-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg className="w-8 h-8 text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </div>
              <h1 className="text-xl font-semibold text-white mb-2">Unsubscribe Failed</h1>
              <p className="text-slate-400 mb-4">{errorMessage}</p>
              <p className="text-slate-500 text-sm">
                Please try again or contact support if the problem persists.
              </p>
              <a 
                href="/"
                className="inline-block mt-6 px-6 py-2 bg-slate-800 hover:bg-slate-700 text-white rounded-lg transition-colors"
              >
                Return to Home
              </a>
            </>
          )}

          {status === 'invalid' && (
            <>
              <div className="w-16 h-16 bg-amber-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
                <svg className="w-8 h-8 text-amber-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                </svg>
              </div>
              <h1 className="text-xl font-semibold text-white mb-2">Invalid Link</h1>
              <p className="text-slate-400 mb-4">
                This unsubscribe link is invalid or has expired.
              </p>
              <p className="text-slate-500 text-sm">
                Please use the unsubscribe link from a recent email.
              </p>
              <a 
                href="/"
                className="inline-block mt-6 px-6 py-2 bg-slate-800 hover:bg-slate-700 text-white rounded-lg transition-colors"
              >
                Return to Home
              </a>
            </>
          )}
        </div>

        <p className="text-center text-slate-600 text-sm mt-6">
          NorthBound FX - FX Signal Forecasting
        </p>
      </div>
    </div>
  );
}

export default function UnsubscribePage() {
  return (
    <Suspense fallback={
      <div className="min-h-screen bg-slate-950 flex items-center justify-center">
        <div className="w-12 h-12 border-4 border-teal-500 border-t-transparent rounded-full animate-spin" />
      </div>
    }>
      <UnsubscribeContent />
    </Suspense>
  );
}
