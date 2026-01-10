'use client';

import { useState } from 'react';
import { ApiClientError, unsubscribe as apiUnsubscribe } from '@/lib/api';

interface UnsubscribeModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function UnsubscribeModal({ isOpen, onClose }: UnsubscribeModalProps) {
  const [email, setEmail] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const validateEmail = (email: string): boolean => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!email) {
      setError('Email is required');
      return;
    }

    if (!validateEmail(email)) {
      setError('Please enter a valid email address');
      return;
    }

    setLoading(true);
    try {
      await apiUnsubscribe(email);
      setSuccess(true);
      setTimeout(() => {
        onClose();
        setEmail('');
        setSuccess(false);
        setError(null);
      }, 2000);
    } catch (err) {
      if (err instanceof ApiClientError) {
        setError(err.message || 'Failed to unsubscribe. Please try again.');
      } else {
        setError('Failed to unsubscribe. Please try again.');
      }
    } finally {
      setLoading(false);
    }
  };

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
          <h2 className="text-xl font-semibold text-white">Unsubscribe</h2>
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
        
        {success ? (
          <div className="space-y-4 text-center">
            <div className="flex items-center justify-center w-16 h-16 rounded-full bg-[#22C55E]/10 border border-[#22C55E]/20 mx-auto">
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
            <p className="text-[#E5E7EB]">You have been unsubscribed.</p>
          </div>
        ) : (
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label htmlFor="unsubscribe-email" className="block text-sm font-medium text-[#94A3B8] mb-1.5">
                Email address
              </label>
              <input
                id="unsubscribe-email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="you@example.com"
                required
                className={`w-full rounded-md border px-4 py-3 text-base text-[#E5E7EB] placeholder:text-[#6b7280] bg-[#0f172a] focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-0 focus:ring-offset-[#1e293b] transition-all duration-150 ease-out ${
                  error
                    ? 'border-[#ef4444]'
                    : 'border-[#475569] focus:border-[#3b82f6]'
                }`}
                aria-invalid={error ? 'true' : 'false'}
                aria-describedby={error ? 'unsubscribe-error' : undefined}
              />
              {error && (
                <p id="unsubscribe-error" className="mt-1.5 text-sm text-[#ef4444]" role="alert">
                  {error}
                </p>
              )}
            </div>

            <div className="flex gap-3">
              <button
                type="button"
                onClick={onClose}
                className="flex-1 rounded-md border border-[#334155] bg-[#0f172a] px-4 py-3 text-sm font-medium text-[#94A3B8] hover:text-[#E5E7EB] hover:bg-[#1e293b] transition-all duration-150 ease-out focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-2 focus:ring-offset-[#1e293b]"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={loading || !email || !validateEmail(email)}
                className="flex-1 rounded-md bg-[#3b82f6] px-4 py-3 text-sm font-medium text-white transition-all duration-150 ease-out hover:bg-[#2563eb] focus:outline-none focus:ring-2 focus:ring-[#3b82f6] focus:ring-offset-2 focus:ring-offset-[#1e293b] disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:bg-[#3b82f6]"
              >
                {loading ? 'Unsubscribing…' : 'Unsubscribe'}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

