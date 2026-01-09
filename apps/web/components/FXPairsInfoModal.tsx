'use client';

interface FXPairsInfoModalProps {
  isOpen: boolean;
  onClose: () => void;
}

const FX_PAIRS_INFO: { code: string; label: string; description: string }[] = [
  { code: 'USDCAD', label: 'USD/CAD', description: 'US Dollar to Canadian Dollar' },
  { code: 'EURCAD', label: 'EUR/CAD', description: 'Euro to Canadian Dollar' },
  { code: 'GBPCAD', label: 'GBP/CAD', description: 'British Pound to Canadian Dollar' },
  { code: 'JPYCAD', label: 'JPY/CAD', description: 'Japanese Yen to Canadian Dollar' },
  { code: 'AUDCAD', label: 'AUD/CAD', description: 'Australian Dollar to Canadian Dollar' },
  { code: 'CHFCAD', label: 'CHF/CAD', description: 'Swiss Franc to Canadian Dollar' },
  { code: 'CNYCAD', label: 'CNY/CAD', description: 'Chinese Yuan to Canadian Dollar' },
  { code: 'HKDCAD', label: 'HKD/CAD', description: 'Hong Kong Dollar to Canadian Dollar' },
  { code: 'SGDCAD', label: 'SGD/CAD', description: 'Singapore Dollar to Canadian Dollar' },
  { code: 'NOKCAD', label: 'NOK/CAD', description: 'Norwegian Krone to Canadian Dollar' },
  { code: 'SEKCAD', label: 'SEK/CAD', description: 'Swedish Krona to Canadian Dollar' },
  { code: 'NZDCAD', label: 'NZD/CAD', description: 'New Zealand Dollar to Canadian Dollar' },
  { code: 'MXNCAD', label: 'MXN/CAD', description: 'Mexican Peso to Canadian Dollar' },
  { code: 'BRLCAD', label: 'BRL/CAD', description: 'Brazilian Real to Canadian Dollar' },
  { code: 'INRCAD', label: 'INR/CAD', description: 'Indian Rupee to Canadian Dollar' },
  { code: 'ZARCAD', label: 'ZAR/CAD', description: 'South African Rand to Canadian Dollar' },
  { code: 'KRWCAD', label: 'KRW/CAD', description: 'South Korean Won to Canadian Dollar' },
  { code: 'TWDCAD', label: 'TWD/CAD', description: 'Taiwan Dollar to Canadian Dollar' },
  { code: 'TRYCAD', label: 'TRY/CAD', description: 'Turkish Lira to Canadian Dollar' },
  { code: 'IDRCAD', label: 'IDR/CAD', description: 'Indonesian Rupiah to Canadian Dollar' },
  { code: 'PENCAD', label: 'PEN/CAD', description: 'Peruvian Sol to Canadian Dollar' },
  { code: 'RUBCAD', label: 'RUB/CAD', description: 'Russian Ruble to Canadian Dollar' },
  { code: 'SARCAD', label: 'SAR/CAD', description: 'Saudi Riyal to Canadian Dollar' },
];

export default function FXPairsInfoModal({ isOpen, onClose }: FXPairsInfoModalProps) {
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
        className="relative z-10 w-full max-w-2xl max-h-[80vh] rounded-lg border border-[#334155] bg-[#1e293b] shadow-xl overflow-hidden flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-[#334155]">
          <h2 className="text-xl font-semibold text-white">FX Pairs Explained</h2>
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
        
        {/* Content - Scrollable */}
        <div className="overflow-y-auto flex-1 p-6">
          <p className="text-sm text-[#94A3B8] mb-6 leading-relaxed">
            All exchange rates are quoted against the Canadian Dollar (CAD). For example, USD/CAD shows how many Canadian dollars one US dollar is worth.
          </p>
          
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {FX_PAIRS_INFO.map((pair) => (
              <div
                key={pair.code}
                className="flex items-start gap-3 p-3 rounded-md border border-[#334155] bg-[#111827] hover:bg-[#0f172a] transition-colors"
              >
                <div className="flex-shrink-0 w-20">
                  <span className="text-sm font-medium text-[#E5E7EB]">{pair.label}</span>
                </div>
                <div className="flex-1">
                  <p className="text-sm text-[#94A3B8]">{pair.description}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

