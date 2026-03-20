import { useEffect, useState } from 'react';
import { Loader2, ExternalLink, MessageCircle, Sparkles } from 'lucide-react';
import { fetchEmbeds } from '../lib/api';

const EXAMPLES = [
  'What is the solvency ratio for Q3 2025?',
  'Which line of business has the highest combined ratio?',
  'Show me the asset allocation breakdown by CIC category',
  'Compare gross written premium across all quarters',
  'What are the top 5 issuer countries by SII value?',
  'How much is the market risk SCR charge?',
  'What is the reinsurance cession rate for motor liability?',
  'Show own funds by tier for the latest quarter',
];

export default function Genie() {
  const [url, setUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchEmbeds()
      .then((e) => setUrl(e.genie_url.replace('/embed/genie/rooms/', '/genie/rooms/')))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
      </div>
    );
  }

  return (
    <div className="max-w-4xl mx-auto p-6 space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Ask Genie</h2>
        <p className="text-sm text-gray-500 mt-1">
          Natural language questions about Bricksurance SE QRT data
        </p>
      </div>

      {url && (
        <a
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center justify-between w-full p-5 bg-violet-600 text-white rounded-lg hover:bg-violet-700 transition-colors group"
        >
          <div>
            <div className="text-lg font-semibold">Open Genie</div>
            <div className="text-sm text-violet-200 mt-0.5">Ask questions about QRT data in a new tab</div>
          </div>
          <ExternalLink className="w-6 h-6 text-violet-200 group-hover:text-white transition-colors" />
        </a>
      )}

      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center gap-3 mb-4">
          <div className="p-2 rounded-lg bg-violet-50 text-violet-600">
            <Sparkles className="w-5 h-5" />
          </div>
          <h3 className="font-semibold text-gray-900">Try These Questions</h3>
        </div>
        <div className="grid gap-2 sm:grid-cols-2">
          {EXAMPLES.map((q) => (
            <a
              key={q}
              href={url || '#'}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-start gap-2.5 p-3 rounded-lg border border-gray-100 hover:border-violet-200 hover:bg-violet-50/50 transition-colors group"
            >
              <MessageCircle className="w-4 h-4 text-gray-400 group-hover:text-violet-500 mt-0.5 shrink-0" />
              <span className="text-sm text-gray-700 group-hover:text-violet-700">{q}</span>
            </a>
          ))}
        </div>
      </div>
      <p className="text-xs text-gray-400 italic">
        Genie spaces can be embedded directly in apps served from the workspace domain.
        For portability, this demo links to the Genie room in a new tab.
      </p>
    </div>
  );
}
