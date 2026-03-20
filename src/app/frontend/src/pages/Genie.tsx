import { useEffect, useState } from 'react';
import { Loader2, ExternalLink, MessageCircle, Sparkles } from 'lucide-react';
import { fetchEmbeds } from '../lib/api';

const EXAMPLE_QUESTIONS = [
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
      .then((e) => {
        // Use the direct Genie URL (not embed)
        const host = e.genie_url.replace('/embed/genie/rooms/', '/genie/rooms/');
        setUrl(host);
      })
      .catch(() => setUrl(null))
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
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Ask Genie</h2>
          <p className="text-sm text-gray-500 mt-1">
            Ask natural language questions about Bricksurance SE QRT data
          </p>
        </div>
        {url && (
          <a
            href={url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-5 py-2.5 bg-violet-600 text-white rounded-lg hover:bg-violet-700 font-medium transition-colors"
          >
            Open Genie <ExternalLink className="w-4 h-4" />
          </a>
        )}
      </div>

      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <div className="flex items-center gap-3 mb-4">
          <div className="p-2 rounded-lg bg-violet-50 text-violet-600">
            <Sparkles className="w-5 h-5" />
          </div>
          <h3 className="font-semibold text-gray-900">Example Questions</h3>
        </div>
        <div className="grid gap-2 sm:grid-cols-2">
          {EXAMPLE_QUESTIONS.map((q) => (
            <a
              key={q}
              href={url || '#'}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-start gap-2 p-3 rounded-lg border border-gray-100 hover:border-violet-200 hover:bg-violet-50/50 transition-colors group"
            >
              <MessageCircle className="w-4 h-4 text-gray-400 group-hover:text-violet-500 mt-0.5 shrink-0" />
              <span className="text-sm text-gray-700 group-hover:text-violet-700">{q}</span>
            </a>
          ))}
        </div>
      </div>

      <div className="bg-gray-50 rounded-lg border border-gray-200 p-5">
        <h3 className="font-semibold text-gray-700 mb-2">Available Data</h3>
        <div className="grid gap-1 sm:grid-cols-3 text-sm text-gray-600">
          {['assets_enriched', 's0602_summary', 's0501_summary', 's0501_premiums_claims_expenses',
            's2501_scr_breakdown', 's2501_summary', 'own_funds', 'balance_sheet', 'risk_factors',
          ].map((t) => (
            <span key={t} className="font-mono text-xs bg-white px-2 py-1 rounded border border-gray-200">{t}</span>
          ))}
        </div>
      </div>

      {!url && (
        <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-3 text-amber-700 text-sm">
          Genie URL not available. Check that GENIE_SPACE_ID is configured in app.yaml.
        </div>
      )}
    </div>
  );
}
