import { useEffect, useState } from 'react';
import { Loader2, ExternalLink, BarChart3, PieChart, TrendingUp, Shield } from 'lucide-react';
import { fetchEmbeds } from '../lib/api';

export default function Dashboard() {
  const [url, setUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchEmbeds()
      .then((e) => {
        // Use the direct dashboard URL (not embed)
        const host = e.dashboard_url.replace('/embed/dashboardsv3/', '/dashboardsv3/');
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

  const tabs = [
    { icon: TrendingUp, label: 'Overview', desc: 'Solvency ratio KPIs, SCR vs Own Funds, balance sheet' },
    { icon: PieChart, label: 'S.06.02 — Assets', desc: 'CIC allocation, credit quality, duration, country exposure' },
    { icon: BarChart3, label: 'S.05.01 — P&L', desc: 'Combined ratios, GWP by LoB, loss/expense ratios, RI cession' },
    { icon: Shield, label: 'S.25.01 — SCR', desc: 'Risk modules, market & NL sub-modules, own funds, solvency trend' },
  ];

  return (
    <div className="max-w-4xl mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">QRT Comparison Dashboard</h2>
          <p className="text-sm text-gray-500 mt-1">Quarterly comparison across Q1–Q3 2025</p>
        </div>
        {url && (
          <a
            href={url}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-2 px-5 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-medium transition-colors"
          >
            Open Dashboard <ExternalLink className="w-4 h-4" />
          </a>
        )}
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        {tabs.map((t) => (
          <div key={t.label} className="bg-white rounded-lg border border-gray-200 p-5 flex items-start gap-4">
            <div className="p-2.5 rounded-lg bg-blue-50 text-blue-600">
              <t.icon className="w-6 h-6" />
            </div>
            <div>
              <h3 className="font-semibold text-gray-900">{t.label}</h3>
              <p className="text-sm text-gray-500 mt-1">{t.desc}</p>
            </div>
          </div>
        ))}
      </div>

      {!url && (
        <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-3 text-amber-700 text-sm">
          Dashboard URL not available. Check that DASHBOARD_ID is configured in app.yaml.
        </div>
      )}
    </div>
  );
}
