import { useEffect, useState } from 'react';
import { Loader2, ExternalLink, BarChart3, PieChart, TrendingUp, Shield } from 'lucide-react';
import { fetchEmbeds } from '../lib/api';

const TABS = [
  { icon: TrendingUp, label: 'Overview', desc: 'Solvency ratio KPIs, SCR vs Own Funds trend, balance sheet' },
  { icon: PieChart, label: 'S.06.02 — Assets', desc: 'CIC allocation, credit quality heatmap, duration, country exposure' },
  { icon: BarChart3, label: 'S.05.01 — P&L', desc: 'Combined ratio heatmap, GWP by LoB, loss/expense ratios, RI cession' },
  { icon: Shield, label: 'S.25.01 — SCR', desc: 'Risk module breakdown, market & NL sub-modules, own funds by tier' },
];

export default function Dashboard() {
  const [url, setUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchEmbeds()
      .then((e) => setUrl(e.dashboard_url.replace('/embed/dashboardsv3/', '/dashboardsv3/')))
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
        <h2 className="text-2xl font-bold text-gray-900">QRT Comparison Dashboard</h2>
        <p className="text-sm text-gray-500 mt-1">Quarterly comparison of regulatory templates across Q1–Q3 2025</p>
      </div>

      {url && (
        <a
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center justify-between w-full p-5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors group"
        >
          <div>
            <div className="text-lg font-semibold">Open Dashboard</div>
            <div className="text-sm text-blue-200 mt-0.5">Opens the Lakeview dashboard in a new tab</div>
          </div>
          <ExternalLink className="w-6 h-6 text-blue-200 group-hover:text-white transition-colors" />
        </a>
      )}

      <div className="grid gap-4 sm:grid-cols-2">
        {TABS.map((t) => (
          <div key={t.label} className="bg-white rounded-lg border border-gray-200 p-5 flex items-start gap-4">
            <div className="p-2.5 rounded-lg bg-blue-50 text-blue-600 shrink-0">
              <t.icon className="w-5 h-5" />
            </div>
            <div>
              <h3 className="font-semibold text-gray-900 text-sm">{t.label}</h3>
              <p className="text-sm text-gray-500 mt-0.5">{t.desc}</p>
            </div>
          </div>
        ))}
      </div>
      <p className="text-xs text-gray-400 italic">
        Lakeview dashboards can be embedded directly in apps served from the workspace domain.
        For portability, this demo links to the published dashboard in a new tab.
      </p>
    </div>
  );
}
