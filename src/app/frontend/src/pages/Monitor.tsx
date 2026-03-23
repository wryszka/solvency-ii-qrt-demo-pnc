import { useEffect, useState } from 'react';
import { Loader2, CheckCircle2, AlertTriangle, XCircle, Clock, Activity, ShieldCheck, ArrowRight } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import StatusBadge from '../components/StatusBadge';
import { fetchSlaStatus, fetchDqSummary, fetchReconciliation, formatEur, type Row } from '../lib/api';

export default function Monitor() {
  const [sla, setSla] = useState<Row[]>([]);
  const [dq, setDq] = useState<{ data: Row[]; aggregate: Row | null }>({ data: [], aggregate: null });
  const [recon, setRecon] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  useEffect(() => {
    Promise.all([
      fetchSlaStatus().then((r) => setSla(r.data)),
      fetchDqSummary().then(setDq),
      fetchReconciliation().then((r) => setRecon(r.data)),
    ])
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="max-w-5xl mx-auto p-6">
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-red-700">
          Failed to load monitoring data: {error}
        </div>
      </div>
    );
  }

  const period = sla[0]?.reporting_period || 'Latest';
  const feedsReceived = sla.filter((f) => f.status === 'on_time').length;
  const feedsLate = sla.filter((f) => f.status === 'late').length;
  const feedsMissing = sla.filter((f) => f.status === 'missing').length;
  const totalFeeds = sla.length;
  const allGreen = feedsLate === 0 && feedsMissing === 0;

  const agg = dq.aggregate;
  const passRate = agg?.overall_pass_rate || '100.0';
  const totalFailing = parseInt(agg?.total_failing || '0');
  const _failingChecks = parseInt(agg?.failing_expectations || '0'); void _failingChecks;

  const reconMatches = recon.filter((r) => r.status === 'MATCH').length;
  const reconTotal = recon.length;

  return (
    <div className="max-w-6xl mx-auto p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Control Tower</h2>
          <p className="text-sm text-gray-500 mt-1">
            {period} Reporting Cycle — Bricksurance SE
          </p>
        </div>
        <div className={`flex items-center gap-2 px-4 py-2 rounded-full text-sm font-semibold ${
          allGreen
            ? 'bg-green-50 text-green-700 border border-green-200'
            : 'bg-amber-50 text-amber-700 border border-amber-200'
        }`}>
          {allGreen ? <CheckCircle2 className="w-4 h-4" /> : <AlertTriangle className="w-4 h-4" />}
          {allGreen ? 'All Feeds Received' : `${feedsLate + feedsMissing} Issue${feedsLate + feedsMissing > 1 ? 's' : ''}`}
        </div>
      </div>

      {/* KPI Strip */}
      <div className="grid grid-cols-4 gap-4">
        <KpiCard
          icon={Activity}
          label="Feeds Received"
          value={`${feedsReceived}/${totalFeeds}`}
          color={allGreen ? 'green' : 'amber'}
        />
        <KpiCard
          icon={ShieldCheck}
          label="DQ Pass Rate"
          value={`${passRate}%`}
          color={parseFloat(String(passRate)) >= 99 ? 'green' : 'amber'}
          onClick={() => navigate('/data-quality')}
        />
        <KpiCard
          icon={CheckCircle2}
          label="Reconciliation"
          value={`${reconMatches}/${reconTotal} Match`}
          color={reconMatches === reconTotal ? 'green' : 'amber'}
        />
        <KpiCard
          icon={Clock}
          label="Quarantined Rows"
          value={String(totalFailing)}
          color={totalFailing === 0 ? 'green' : totalFailing < 50 ? 'amber' : 'red'}
          onClick={() => navigate('/data-quality')}
        />
      </div>

      {/* Feed Status Cards */}
      <div>
        <h3 className="text-lg font-semibold text-gray-900 mb-3">Data Feed Status</h3>
        <div className="grid gap-3">
          {sla.map((feed) => (
            <FeedCard key={feed.feed_name} feed={feed} />
          ))}
        </div>
      </div>

      {/* Reconciliation Summary */}
      <div>
        <h3 className="text-lg font-semibold text-gray-900 mb-3">Cross-QRT Reconciliation</h3>
        <div className="grid gap-3 sm:grid-cols-2">
          {recon.map((check) => (
            <ReconCard key={check.check_name} check={check} />
          ))}
        </div>
      </div>

      {/* Quick Actions */}
      <div className="flex items-center gap-3">
        <button
          onClick={() => navigate('/')}
          className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm font-medium"
        >
          View QRT Reports <ArrowRight className="w-4 h-4" />
        </button>
        <button
          onClick={() => navigate('/data-quality')}
          className="inline-flex items-center gap-2 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 text-sm font-medium"
        >
          DQ Dashboard <ArrowRight className="w-4 h-4" />
        </button>
      </div>
    </div>
  );
}

function KpiCard({ icon: Icon, label, value, color, onClick }: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  value: string;
  color: 'green' | 'amber' | 'red';
  onClick?: () => void;
}) {
  const colors = {
    green: 'bg-green-50 border-green-200 text-green-700',
    amber: 'bg-amber-50 border-amber-200 text-amber-700',
    red: 'bg-red-50 border-red-200 text-red-700',
  };
  const iconColors = {
    green: 'text-green-500',
    amber: 'text-amber-500',
    red: 'text-red-500',
  };

  return (
    <div
      className={`rounded-lg border p-4 ${colors[color]} ${onClick ? 'cursor-pointer hover:shadow-md transition-shadow' : ''}`}
      onClick={onClick}
    >
      <div className="flex items-center gap-2 mb-1">
        <Icon className={`w-4 h-4 ${iconColors[color]}`} />
        <span className="text-xs font-medium uppercase tracking-wide opacity-75">{label}</span>
      </div>
      <div className="text-2xl font-bold">{value}</div>
    </div>
  );
}

function FeedCard({ feed }: { feed: Row }) {
  const statusConfig: Record<string, { icon: typeof CheckCircle2; color: string; badge: 'success' | 'warning' | 'error' }> = {
    on_time: { icon: CheckCircle2, color: 'text-green-500', badge: 'success' },
    late: { icon: AlertTriangle, color: 'text-amber-500', badge: 'warning' },
    missing: { icon: XCircle, color: 'text-red-500', badge: 'error' },
  };
  const cfg = statusConfig[feed.status] || statusConfig.missing;
  const Icon = cfg.icon;

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4 flex items-center justify-between">
      <div className="flex items-center gap-4">
        <Icon className={`w-5 h-5 ${cfg.color}`} />
        <div>
          <div className="font-semibold text-gray-900 capitalize">{String(feed.feed_name).replace(/_/g, ' ')}</div>
          <div className="text-xs text-gray-500">{feed.source_system}</div>
        </div>
      </div>
      <div className="flex items-center gap-6 text-sm">
        <div className="text-right">
          <div className="text-gray-500 text-xs">Rows</div>
          <div className="font-mono text-gray-800">{parseInt(feed.row_count || '0').toLocaleString()}</div>
        </div>
        <div className="text-right">
          <div className="text-gray-500 text-xs">DQ Pass</div>
          <div className="font-mono text-gray-800">{(parseFloat(feed.dq_pass_rate || '1') * 100).toFixed(1)}%</div>
        </div>
        <div className="text-right min-w-[100px]">
          <div className="text-gray-500 text-xs">SLA</div>
          <StatusBadge
            label={feed.status === 'on_time' ? 'On Time' : feed.status === 'late' ? 'Late' : 'Missing'}
            variant={cfg.badge}
          />
        </div>
        <div className="text-right text-xs text-gray-400 min-w-[120px]">
          {feed.notes}
        </div>
      </div>
    </div>
  );
}

function ReconCard({ check }: { check: Row }) {
  const isMatch = check.status === 'MATCH';
  return (
    <div className={`rounded-lg border p-4 ${isMatch ? 'bg-white border-gray-200' : 'bg-red-50 border-red-200'}`}>
      <div className="flex items-center justify-between mb-2">
        <span className="text-sm font-semibold text-gray-900">{check.source_qrt} vs {check.target_qrt}</span>
        <StatusBadge
          label={check.status}
          variant={isMatch ? 'success' : 'error'}
        />
      </div>
      <p className="text-xs text-gray-500 mb-2">{check.check_description}</p>
      <div className="flex items-center gap-4 text-xs font-mono">
        <span>Source: {formatEur(check.source_value)}</span>
        <span>Target: {formatEur(check.target_value)}</span>
        <span className={isMatch ? 'text-green-600' : 'text-red-600'}>
          Diff: {formatEur(check.difference)}
        </span>
      </div>
    </div>
  );
}
