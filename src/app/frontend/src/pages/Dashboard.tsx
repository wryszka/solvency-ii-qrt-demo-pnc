import { useEffect, useState } from 'react';
import { Loader2, TrendingUp, Shield, Clock, CheckCircle2 } from 'lucide-react';
import MetricCard from '../components/MetricCard';
import StatusBadge from '../components/StatusBadge';
import { fetchDashboard, fetchCurrentApproval, formatEur, type DashboardData, type ApprovalRecord } from '../lib/api';

export default function Dashboard() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [approval, setApproval] = useState<ApprovalRecord | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([fetchDashboard(), fetchCurrentApproval().catch(() => null)])
      .then(([d, a]) => {
        setData(d);
        setApproval(a);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <p className="text-red-600 font-medium">Failed to load dashboard</p>
          <p className="text-sm text-gray-500 mt-1">{error}</p>
        </div>
      </div>
    );
  }

  // combined_ratio and solvency_ratio come as percentages (e.g. 95.2 = 95.2%)
  const cr = data.combined_ratio;
  const combinedRatioColor =
    cr == null ? 'gray' : cr < 100 ? 'green' : cr <= 110 ? 'amber' : 'red';

  const sr = data.solvency_ratio;
  const solvencyColor =
    sr == null ? 'gray' : sr >= 150 ? 'green' : sr >= 100 ? 'amber' : 'red';

  const dqPassRate =
    data.dq_checks_total > 0
      ? `${data.dq_checks_passed}/${data.dq_checks_total} (${((data.dq_checks_passed / data.dq_checks_total) * 100).toFixed(0)}%)`
      : '—';

  function approvalBadge() {
    if (!approval) return <StatusBadge label="Not Submitted" variant="neutral" />;
    switch (approval.status) {
      case 'pending':
        return <StatusBadge label="Pending Review" variant="warning" />;
      case 'approved':
        return <StatusBadge label="Approved" variant="success" />;
      case 'rejected':
        return <StatusBadge label="Rejected" variant="error" />;
      default:
        return <StatusBadge label={approval.status} variant="neutral" />;
    }
  }

  const fmtPct = (v: number | null) => v != null ? `${v.toFixed(1)}%` : '—';

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Dashboard</h2>
        <p className="text-sm text-gray-500 mt-1">Solvency II QRT key metrics overview</p>
      </div>

      {/* Top row - 4 main metrics */}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
        <MetricCard
          title="Total Assets (SII)"
          value={formatEur(data.total_assets_sii)}
          subtitle={`${data.asset_count.toLocaleString()} assets`}
          accent="blue"
        />
        <MetricCard
          title="Gross Written Premium"
          value={formatEur(data.gwp)}
          subtitle={`NWP: ${formatEur(data.nwp)}`}
          accent="blue"
        />
        <MetricCard
          title="SCR"
          value={formatEur(data.scr)}
          subtitle={`BSCR: ${formatEur(data.bscr)}`}
          accent="blue"
        />
        <MetricCard
          title="Solvency Ratio"
          value={fmtPct(sr)}
          subtitle="Total Assets / SCR"
          accent={solvencyColor}
        />
      </div>

      {/* Second row */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div
          className={`bg-white rounded-lg shadow-sm border border-gray-200 border-l-4 ${
            combinedRatioColor === 'green'
              ? 'border-l-green-600'
              : combinedRatioColor === 'amber'
                ? 'border-l-amber-500'
                : combinedRatioColor === 'red'
                  ? 'border-l-red-600'
                  : 'border-l-gray-400'
          } p-5`}
        >
          <div className="flex items-center gap-2 text-sm font-medium text-gray-500 uppercase tracking-wide">
            <TrendingUp className="w-4 h-4" />
            Combined Ratio
          </div>
          <p className="mt-2 text-2xl font-bold text-gray-900">{fmtPct(cr)}</p>
          <p className="mt-1 text-sm text-gray-500">
            Claims: {formatEur(data.net_claims)} | Expenses: {formatEur(data.total_expenses)}
          </p>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 border-l-4 border-l-blue-600 p-5">
          <div className="flex items-center gap-2 text-sm font-medium text-gray-500 uppercase tracking-wide">
            <CheckCircle2 className="w-4 h-4" />
            Data Quality
          </div>
          <p className="mt-2 text-2xl font-bold text-gray-900">{dqPassRate}</p>
          <p className="mt-1 text-sm text-gray-500">checks passed</p>
        </div>

        <div className="bg-white rounded-lg shadow-sm border border-gray-200 border-l-4 border-l-gray-400 p-5">
          <div className="flex items-center gap-2 text-sm font-medium text-gray-500 uppercase tracking-wide">
            <Clock className="w-4 h-4" />
            Pipeline Last Run
          </div>
          <p className="mt-2 text-lg font-bold text-gray-900">
            {data.pipeline_last_run ? new Date(data.pipeline_last_run).toLocaleString() : '—'}
          </p>
        </div>
      </div>

      {/* Approval status */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wide">Approval Status</h3>
            <div className="mt-2 flex items-center gap-3">
              {approvalBadge()}
              {approval?.reviewed_by && (
                <span className="text-sm text-gray-500">
                  by {approval.reviewed_by} on {approval.reviewed_at ? new Date(approval.reviewed_at).toLocaleString() : ''}
                </span>
              )}
            </div>
            {approval?.comments && (
              <p className="mt-2 text-sm text-gray-600 italic">"{approval.comments}"</p>
            )}
          </div>
          <Shield className="w-10 h-10 text-gray-200" />
        </div>
      </div>
    </div>
  );
}
