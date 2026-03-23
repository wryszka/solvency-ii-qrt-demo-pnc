import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Loader2, ChevronRight, Shield, BarChart3, Landmark, Flame } from 'lucide-react';
import StatusBadge from '../components/StatusBadge';
import { fetchReports, type ReportSummary } from '../lib/api';

const QRT_ICONS: Record<string, React.ComponentType<{ className?: string }>> = {
  s0602: Landmark,
  s0501: BarChart3,
  s2501: Shield,
  s2606: Flame,
};

const QRT_COLORS: Record<string, string> = {
  s0602: 'border-l-blue-500',
  s0501: 'border-l-emerald-500',
  s2501: 'border-l-violet-500',
  s2606: 'border-l-orange-500',
};

const STATUS_VARIANT: Record<string, 'success' | 'error' | 'warning' | 'neutral' | 'info'> = {
  approved: 'success',
  pending: 'warning',
  rejected: 'error',
  draft: 'neutral',
};

const STATUS_LABEL: Record<string, string> = {
  approved: 'Approved',
  pending: 'Pending Review',
  rejected: 'Rejected',
  draft: 'Draft',
};

export default function ReportsList() {
  const [reports, setReports] = useState<ReportSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  useEffect(() => {
    fetchReports()
      .then((r) => setReports(r.data))
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
      <div className="max-w-4xl mx-auto p-6">
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-red-700">
          Failed to load reports: {error}
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-5xl mx-auto p-6 space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">QRT Reports</h2>
        <p className="text-sm text-gray-500 mt-1">
          Click a report to view content, data quality, lineage, and approval status
        </p>
      </div>

      <div className="grid gap-4">
        {reports.map((rpt) => {
          const Icon = QRT_ICONS[rpt.id] || Shield;
          const borderColor = QRT_COLORS[rpt.id] || 'border-l-gray-400';
          const status = rpt.approval_status || 'draft';

          return (
            <button
              key={rpt.id}
              onClick={() => navigate(`/report/${rpt.id}`)}
              className={`w-full text-left bg-white rounded-lg shadow-sm border border-gray-200 border-l-4 ${borderColor} p-5 hover:shadow-md hover:border-gray-300 transition-all group`}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <div className="p-2.5 rounded-lg bg-gray-50 text-gray-500 group-hover:bg-blue-50 group-hover:text-blue-600 transition-colors">
                    <Icon className="w-6 h-6" />
                  </div>
                  <div>
                    <div className="flex items-center gap-3">
                      <h3 className="text-lg font-semibold text-gray-900">
                        {rpt.name} — {rpt.title}
                      </h3>
                      <StatusBadge
                        label={STATUS_LABEL[status] || status}
                        variant={STATUS_VARIANT[status] || 'neutral'}
                      />
                    </div>
                    <div className="flex items-center gap-4 mt-1 text-sm text-gray-500">
                      {rpt.period && <span>Latest: <strong className="text-gray-700">{rpt.period}</strong></span>}
                      {rpt.row_count && <span>{rpt.row_count} rows</span>}
                      {rpt.metric_label && rpt.metric_value && (
                        <span>{rpt.metric_label}: <strong className="text-gray-700">{rpt.metric_value}</strong></span>
                      )}
                      {rpt.scr && <span>SCR: <strong className="text-gray-700">{rpt.scr}</strong></span>}
                    </div>
                  </div>
                </div>
                <ChevronRight className="w-5 h-5 text-gray-300 group-hover:text-blue-500 transition-colors" />
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
