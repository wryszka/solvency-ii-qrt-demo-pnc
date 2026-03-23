import { useEffect, useState } from 'react';
import { Loader2, CheckCircle2, XCircle, ShieldCheck, TrendingUp } from 'lucide-react';
import StatusBadge from '../components/StatusBadge';
import { fetchDqSummary, fetchDqTrends, type Row } from '../lib/api';

export default function DataQuality() {
  const [expectations, setExpectations] = useState<Row[]>([]);
  const [aggregate, setAggregate] = useState<Row | null>(null);
  const [trends, setTrends] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([
      fetchDqSummary().then((r) => {
        setExpectations(r.data);
        setAggregate(r.aggregate);
      }),
      fetchDqTrends().then((r) => setTrends(r.data)),
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
          Failed to load DQ data: {error}
        </div>
      </div>
    );
  }

  const passRate = parseFloat(aggregate?.overall_pass_rate || '100');
  const totalFailing = parseInt(aggregate?.total_failing || '0');
  const failingChecks = parseInt(aggregate?.failing_expectations || '0');
  const totalChecks = parseInt(aggregate?.total_expectations || '0');

  // Group expectations by pipeline
  const byPipeline = new Map<string, Row[]>();
  for (const e of expectations) {
    const key = e.pipeline_name;
    if (!byPipeline.has(key)) byPipeline.set(key, []);
    byPipeline.get(key)!.push(e);
  }

  return (
    <div className="max-w-6xl mx-auto p-6 space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Data Quality Dashboard</h2>
        <p className="text-sm text-gray-500 mt-1">DLT expectation results across all QRT pipelines</p>
      </div>

      {/* KPI Strip */}
      <div className="grid grid-cols-4 gap-4">
        <div className={`rounded-lg border p-4 ${passRate >= 99 ? 'bg-green-50 border-green-200' : 'bg-amber-50 border-amber-200'}`}>
          <div className="flex items-center gap-2 mb-1">
            <ShieldCheck className={`w-4 h-4 ${passRate >= 99 ? 'text-green-500' : 'text-amber-500'}`} />
            <span className="text-xs font-medium uppercase tracking-wide text-gray-500">Overall Pass Rate</span>
          </div>
          <div className={`text-3xl font-bold ${passRate >= 99 ? 'text-green-700' : 'text-amber-700'}`}>{passRate}%</div>
        </div>
        <div className="rounded-lg border bg-white p-4">
          <div className="text-xs font-medium uppercase tracking-wide text-gray-500 mb-1">Total Expectations</div>
          <div className="text-3xl font-bold text-gray-900">{totalChecks}</div>
        </div>
        <div className="rounded-lg border bg-white p-4">
          <div className="text-xs font-medium uppercase tracking-wide text-gray-500 mb-1">Failing Checks</div>
          <div className={`text-3xl font-bold ${failingChecks > 0 ? 'text-amber-600' : 'text-green-600'}`}>{failingChecks}</div>
        </div>
        <div className="rounded-lg border bg-white p-4">
          <div className="text-xs font-medium uppercase tracking-wide text-gray-500 mb-1">Quarantined Rows</div>
          <div className={`text-3xl font-bold ${totalFailing > 0 ? 'text-amber-600' : 'text-green-600'}`}>{totalFailing}</div>
        </div>
      </div>

      {/* DQ Trend */}
      {trends.length > 1 && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <div className="flex items-center gap-2 mb-4">
            <TrendingUp className="w-5 h-5 text-blue-500" />
            <h3 className="font-semibold text-gray-900">Quality Trend by Quarter</h3>
          </div>
          <div className="flex items-end gap-3 h-32">
            {trends.map((t) => {
              const rate = parseFloat(t.pass_rate_pct || '100');
              const height = Math.max(10, ((rate - 95) / 5) * 100); // Scale 95-100% to 0-100%
              const failing = parseInt(t.total_failing || '0');
              return (
                <div key={t.reporting_period} className="flex-1 flex flex-col items-center gap-1">
                  <span className="text-xs font-mono text-gray-500">{rate}%</span>
                  <div
                    className={`w-full rounded-t ${rate >= 99.5 ? 'bg-green-400' : rate >= 99 ? 'bg-green-300' : 'bg-amber-400'}`}
                    style={{ height: `${height}%` }}
                  />
                  <span className="text-xs text-gray-500">{t.reporting_period}</span>
                  <span className="text-[10px] text-gray-400">{failing} quarantined</span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* By Pipeline */}
      {Array.from(byPipeline.entries()).map(([pipeline, checks]) => (
        <PipelineSection key={pipeline} pipeline={pipeline} checks={checks} />
      ))}
    </div>
  );
}

function PipelineSection({ pipeline, checks }: { pipeline: string; checks: Row[] }) {
  const allPass = checks.every((c) => parseInt(c.failing_records || '0') === 0);

  return (
    <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div className={`px-4 py-3 border-b flex items-center justify-between ${allPass ? 'bg-green-50 border-green-100' : 'bg-amber-50 border-amber-100'}`}>
        <div className="flex items-center gap-2">
          {allPass ? <CheckCircle2 className="w-4 h-4 text-green-600" /> : <XCircle className="w-4 h-4 text-amber-600" />}
          <span className="font-semibold text-gray-900">{pipeline}</span>
        </div>
        <span className="text-sm text-gray-500">
          {checks.filter((c) => parseInt(c.failing_records || '0') === 0).length}/{checks.length} passing
        </span>
      </div>
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200 bg-gray-50">
            <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Table</th>
            <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Expectation</th>
            <th className="px-4 py-2 text-right text-xs font-semibold text-gray-600 uppercase">Total</th>
            <th className="px-4 py-2 text-right text-xs font-semibold text-gray-600 uppercase">Passing</th>
            <th className="px-4 py-2 text-right text-xs font-semibold text-gray-600 uppercase">Failing</th>
            <th className="px-4 py-2 text-center text-xs font-semibold text-gray-600 uppercase">Action</th>
            <th className="px-4 py-2 text-center text-xs font-semibold text-gray-600 uppercase">Result</th>
          </tr>
        </thead>
        <tbody>
          {checks.map((c, i) => {
            const failing = parseInt(c.failing_records || '0');
            return (
              <tr key={i} className={`border-b border-gray-100 ${failing > 0 ? 'bg-red-50' : i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}>
                <td className="px-4 py-2 text-gray-700 font-mono text-xs">{c.table_name}</td>
                <td className="px-4 py-2 text-gray-900">{String(c.expectation_name).replace(/_/g, ' ')}</td>
                <td className="px-4 py-2 text-right text-gray-700">{parseInt(c.total_records || '0').toLocaleString()}</td>
                <td className="px-4 py-2 text-right text-green-700">{parseInt(c.passing_records || '0').toLocaleString()}</td>
                <td className="px-4 py-2 text-right text-red-700">{failing}</td>
                <td className="px-4 py-2 text-center">
                  <span className={`inline-flex px-1.5 py-0.5 rounded text-[10px] font-semibold ${
                    c.action === 'DROP ROW' ? 'bg-red-100 text-red-700' :
                    c.action === 'FAIL UPDATE' ? 'bg-orange-100 text-orange-700' :
                    'bg-yellow-100 text-yellow-700'
                  }`}>{c.action}</span>
                </td>
                <td className="px-4 py-2 text-center">
                  <StatusBadge
                    label={failing === 0 ? 'PASS' : 'FAIL'}
                    variant={failing === 0 ? 'success' : 'error'}
                  />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
