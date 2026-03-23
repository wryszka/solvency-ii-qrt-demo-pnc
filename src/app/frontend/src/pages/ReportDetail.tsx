import { useEffect, useState, useMemo } from 'react';
import { useParams, Link } from 'react-router-dom';
import {
  Loader2, ArrowLeft, Download, CheckCircle2, XCircle, Send,
  ChevronLeft, ChevronRight, FileCheck, Clock, GitCompare, FlaskConical,
} from 'lucide-react';
import StatusBadge from '../components/StatusBadge';
import {
  fetchContent, fetchQuality, fetchComparison, fetchLineage, fetchApproval,
  submitForReview, reviewApproval, generateCertificate, downloadFile,
  fetchReconciliation, fetchModelVersions, fetchTemplate,
  formatEur, formatPct,
  type ContentResponse, type QualityCheck, type LineageStep, type ApprovalRecord, type Row,
} from '../lib/api';

const QRT_TITLES: Record<string, { name: string; title: string }> = {
  s0602: { name: 'S.06.02', title: 'List of Assets' },
  s0501: { name: 'S.05.01', title: 'Premiums, Claims & Expenses' },
  s2501: { name: 'S.25.01', title: 'SCR Standard Formula' },
  s2606: { name: 'S.26.06', title: 'NL Underwriting Risk' },
};

type Tab = 'content' | 'quality' | 'comparison' | 'reconciliation' | 'template' | 'lineage' | 'model' | 'approval';

export default function ReportDetail() {
  const { qrtId } = useParams<{ qrtId: string }>();
  const [tab, setTab] = useState<Tab>('content');
  const info = QRT_TITLES[qrtId || ''];

  if (!qrtId || !info) {
    return (
      <div className="max-w-5xl mx-auto p-6">
        <p className="text-red-600">Unknown QRT: {qrtId}</p>
        <Link to="/" className="text-blue-600 text-sm mt-2 inline-block">Back to reports</Link>
      </div>
    );
  }

  const tabs: { id: Tab; label: string }[] = [
    { id: 'content', label: 'Content' },
    { id: 'quality', label: 'Data Quality' },
    { id: 'comparison', label: 'Period Comparison' },
    { id: 'reconciliation', label: 'Reconciliation' },
    { id: 'template', label: 'EIOPA Template' },
    { id: 'lineage', label: 'Lineage' },
    ...(qrtId === 's2501' ? [{ id: 'model' as Tab, label: 'Model Governance' }] : []),
    { id: 'approval', label: 'Approve / Export' },
  ];

  return (
    <div className="max-w-7xl mx-auto p-6 space-y-4">
      <div className="flex items-center gap-3">
        <Link to="/" className="p-1.5 rounded-md hover:bg-gray-200 transition-colors text-gray-500">
          <ArrowLeft className="w-5 h-5" />
        </Link>
        <div>
          <h2 className="text-2xl font-bold text-gray-900">{info.name} — {info.title}</h2>
        </div>
      </div>

      <div className="flex items-center gap-1 border-b border-gray-200 overflow-x-auto">
        {tabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
              tab === t.id
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === 'content' && <ContentTab qrtId={qrtId} />}
      {tab === 'quality' && <QualityTab qrtId={qrtId} />}
      {tab === 'comparison' && <ComparisonTab qrtId={qrtId} />}
      {tab === 'reconciliation' && <ReconciliationTab />}
      {tab === 'template' && <TemplateTab qrtId={qrtId} />}
      {tab === 'lineage' && <LineageTab qrtId={qrtId} />}
      {tab === 'model' && qrtId === 's2501' && <ModelGovernanceTab />}
      {tab === 'approval' && <ApprovalTab qrtId={qrtId} />}
    </div>
  );
}

/* ═══════ Content Tab ═══════ */
function ContentTab({ qrtId }: { qrtId: string }) {
  const [data, setData] = useState<ContentResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);

  useEffect(() => {
    setLoading(true);
    fetchContent(qrtId, page)
      .then(setData)
      .finally(() => setLoading(false));
  }, [qrtId, page]);

  if (loading) return <Spinner />;
  if (!data || !data.data.length) return <Empty msg="No data available" />;

  const rows = data.data;
  const columns = Object.keys(rows[0]).filter((c) => !HIDDEN_COLS.has(c));

  // For S.05.01, render as pivot table
  if (qrtId === 's0501') return <S0501Content rows={rows} qrtId={qrtId} />;

  const totalPages = data.total ? Math.ceil(data.total / (data.page_size || 100)) : 1;
  const showPager = qrtId === 's0602' && data.total && data.total > (data.page_size || 100);

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
        <span className="text-sm text-gray-500">
          {data.total ? `${data.total.toLocaleString()} total records` : `${rows.length} rows`}
        </span>
        <button
          onClick={() => downloadFile(`/api/reports/${qrtId}/csv`, `${qrtId}.csv`)}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-blue-600 border border-blue-200 rounded-md hover:bg-blue-50"
        >
          <Download className="w-4 h-4" /> CSV
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50">
              {columns.map((col) => (
                <th key={col} className="px-3 py-2 text-left text-xs font-semibold text-gray-600 uppercase whitespace-nowrap">
                  {col.replace(/_/g, ' ').replace(/^c\d+\s*/i, (m) => m.toUpperCase())}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => {
              const isHighlight = row.template_row_id === 'R0200' || row.template_row_id === 'R0100';
              return (
                <tr key={i} className={`border-b border-gray-100 ${isHighlight ? 'bg-blue-50 font-semibold' : i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}>
                  {columns.map((col) => (
                    <td key={col} className="px-3 py-1.5 text-gray-800 whitespace-nowrap">
                      {formatCell(col, row[col])}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {showPager && (
        <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200 text-sm text-gray-600">
          <span>Page {page} of {totalPages}</span>
          <div className="flex items-center gap-2">
            <button disabled={page <= 1} onClick={() => setPage(p => p - 1)}
              className="p-1 rounded hover:bg-gray-100 disabled:opacity-30">
              <ChevronLeft className="w-5 h-5" />
            </button>
            <button disabled={page >= totalPages} onClick={() => setPage(p => p + 1)}
              className="p-1 rounded hover:bg-gray-100 disabled:opacity-30">
              <ChevronRight className="w-5 h-5" />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function isNumericCol(col: string): boolean {
  const lower = col.toLowerCase();
  return (lower.includes('amount') || lower.includes('eur') || lower.includes('sii')
    || lower.includes('accrued') || lower.includes('acquisition')
    || lower.includes('c0130') || lower.includes('c0140') || lower.includes('c0160')
    || lower.includes('c0170') || lower.includes('c0180'))
    && !lower.includes('method') && !lower.includes('type') && !lower.includes('code');
}

// Columns that are always empty/zero for P&C — hide to reduce clutter
const HIDDEN_COLS = new Set([
  'C0070_Fund_Number', 'C0080_Matching_Adj_Portfolio',
  'C0090_Unit_Linked', 'C0100_Pledged_As_Collateral',
  'C0240_Issuer_Group_Code', 'C0320_Internal_Rating',
]);

function formatCell(col: string, value: unknown): string {
  if (value == null || value === '') return '\u2014';
  // Infrastructure flag: 0/1 → No/Yes
  if (col === 'C0280_Infrastructure_Investment') return value === '1' ? 'Yes' : 'No';
  // Valuation method: 1=Mark-to-market, 2=Mark-to-model
  if (col === 'C0150_Valuation_Method') return value === '1' ? 'Mark-to-market' : 'Mark-to-model';
  // Credit quality step: strip trailing .0
  if (col === 'C0310_Credit_Quality_Step') {
    const s = String(value).replace(/\.0$/, '');
    return `CQS ${s}`;
  }
  // Numeric columns
  if (isNumericCol(col)) return formatEur(value as number | string);
  return String(value);
}

/* ─── S.05.01 pivot ─── */
function S0501Content({ rows, qrtId }: { rows: Row[]; qrtId: string }) {
  const { sections, lobLabels } = useMemo(() => {
    const lobSet = new Set<string>();
    const rowMap = new Map<string, Map<string, number>>();
    const rowLabels = new Map<string, string>();

    for (const row of rows) {
      const lob = row.lob_name || row.lob_label || `LoB ${row.lob_code}`;
      lobSet.add(lob);
      const rid = row.template_row_id;
      if (!rowMap.has(rid)) rowMap.set(rid, new Map());
      rowMap.get(rid)!.set(lob, parseFloat(row.amount_eur));
      rowLabels.set(rid, row.template_row_label);
    }

    const lobLabels = [...lobSet].sort((a, b) => {
      if (a === 'Total') return 1;
      if (b === 'Total') return -1;
      return a.localeCompare(b);
    });

    const sectionDefs: Record<string, string> = {
      R0110: 'Premiums Written', R0140: 'Premiums Written', R0200: 'Premiums Written',
      R0210: 'Premiums Earned', R0240: 'Premiums Earned', R0300: 'Premiums Earned',
      R0310: 'Claims Incurred', R0340: 'Claims Incurred', R0400: 'Claims Incurred',
      R0410: 'Claims Paid', R0500: 'Claims Paid',
      R0550: 'Expenses', R0610: 'Expenses', R0620: 'Expenses',
      R0630: 'Expenses', R0640: 'Expenses', R0680: 'Expenses', R1200: 'Other Expenses',
    };

    const sectionMap = new Map<string, { rowId: string; label: string; values: Map<string, number> }[]>();
    for (const [rowId, values] of rowMap.entries()) {
      const section = sectionDefs[rowId] || 'Other';
      if (!sectionMap.has(section)) sectionMap.set(section, []);
      sectionMap.get(section)!.push({ rowId, label: rowLabels.get(rowId) || rowId, values });
    }

    return {
      sections: [...sectionMap.entries()].map(([name, rows]) => ({ name, rows })),
      lobLabels,
    };
  }, [rows]);

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200">
      <div className="flex items-center justify-end px-4 py-3 border-b border-gray-200">
        <button
          onClick={() => downloadFile(`/api/reports/${qrtId}/csv`, 's0501.csv')}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-blue-600 border border-blue-200 rounded-md hover:bg-blue-50"
        >
          <Download className="w-4 h-4" /> CSV
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50">
              <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Row</th>
              <th className="px-3 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Description</th>
              {lobLabels.map((l) => (
                <th key={l} className={`px-3 py-2 text-right text-xs font-semibold text-gray-600 uppercase whitespace-nowrap ${l === 'Total' ? 'bg-blue-50' : ''}`}>
                  {l}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sections.map((sec) => (
              <SectionRows key={sec.name} section={sec} lobLabels={lobLabels} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SectionRows({ section, lobLabels }: {
  section: { name: string; rows: { rowId: string; label: string; values: Map<string, number> }[] };
  lobLabels: string[];
}) {
  return (
    <>
      <tr className="bg-gray-100 border-t-2 border-gray-300">
        <td colSpan={2 + lobLabels.length} className="px-3 py-2 text-xs font-bold text-gray-700 uppercase tracking-wide">
          {section.name}
        </td>
      </tr>
      {section.rows.map((row, i) => (
        <tr key={row.rowId} className={`border-b border-gray-100 ${i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}>
          <td className="px-3 py-1.5 text-gray-500 font-mono text-xs">{row.rowId}</td>
          <td className="px-3 py-1.5 text-gray-800">{row.label}</td>
          {lobLabels.map((lob) => (
            <td key={lob} className={`px-3 py-1.5 text-right font-mono ${lob === 'Total' ? 'bg-blue-50/50 font-semibold' : ''}`}>
              {row.values.has(lob) ? formatEur(row.values.get(lob)!) : '\u2014'}
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

/* ═══════ Quality Tab ═══════ */
function QualityTab({ qrtId }: { qrtId: string }) {
  const [checks, setChecks] = useState<QualityCheck[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchQuality(qrtId)
      .then((r) => setChecks(r.data))
      .finally(() => setLoading(false));
  }, [qrtId]);

  if (loading) return <Spinner />;

  const allPass = checks.every((c) => c.status === 'PASS');
  const passCount = checks.filter((c) => c.status === 'PASS').length;

  return (
    <div className="space-y-4">
      <div className={`rounded-lg p-4 border ${allPass ? 'bg-green-50 border-green-200' : 'bg-amber-50 border-amber-200'}`}>
        <div className="flex items-center gap-2">
          {allPass ? <CheckCircle2 className="w-5 h-5 text-green-600" /> : <XCircle className="w-5 h-5 text-amber-600" />}
          <span className={`font-semibold ${allPass ? 'text-green-800' : 'text-amber-800'}`}>
            {allPass ? 'All checks passed' : `${passCount}/${checks.length} checks passed`}
          </span>
        </div>
      </div>

      <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50">
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Check</th>
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Constraint</th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Total</th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Passing</th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Failing</th>
              <th className="px-4 py-3 text-center text-xs font-semibold text-gray-600 uppercase">Result</th>
              <th className="px-4 py-3 text-center text-xs font-semibold text-gray-600 uppercase">Severity</th>
            </tr>
          </thead>
          <tbody>
            {checks.map((c, i) => (
              <tr key={i} className={`border-b border-gray-100 ${c.status === 'FAIL' ? 'bg-red-50' : i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}>
                <td className="px-4 py-2.5 font-medium text-gray-900">{c.check}</td>
                <td className="px-4 py-2.5 text-gray-600 font-mono text-xs">{c.constraint}</td>
                <td className="px-4 py-2.5 text-right text-gray-700">{c.total.toLocaleString()}</td>
                <td className="px-4 py-2.5 text-right text-green-700">{c.passing.toLocaleString()}</td>
                <td className="px-4 py-2.5 text-right text-red-700">{c.failing}</td>
                <td className="px-4 py-2.5 text-center">
                  <StatusBadge label={c.status} variant={c.status === 'PASS' ? 'success' : 'error'} />
                </td>
                <td className="px-4 py-2.5 text-center text-xs text-gray-500">{c.severity}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ═══════ Comparison Tab ═══════ */
function ComparisonTab({ qrtId }: { qrtId: string }) {
  const [rows, setRows] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchComparison(qrtId)
      .then((r) => setRows(r.data))
      .finally(() => setLoading(false));
  }, [qrtId]);

  if (loading) return <Spinner />;
  if (!rows.length) return <Empty msg="No comparison data" />;

  const columns = Object.keys(rows[0]);

  // Detect numeric-like columns for formatting
  const numericCols = new Set(columns.filter((c) => {
    const lower = c.toLowerCase();
    return lower.includes('eur') || lower.includes('amount') || lower.includes('sii')
      || lower.includes('premium') || lower.includes('incurred') || lower.includes('expense')
      || lower.includes('paid') || lower.includes('scr') || lower.includes('bscr')
      || lower.includes('mcr') || lower.includes('surplus') || lower.includes('funds')
      || lower.includes('risk') || lower.includes('lac');
  }));
  const pctCols = new Set(columns.filter((c) => c.toLowerCase().includes('pct') || c.toLowerCase().includes('ratio')));

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200 bg-gray-50">
            {columns.map((col) => (
              <th key={col} className="px-3 py-2 text-left text-xs font-semibold text-gray-600 uppercase whitespace-nowrap">
                {col.replace(/_/g, ' ')}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className={`border-b border-gray-100 ${i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}>
              {columns.map((col) => (
                <td key={col} className="px-3 py-2 text-gray-800 whitespace-nowrap font-mono text-xs">
                  {pctCols.has(col) ? formatPct(row[col])
                    : numericCols.has(col) ? formatEur(row[col])
                    : String(row[col] ?? '\u2014')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ═══════ Lineage Tab ═══════ */
function LineageTab({ qrtId }: { qrtId: string }) {
  const [steps, setSteps] = useState<LineageStep[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchLineage(qrtId)
      .then((r) => setSteps(r.data))
      .finally(() => setLoading(false));
  }, [qrtId]);

  if (loading) return <Spinner />;

  // Group steps by phase
  const phases = ['Ingestion', 'Preparation', 'Stochastic', 'Transformation', 'Confirmation', 'Export'];
  const phaseConfig: Record<string, { color: string; bg: string; border: string; icon: string }> = {
    Ingestion: { color: 'text-gray-700', bg: 'bg-gray-50', border: 'border-gray-300', icon: 'Download' },
    Preparation: { color: 'text-purple-700', bg: 'bg-purple-50', border: 'border-purple-300', icon: 'Package' },
    Stochastic: { color: 'text-pink-700', bg: 'bg-pink-50', border: 'border-pink-300', icon: 'Zap' },
    Transformation: { color: 'text-blue-700', bg: 'bg-blue-50', border: 'border-blue-300', icon: 'Wrench' },
    Confirmation: { color: 'text-amber-700', bg: 'bg-amber-50', border: 'border-amber-300', icon: 'CheckSquare' },
    Export: { color: 'text-green-700', bg: 'bg-green-50', border: 'border-green-300', icon: 'FileOutput' },
  };

  const grouped = phases
    .map((phase) => ({ phase, steps: steps.filter((s) => s.phase === phase) }))
    .filter((g) => g.steps.length > 0);

  return (
    <div className="space-y-6">
      {/* Phase overview bar */}
      <div className="flex items-center gap-1">
        {grouped.map((g, i) => {
          const cfg = phaseConfig[g.phase];
          return (
            <div key={g.phase} className="flex items-center">
              <div className={`px-3 py-1.5 rounded-full text-xs font-bold ${cfg.bg} ${cfg.color} border ${cfg.border}`}>
                {g.phase} ({g.steps.length})
              </div>
              {i < grouped.length - 1 && <div className="w-6 h-0.5 bg-gray-300 mx-1" />}
            </div>
          );
        })}
      </div>

      {/* Phase sections */}
      {grouped.map((g) => {
        const cfg = phaseConfig[g.phase];
        return (
          <div key={g.phase}>
            <div className={`px-4 py-2 rounded-t-lg border-b-2 ${cfg.bg} ${cfg.border}`}>
              <h4 className={`text-sm font-bold uppercase tracking-wide ${cfg.color}`}>{g.phase}</h4>
            </div>
            <div className={`grid gap-3 p-3 bg-white rounded-b-lg border border-t-0 ${cfg.border} ${g.steps.length > 1 && g.phase !== 'Confirmation' ? 'sm:grid-cols-' + Math.min(g.steps.length, 3) : ''}`}
                 style={g.steps.length > 1 && g.phase !== 'Confirmation' ? { gridTemplateColumns: `repeat(${Math.min(g.steps.length, 3)}, 1fr)` } : undefined}>
              {g.steps.map((s) => (
                <LineageCard key={s.step} step={s} />
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function LineageCard({ step: s }: { step: LineageStep }) {
  const [showSql, setShowSql] = useState(false);

  const layerColors: Record<string, string> = {
    Bronze: 'bg-orange-100 text-orange-800',
    Silver: 'bg-blue-100 text-blue-800',
    Gold: 'bg-amber-100 text-amber-800',
    Model: 'bg-violet-100 text-violet-800',
    Export: 'bg-green-100 text-green-800',
  };

  const actionColors: Record<string, string> = {
    'DROP ROW': 'bg-red-100 text-red-700',
    'FAIL UPDATE': 'bg-orange-100 text-orange-700',
    'WARN': 'bg-yellow-100 text-yellow-700',
  };

  return (
    <div className="border border-gray-200 rounded-lg p-4 space-y-2">
      {/* Header */}
      <div className="flex items-center gap-2 flex-wrap">
        <span className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold text-white ${
          s.layer === 'Bronze' ? 'bg-orange-500' :
          s.layer === 'Silver' ? 'bg-blue-500' :
          s.layer === 'Gold' ? 'bg-amber-500' :
          s.layer === 'Model' ? 'bg-violet-500' : 'bg-green-500'
        }`}>{s.step}</span>
        <span className="font-mono text-xs text-gray-500">{s.source}</span>
        <span className="text-gray-300">&rarr;</span>
        <span className="font-mono text-xs font-semibold text-gray-900">{s.target}</span>
        <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold ${layerColors[s.layer] || 'bg-gray-100 text-gray-700'}`}>
          {s.layer}
        </span>
      </div>

      {/* Description */}
      <p className="text-sm text-gray-600 leading-relaxed">{s.description}</p>

      {/* Row count hint */}
      {s.row_count_hint && (
        <div className="text-xs text-gray-400 font-mono">{s.row_count_hint}</div>
      )}

      {/* Expectations */}
      {s.expectations && s.expectations.length > 0 && (
        <div className="space-y-1">
          <div className="text-xs font-semibold text-gray-500 uppercase">DLT Expectations</div>
          {s.expectations.map((e, i) => (
            <div key={i} className="flex items-center gap-2 text-xs">
              <CheckCircle2 className="w-3 h-3 text-green-500 shrink-0" />
              <code className="text-gray-700 bg-gray-50 px-1 rounded">{e.rule}</code>
              <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold ${actionColors[e.action] || 'bg-gray-100 text-gray-600'}`}>
                {e.action}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* SQL snippet toggle */}
      {s.sql_snippet && (
        <div>
          <button
            onClick={() => setShowSql(!showSql)}
            className="text-xs text-blue-600 hover:text-blue-800 font-medium"
          >
            {showSql ? 'Hide SQL' : 'Show SQL'}
          </button>
          {showSql && (
            <pre className="mt-2 p-3 bg-gray-900 text-green-300 text-xs rounded-lg overflow-x-auto max-h-64 overflow-y-auto font-mono leading-relaxed">
              {s.sql_snippet}
            </pre>
          )}
        </div>
      )}
    </div>
  );
}

/* ═══════ Approval Tab ═══════ */
function ApprovalTab({ qrtId }: { qrtId: string }) {
  const [approval, setApproval] = useState<ApprovalRecord | null>(null);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [reviewing, setReviewing] = useState(false);
  const [comments, setComments] = useState('');
  const [error, setError] = useState<string | null>(null);

  function load() {
    setLoading(true);
    fetchApproval(qrtId)
      .then((r) => setApproval(r.data))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }

  useEffect(() => { load(); }, [qrtId]);

  async function handleSubmit() {
    setSubmitting(true);
    setError(null);
    try {
      await submitForReview(qrtId);
      load();
    } catch (e: unknown) {
      setError((e as Error).message);
    } finally {
      setSubmitting(false);
    }
  }

  async function handleReview(status: 'approved' | 'rejected') {
    setReviewing(true);
    setError(null);
    try {
      await reviewApproval(qrtId, status, comments);
      setComments('');
      load();
    } catch (e: unknown) {
      setError((e as Error).message);
    } finally {
      setReviewing(false);
    }
  }

  if (loading) return <Spinner />;

  const status = approval?.status || 'none';
  const statusConfig: Record<string, { label: string; variant: 'success' | 'error' | 'warning' | 'neutral'; Icon: typeof FileCheck }> = {
    approved: { label: 'Approved & Exported', variant: 'success', Icon: CheckCircle2 },
    pending: { label: 'Pending Review', variant: 'warning', Icon: Clock },
    rejected: { label: 'Rejected', variant: 'error', Icon: XCircle },
    none: { label: 'Not Submitted', variant: 'neutral', Icon: FileCheck },
  };
  const cfg = statusConfig[status] || statusConfig.none;
  const StatusIcon = cfg.Icon;

  return (
    <div className="space-y-4">
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-700">{error}</div>
      )}

      {/* Status card */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-start gap-5">
          <div className={`p-3 rounded-full ${
            cfg.variant === 'success' ? 'bg-green-100 text-green-600'
              : cfg.variant === 'error' ? 'bg-red-100 text-red-600'
              : cfg.variant === 'warning' ? 'bg-amber-100 text-amber-600'
              : 'bg-gray-100 text-gray-500'
          }`}>
            <StatusIcon className="w-8 h-8" />
          </div>
          <div className="flex-1">
            <h3 className="text-lg font-semibold text-gray-900">Approval Status</h3>
            <div className="mt-2">
              <StatusBadge label={cfg.label} variant={cfg.variant} />
            </div>
            {approval && (
              <div className="mt-3 space-y-1 text-sm text-gray-600">
                <p><span className="font-medium">Period:</span> {approval.reporting_period}</p>
                <p><span className="font-medium">Submitted by:</span> {approval.submitted_by}</p>
                <p><span className="font-medium">Submitted at:</span> {approval.submitted_at}</p>
                {approval.reviewed_by && (
                  <p><span className="font-medium">Reviewed by:</span> {approval.reviewed_by} on {approval.reviewed_at}</p>
                )}
                {approval.export_path && (
                  <div className="mt-2 p-3 bg-green-50 rounded-md border border-green-200">
                    <p className="text-xs font-medium text-green-700 uppercase mb-1">Tagetik Export</p>
                    <p className="text-green-800 font-mono text-xs break-all">{approval.export_path}</p>
                  </div>
                )}
                {approval.comments && (
                  <div className="mt-2 p-3 bg-gray-50 rounded-md border border-gray-200">
                    <p className="text-xs font-medium text-gray-500 uppercase mb-1">Comments</p>
                    <p className="text-gray-700">{approval.comments}</p>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Certificate generation (only for approved) */}
      {status === 'approved' && <CertificateSection qrtId={qrtId} />}

      {/* Submit action */}
      {(!approval || status === 'rejected' || status === 'none') && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-3">Submit for Review</h3>
          <p className="text-sm text-gray-600 mb-4">
            Submit this QRT for actuarial review. Upon approval, data will be exported to the
            regulatory volume (simulated Tagetik export).
          </p>
          <button
            onClick={handleSubmit}
            disabled={submitting}
            className="inline-flex items-center gap-2 px-5 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 font-medium"
          >
            {submitting ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
            Submit for Review
          </button>
        </div>
      )}

      {/* Review action */}
      {status === 'pending' && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-3">Review Submission</h3>
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Comments</label>
              <textarea
                rows={3}
                value={comments}
                onChange={(e) => setComments(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                placeholder="Add review comments..."
              />
            </div>
            <div className="flex items-center gap-3">
              <button
                onClick={() => handleReview('approved')}
                disabled={reviewing}
                className="inline-flex items-center gap-2 px-5 py-2.5 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 font-medium"
              >
                {reviewing ? <Loader2 className="w-4 h-4 animate-spin" /> : <CheckCircle2 className="w-4 h-4" />}
                Approve & Export to Tagetik
              </button>
              <button
                onClick={() => handleReview('rejected')}
                disabled={reviewing}
                className="inline-flex items-center gap-2 px-5 py-2.5 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 font-medium"
              >
                {reviewing ? <Loader2 className="w-4 h-4 animate-spin" /> : <XCircle className="w-4 h-4" />}
                Reject
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* ═══════ Certificate Section ═══════ */
function CertificateSection({ qrtId }: { qrtId: string }) {
  const [certPath, setCertPath] = useState<string | null>(null);
  const [generating, setGenerating] = useState(false);
  const [certError, setCertError] = useState<string | null>(null);

  async function handleGenerate() {
    setGenerating(true);
    setCertError(null);
    try {
      const result = await generateCertificate(qrtId);
      setCertPath(result.certificate_path);
    } catch (e: unknown) {
      setCertError((e as Error).message);
    } finally {
      setGenerating(false);
    }
  }

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
      <h3 className="text-lg font-semibold text-gray-900 mb-3">Approval Certificate</h3>
      <p className="text-sm text-gray-600 mb-4">
        Generate a PDF certificate with approval details, data hash, and export path.
        The certificate is stored in the regulatory exports volume.
      </p>
      {certError && (
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-700 mb-4">{certError}</div>
      )}
      {certPath ? (
        <div className="p-4 bg-blue-50 rounded-lg border border-blue-200">
          <p className="text-xs font-medium text-blue-700 uppercase mb-1">Certificate Generated</p>
          <p className="text-blue-800 font-mono text-xs break-all">{certPath}</p>
        </div>
      ) : (
        <button
          onClick={handleGenerate}
          disabled={generating}
          className="inline-flex items-center gap-2 px-5 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 font-medium"
        >
          {generating ? <Loader2 className="w-4 h-4 animate-spin" /> : <FileCheck className="w-4 h-4" />}
          Generate Certificate
        </button>
      )}
    </div>
  );
}

/* ═══════ EIOPA Template Tab ═══════ */
function TemplateTab({ qrtId }: { qrtId: string }) {
  const [template, setTemplate] = useState<Row | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchTemplate(qrtId)
      .then(setTemplate)
      .finally(() => setLoading(false));
  }, [qrtId]);

  if (loading) return <Spinner />;
  if (!template) return <Empty msg="No template data available" />;

  return (
    <div className="space-y-4">
      {/* Header bar */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-gray-900">{template.qrt} — {template.title}</h3>
          <p className="text-sm text-gray-500">EIOPA regulatory template format | Period: {template.period || 'Latest'}</p>
        </div>
        <a
          href={`/api/reports/${qrtId}/template-pdf`}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-blue-600 border border-blue-200 rounded-md hover:bg-blue-50"
        >
          <Download className="w-4 h-4" /> Download PDF
        </a>
      </div>

      {/* Render based on format */}
      {template.format === 'crosstab' && <S0501Template data={template.data || []} />}
      {template.format === 'waterfall' && <S2501Template data={template.data || []} summary={template.summary} />}
      {template.format === 'summary' && <S0602Template data={template.data || []} totals={template.totals} />}
    </div>
  );
}

function S0501Template({ data }: { data: Row[] }) {
  const lobSet = new Map<string, boolean>();
  const rowMap = new Map<string, Map<string, number>>();
  const rowLabels = new Map<string, string>();

  for (const r of data) {
    const lob = r.lob_name;
    const rid = r.template_row_id;
    lobSet.set(lob, true);
    rowLabels.set(rid, r.template_row_label);
    if (!rowMap.has(rid)) rowMap.set(rid, new Map());
    rowMap.get(rid)!.set(lob, parseFloat(r.amount_eur));
  }

  const lobs = [...lobSet.keys()].sort((a, b) => a === 'Total' ? 1 : b === 'Total' ? -1 : a.localeCompare(b));
  const sectionBreaks = new Set(['R0210', 'R0310', 'R0410', 'R0550']);

  return (
    <div className="bg-white rounded-lg border-2 border-gray-300 overflow-x-auto">
      <div className="bg-blue-900 text-white px-4 py-2 text-sm font-bold">
        S.05.01.02 — Non-Life — Premiums, claims and expenses by line of business
      </div>
      <table className="min-w-full text-xs">
        <thead>
          <tr className="bg-blue-50 border-b-2 border-blue-200">
            <th className="px-2 py-1.5 text-left font-bold text-blue-900 w-10">Row</th>
            <th className="px-2 py-1.5 text-left font-bold text-blue-900 min-w-[180px]">Description</th>
            {lobs.map((l) => (
              <th key={l} className={`px-2 py-1.5 text-right font-bold text-blue-900 whitespace-nowrap ${l === 'Total' ? 'bg-blue-100' : ''}`}>
                {l.length > 15 ? l.substring(0, 15) + '...' : l}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {[...rowMap.entries()].map(([rid, values]) => {
            const isNet = rid.startsWith('R02') || rid.startsWith('R03') || rid.startsWith('R04') || rid.startsWith('R05');
            const isSection = sectionBreaks.has(rid);
            return (
              <tr key={rid} className={`border-b ${isSection ? 'border-t-2 border-gray-300' : 'border-gray-100'} ${isNet ? 'bg-gray-50 font-semibold' : ''}`}>
                <td className="px-2 py-1 font-mono text-gray-500">{rid}</td>
                <td className="px-2 py-1 text-gray-800">{rowLabels.get(rid)}</td>
                {lobs.map((lob) => (
                  <td key={lob} className={`px-2 py-1 text-right font-mono ${lob === 'Total' ? 'bg-blue-50/50 font-bold' : ''}`}>
                    {values.has(lob) ? formatEur(values.get(lob)!) : ''}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function S2501Template({ data, summary }: { data: Row[]; summary?: Row }) {
  const mainRows = data.filter((r) => !String(r.template_row_id).includes('.'));
  const subRows = data.filter((r) => String(r.template_row_id).includes('.'));

  return (
    <div className="space-y-4">
      {/* Main SCR breakdown */}
      <div className="bg-white rounded-lg border-2 border-gray-300 overflow-hidden">
        <div className="bg-blue-900 text-white px-4 py-2 text-sm font-bold">
          S.25.01.01 — Solvency Capital Requirement — Standard Formula
        </div>
        <table className="min-w-full text-sm">
          <thead>
            <tr className="bg-blue-50 border-b-2 border-blue-200">
              <th className="px-4 py-2 text-left font-bold text-blue-900 w-20">Row</th>
              <th className="px-4 py-2 text-left font-bold text-blue-900">Component</th>
              <th className="px-4 py-2 text-right font-bold text-blue-900 w-40">Amount (EUR)</th>
            </tr>
          </thead>
          <tbody>
            {mainRows.map((r) => {
              const isKey = r.template_row_id === 'R0100' || r.template_row_id === 'R0200';
              return (
                <tr key={r.template_row_id} className={`border-b ${isKey ? 'bg-blue-50 font-bold border-blue-200' : 'border-gray-100'}`}>
                  <td className="px-4 py-2 font-mono text-gray-500">{r.template_row_id}</td>
                  <td className="px-4 py-2 text-gray-900">{r.template_row_label}</td>
                  <td className={`px-4 py-2 text-right font-mono ${isKey ? 'text-blue-900' : 'text-gray-800'}`}>
                    {formatEur(r.amount_eur)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Sub-module detail */}
      {subRows.length > 0 && (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <div className="px-4 py-2 bg-gray-50 border-b text-sm font-semibold text-gray-700">
            Sub-module Detail
          </div>
          <table className="min-w-full text-sm">
            <tbody>
              {subRows.map((r) => (
                <tr key={r.template_row_id} className="border-b border-gray-100">
                  <td className="px-4 py-1.5 font-mono text-gray-400 w-20">{r.template_row_id}</td>
                  <td className="px-4 py-1.5 text-gray-700">{r.template_row_label}</td>
                  <td className="px-4 py-1.5 text-right font-mono text-gray-700 w-40">{formatEur(r.amount_eur)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Solvency position */}
      {summary && (
        <div className="bg-white rounded-lg border-2 border-green-300 p-5">
          <h4 className="font-bold text-gray-900 mb-3">Solvency Position</h4>
          <div className="grid grid-cols-3 gap-6">
            <div>
              <span className="text-xs text-gray-500 uppercase">Eligible Own Funds</span>
              <div className="text-xl font-bold text-gray-900">{formatEur(summary.eligible_own_funds_eur)}</div>
            </div>
            <div>
              <span className="text-xs text-gray-500 uppercase">SCR</span>
              <div className="text-xl font-bold text-gray-900">{formatEur(summary.scr_eur)}</div>
            </div>
            <div>
              <span className="text-xs text-gray-500 uppercase">Solvency Ratio</span>
              <div className="text-xl font-bold text-green-700">{summary.solvency_ratio_pct}%</div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function S0602Template({ data, totals }: { data: Row[]; totals?: Row }) {
  return (
    <div className="bg-white rounded-lg border-2 border-gray-300 overflow-hidden">
      <div className="bg-blue-900 text-white px-4 py-2 text-sm font-bold">
        S.06.02.01 — List of Assets — Summary by CIC Category
      </div>
      <table className="min-w-full text-sm">
        <thead>
          <tr className="bg-blue-50 border-b-2 border-blue-200">
            <th className="px-4 py-2 text-left font-bold text-blue-900">CIC Category</th>
            <th className="px-4 py-2 text-right font-bold text-blue-900">Assets</th>
            <th className="px-4 py-2 text-right font-bold text-blue-900">Total SII (EUR)</th>
            <th className="px-4 py-2 text-right font-bold text-blue-900">% of Total</th>
            <th className="px-4 py-2 text-right font-bold text-blue-900">Inv. Grade</th>
            <th className="px-4 py-2 text-right font-bold text-blue-900">Avg Duration</th>
          </tr>
        </thead>
        <tbody>
          {data.map((r, i) => (
            <tr key={i} className={`border-b border-gray-100 ${i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}>
              <td className="px-4 py-2 text-gray-900 font-medium">{r.cic_category_name}</td>
              <td className="px-4 py-2 text-right font-mono">{parseInt(r.asset_count || '0').toLocaleString()}</td>
              <td className="px-4 py-2 text-right font-mono">{formatEur(r.total_sii_amount)}</td>
              <td className="px-4 py-2 text-right font-mono">{r.pct_of_total_sii}%</td>
              <td className="px-4 py-2 text-right font-mono">{r.investment_grade_count || ''}</td>
              <td className="px-4 py-2 text-right font-mono">{r.avg_duration ? parseFloat(r.avg_duration).toFixed(1) : ''}</td>
            </tr>
          ))}
          {totals && (
            <tr className="border-t-2 border-gray-300 bg-blue-50 font-bold">
              <td className="px-4 py-2 text-blue-900">TOTAL</td>
              <td className="px-4 py-2 text-right font-mono text-blue-900">{parseInt(totals.cnt || '0').toLocaleString()}</td>
              <td className="px-4 py-2 text-right font-mono text-blue-900">{formatEur(totals.total_sii)}</td>
              <td className="px-4 py-2 text-right font-mono text-blue-900">100.0%</td>
              <td className="px-4 py-2"></td>
              <td className="px-4 py-2"></td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

/* ═══════ Reconciliation Tab ═══════ */
function ReconciliationTab() {
  const [checks, setChecks] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchReconciliation()
      .then((r) => setChecks(r.data))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <Spinner />;
  if (!checks.length) return <Empty msg="No reconciliation data available" />;

  const allMatch = checks.every((c) => c.status === 'MATCH');

  return (
    <div className="space-y-4">
      <div className={`rounded-lg p-4 border ${allMatch ? 'bg-green-50 border-green-200' : 'bg-amber-50 border-amber-200'}`}>
        <div className="flex items-center gap-2">
          {allMatch ? <CheckCircle2 className="w-5 h-5 text-green-600" /> : <GitCompare className="w-5 h-5 text-amber-600" />}
          <span className={`font-semibold ${allMatch ? 'text-green-800' : 'text-amber-800'}`}>
            {allMatch ? 'All cross-QRT reconciliation checks passed' : `${checks.filter(c => c.status === 'MATCH').length}/${checks.length} checks passed`}
          </span>
        </div>
      </div>

      <div className="space-y-3">
        {checks.map((check, i) => {
          const isMatch = check.status === 'MATCH';
          return (
            <div key={i} className={`bg-white rounded-lg border p-5 ${isMatch ? 'border-gray-200' : 'border-red-200 bg-red-50'}`}>
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-3">
                  <span className="text-sm font-semibold text-gray-900">{check.source_qrt}</span>
                  <span className="text-gray-300">&harr;</span>
                  <span className="text-sm font-semibold text-gray-900">{check.target_qrt}</span>
                </div>
                <StatusBadge label={check.status} variant={isMatch ? 'success' : 'error'} />
              </div>
              <p className="text-sm text-gray-600 mb-3">{check.check_description}</p>
              <div className="grid grid-cols-4 gap-4 text-sm">
                <div>
                  <span className="text-xs text-gray-500 uppercase">Source Value</span>
                  <div className="font-mono font-semibold text-gray-900">{formatEur(check.source_value)}</div>
                </div>
                <div>
                  <span className="text-xs text-gray-500 uppercase">Target Value</span>
                  <div className="font-mono font-semibold text-gray-900">{formatEur(check.target_value)}</div>
                </div>
                <div>
                  <span className="text-xs text-gray-500 uppercase">Difference</span>
                  <div className={`font-mono font-semibold ${isMatch ? 'text-green-600' : 'text-red-600'}`}>
                    {formatEur(check.difference)}
                  </div>
                </div>
                <div>
                  <span className="text-xs text-gray-500 uppercase">Tolerance</span>
                  <div className="font-mono text-gray-600">{formatEur(check.tolerance)}</div>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* ═══════ Model Governance Tab (S.25.01 only) ═══════ */
function ModelGovernanceTab() {
  const [versions, setVersions] = useState<Row[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchModelVersions()
      .then((r) => setVersions(r.data))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <Spinner />;
  if (!versions.length) return <Empty msg="No model version data available" />;

  const champion = versions.find((v) => v.alias === 'Champion');
  const challenger = versions.find((v) => v.alias === 'Challenger');

  const champScr = parseFloat(champion?.scr_result_eur || '0');
  const challScr = parseFloat(challenger?.scr_result_eur || '0');
  const diff = challScr - champScr;
  const diffPct = champScr > 0 ? ((diff / champScr) * 100).toFixed(1) : '0.0';

  return (
    <div className="space-y-4">
      {/* Model cards */}
      <div className="grid gap-4 sm:grid-cols-2">
        {champion && (
          <div className="bg-white rounded-lg border-2 border-green-300 p-5">
            <div className="flex items-center gap-2 mb-3">
              <div className="px-2 py-0.5 bg-green-100 text-green-800 rounded text-xs font-bold uppercase">Champion</div>
              <span className="text-sm text-gray-500">v{champion.model_version} — {champion.calibration_year} Calibration</span>
            </div>
            <div className="mb-3">
              <span className="text-xs text-gray-500 uppercase">SCR Result</span>
              <div className="text-2xl font-bold text-gray-900">{formatEur(champion.scr_result_eur)}</div>
            </div>
            <p className="text-sm text-gray-600">{champion.description}</p>
            <div className="mt-3 text-xs text-gray-400">
              Registered by {champion.registered_by} on {String(champion.run_timestamp).split('T')[0]}
            </div>
          </div>
        )}
        {challenger && (
          <div className="bg-white rounded-lg border-2 border-violet-300 p-5">
            <div className="flex items-center gap-2 mb-3">
              <div className="px-2 py-0.5 bg-violet-100 text-violet-800 rounded text-xs font-bold uppercase">Challenger</div>
              <span className="text-sm text-gray-500">v{challenger.model_version} — {challenger.calibration_year} Calibration</span>
            </div>
            <div className="mb-3">
              <span className="text-xs text-gray-500 uppercase">SCR Result</span>
              <div className="text-2xl font-bold text-gray-900">{formatEur(challenger.scr_result_eur)}</div>
            </div>
            <p className="text-sm text-gray-600">{challenger.description}</p>
            <div className="mt-3 text-xs text-gray-400">
              Registered by {challenger.registered_by} on {String(challenger.run_timestamp).split('T')[0]}
            </div>
          </div>
        )}
      </div>

      {/* Impact analysis */}
      {champion && challenger && (
        <div className="bg-white rounded-lg border border-gray-200 p-5">
          <div className="flex items-center gap-2 mb-4">
            <FlaskConical className="w-5 h-5 text-violet-500" />
            <h3 className="font-semibold text-gray-900">Impact Analysis: Challenger vs Champion</h3>
          </div>
          <div className="grid grid-cols-3 gap-6">
            <div>
              <span className="text-xs text-gray-500 uppercase">SCR Difference</span>
              <div className={`text-xl font-bold ${diff > 0 ? 'text-red-600' : 'text-green-600'}`}>
                {diff > 0 ? '+' : ''}{formatEur(diff)}
              </div>
            </div>
            <div>
              <span className="text-xs text-gray-500 uppercase">% Change</span>
              <div className={`text-xl font-bold ${diff > 0 ? 'text-red-600' : 'text-green-600'}`}>
                {diff > 0 ? '+' : ''}{diffPct}%
              </div>
            </div>
            <div>
              <span className="text-xs text-gray-500 uppercase">Key Changes in 2026</span>
              <ul className="text-sm text-gray-600 mt-1 space-y-0.5">
                <li>Market&harr;NL correlation: 0.25 &rarr; 0.30</li>
                <li>Op risk factor: 3.0% &rarr; 3.5%</li>
                <li>LAC_DT cap: 10% &rarr; 8%</li>
              </ul>
            </div>
          </div>
        </div>
      )}

      {/* Version history table */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-4 py-3 border-b bg-gray-50">
          <h3 className="font-semibold text-gray-900 text-sm">Version History</h3>
        </div>
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b bg-gray-50">
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Period</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Version</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Alias</th>
              <th className="px-4 py-2 text-left text-xs font-semibold text-gray-600 uppercase">Calibration</th>
              <th className="px-4 py-2 text-right text-xs font-semibold text-gray-600 uppercase">SCR (EUR)</th>
            </tr>
          </thead>
          <tbody>
            {versions.map((v, i) => (
              <tr key={i} className={`border-b border-gray-100 ${i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}>
                <td className="px-4 py-2 text-gray-800">{v.reporting_period}</td>
                <td className="px-4 py-2 text-gray-800">v{v.model_version}</td>
                <td className="px-4 py-2">
                  <span className={`px-2 py-0.5 rounded text-xs font-bold ${
                    v.alias === 'Champion' ? 'bg-green-100 text-green-800' : 'bg-violet-100 text-violet-800'
                  }`}>{v.alias}</span>
                </td>
                <td className="px-4 py-2 text-gray-800">{v.calibration_year}</td>
                <td className="px-4 py-2 text-right font-mono text-gray-800">{formatEur(v.scr_result_eur)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ═══════ Helpers ═══════ */
function Spinner() {
  return (
    <div className="flex justify-center py-12">
      <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
    </div>
  );
}

function Empty({ msg }: { msg: string }) {
  return (
    <div className="flex justify-center py-12 text-gray-400">{msg}</div>
  );
}
