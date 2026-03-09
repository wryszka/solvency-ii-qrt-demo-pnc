import { useEffect, useState } from 'react';
import {
  Loader2,
  ChevronDown,
  ChevronRight,
  Download,
  Package,
  FileText,
  Database,
} from 'lucide-react';
import DataTable, { type Column } from '../components/DataTable';
import StatusBadge from '../components/StatusBadge';
import {
  fetchLineage,
  fetchQuality,
  fetchTables,
  downloadCsv,
  downloadRegulatoryPackage,
  type LineageRecord,
  type QualityRecord,
  type TableRecord,
} from '../lib/api';

export default function Audit() {
  return (
    <div className="p-6 max-w-7xl mx-auto space-y-4">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Audit & Lineage</h2>
        <p className="text-sm text-gray-500 mt-1">Pipeline lineage, data quality checks, and regulatory export</p>
      </div>
      <LineageSection />
      <QualitySection />
      <ExportSection />
      <SchemaSection />
    </div>
  );
}

/* ───── Collapsible wrapper ───── */
function Section({
  title,
  icon: Icon,
  defaultOpen = true,
  children,
}: {
  title: string;
  icon: React.ComponentType<{ className?: string }>;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-3 px-5 py-4 text-left hover:bg-gray-50 transition-colors"
      >
        {open ? <ChevronDown className="w-4 h-4 text-gray-400" /> : <ChevronRight className="w-4 h-4 text-gray-400" />}
        <Icon className="w-5 h-5 text-gray-500" />
        <span className="font-semibold text-gray-800">{title}</span>
      </button>
      {open && <div className="border-t border-gray-200">{children}</div>}
    </div>
  );
}

/* ───── Pipeline Lineage ───── */
function LineageSection() {
  const [data, setData] = useState<LineageRecord[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchLineage()
      .then((d) => {
        const sorted = [...d].sort((a, b) => Number(a.step_sequence) - Number(b.step_sequence));
        setData(sorted);
      })
      .catch((e) => console.error('Lineage fetch failed:', e))
      .finally(() => setLoading(false));
  }, []);

  const columns: Column<LineageRecord>[] = [
    { key: 'step_sequence', header: '#', className: 'w-12' },
    { key: 'step_name', header: 'Step Name' },
    { key: 'source_tables', header: 'Source Tables' },
    { key: 'target_table', header: 'Target Table' },
    {
      key: 'row_count_in',
      header: 'Rows In/Out',
      render: (r) => (
        <span className="font-mono text-xs">
          {r.row_count_in ? Number(r.row_count_in).toLocaleString() : '—'} &rarr; {r.row_count_out ? Number(r.row_count_out).toLocaleString() : '—'}
        </span>
      ),
    },
    { key: 'duration_seconds', header: 'Duration', render: (r) => r.duration_seconds ? `${Number(r.duration_seconds).toFixed(1)}s` : '—' },
    {
      key: 'status',
      header: 'Status',
      render: (r) => (
        <StatusBadge
          label={r.status}
          variant={r.status === 'success' ? 'success' : 'error'}
        />
      ),
    },
    { key: 'executed_by', header: 'Executed By' },
    { key: 'executed_at', header: 'Executed At', render: (r) => r.executed_at ? new Date(r.executed_at).toLocaleString() : '—' },
  ];

  return (
    <Section title="Pipeline Lineage Flow" icon={Database}>
      {loading ? (
        <div className="flex justify-center py-8">
          <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
        </div>
      ) : (
        <div className="px-1">
          <DataTable columns={columns} data={data} compact rowKey={(r) => r.step_name} />
        </div>
      )}
    </Section>
  );
}

/* ───── Data Quality ───── */
function QualitySection() {
  const [data, setData] = useState<QualityRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterPassed, setFilterPassed] = useState<'' | 'true' | 'false'>('');

  useEffect(() => {
    fetchQuality()
      .then(setData)
      .catch((e) => console.error('Quality fetch failed:', e))
      .finally(() => setLoading(false));
  }, []);

  const filtered =
    filterPassed === ''
      ? data
      : data.filter((r) => String(r.passed) === filterPassed);

  const columns: Column<QualityRecord>[] = [
    { key: 'step_name', header: 'Step' },
    { key: 'table_name', header: 'Table', render: (r) => r.table_name?.split('.').pop() || r.table_name },
    { key: 'check_name', header: 'Check' },
    { key: 'check_category', header: 'Category' },
    { key: 'expected_value', header: 'Expected' },
    { key: 'actual_value', header: 'Actual' },
    {
      key: 'passed',
      header: 'Result',
      render: (r) => {
        const passed = String(r.passed) === 'true';
        return (
          <StatusBadge
            label={passed ? 'PASS' : 'FAIL'}
            variant={passed ? 'success' : 'error'}
          />
        );
      },
    },
    { key: 'severity', header: 'Severity' },
  ];

  return (
    <Section title="Data Quality Checks" icon={FileText}>
      {loading ? (
        <div className="flex justify-center py-8">
          <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
        </div>
      ) : (
        <>
          <div className="px-4 py-3 flex items-center gap-3 border-b border-gray-100">
            <select
              id="dq-filter"
              className="text-sm border border-gray-300 rounded-md px-2 py-1.5 bg-white text-gray-700"
              value={filterPassed}
              onChange={(e) => setFilterPassed(e.target.value as '' | 'true' | 'false')}
            >
              <option value="">All results</option>
              <option value="true">Passed only</option>
              <option value="false">Failed only</option>
            </select>
            <span className="text-sm text-gray-500">
              {filtered.length} of {data.length} checks shown
            </span>
          </div>
          <div className="px-1">
            <DataTable columns={columns} data={filtered} compact rowKey={(r, i) => `${r.check_name}-${i}`} />
          </div>
        </>
      )}
    </Section>
  );
}

/* ───── Regulatory Export ───── */
function ExportSection() {
  const downloads = [
    { label: 'Audit Trail (CSV)', endpoint: '/api/audit/lineage/csv', filename: 'audit_lineage.csv' },
    { label: 'Data Quality Log (CSV)', endpoint: '/api/audit/quality/csv', filename: 'dq_checks.csv' },
    { label: 'S.06.02 Report (CSV)', endpoint: '/api/reports/s0602/csv', filename: 's0602.csv' },
    { label: 'S.05.01 Report (CSV)', endpoint: '/api/reports/s0501/csv', filename: 's0501.csv' },
    { label: 'S.25.01 Report (CSV)', endpoint: '/api/reports/s2501/csv', filename: 's2501.csv' },
  ];

  return (
    <Section title="Regulatory Export" icon={Package}>
      <div className="p-5 space-y-4">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {downloads.map((d) => (
            <button
              key={d.endpoint}
              onClick={() => downloadCsv(d.endpoint, d.filename)}
              className="flex items-center gap-3 px-4 py-3 border border-gray-200 rounded-lg hover:border-blue-300 hover:bg-blue-50/50 transition-colors text-left"
            >
              <Download className="w-4 h-4 text-blue-600 flex-shrink-0" />
              <div>
                <p className="text-sm font-medium text-gray-800">{d.label}</p>
                <p className="text-xs text-gray-500">{d.filename}</p>
              </div>
            </button>
          ))}
        </div>

        <button
          onClick={() => downloadRegulatoryPackage()}
          className="w-full flex items-center justify-center gap-3 px-6 py-4 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-medium"
        >
          <Package className="w-5 h-5" />
          Download Full Regulatory Package (ZIP)
        </button>
      </div>
    </Section>
  );
}

/* ───── Schema Inventory ───── */
function SchemaSection() {
  const [data, setData] = useState<TableRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchTables()
      .then(setData)
      .catch((e) => { console.error('Tables fetch failed:', e); setError(String(e)); })
      .finally(() => setLoading(false));
  }, []);

  const columns: Column<TableRecord>[] = [
    { key: 'table_name', header: 'Table Name' },
    {
      key: 'row_count',
      header: 'Row Count',
      className: 'text-right',
      render: (r) => r.row_count != null ? Number(r.row_count).toLocaleString() : '—',
    },
  ];

  return (
    <Section title="Schema Inventory" icon={Database} defaultOpen={false}>
      {loading ? (
        <div className="flex justify-center py-8">
          <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
        </div>
      ) : error ? (
        <div className="px-5 py-4 text-sm text-red-600">Failed to load schema inventory: {error}</div>
      ) : (
        <div className="px-1">
          <DataTable columns={columns} data={data} compact rowKey={(r) => r.table_name} />
        </div>
      )}
    </Section>
  );
}
