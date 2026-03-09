import { useEffect, useState, useMemo } from 'react';
import { Download, Loader2, ChevronLeft, ChevronRight } from 'lucide-react';
import DataTable, { type Column } from '../components/DataTable';
import {
  fetchS0602,
  fetchS0501,
  fetchS2501,
  downloadCsv,
  formatEur,
  type S0602Row,
  type S0602Response,
  type S0501Row,
  type S2501Row,
} from '../lib/api';

type Tab = 's0602' | 's0501' | 's2501';

const tabs: { id: Tab; label: string }[] = [
  { id: 's0602', label: 'S.06.02 Assets' },
  { id: 's0501', label: 'S.05.01 P&C' },
  { id: 's2501', label: 'S.25.01 SCR' },
];

export default function Reports() {
  const [activeTab, setActiveTab] = useState<Tab>('s0602');

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-4">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">QRT Reports</h2>
        <p className="text-sm text-gray-500 mt-1">Quantitative Reporting Templates</p>
      </div>

      <div className="flex items-center gap-1 border-b border-gray-200">
        {tabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setActiveTab(t.id)}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              activeTab === t.id
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {activeTab === 's0602' && <S0602Tab />}
      {activeTab === 's0501' && <S0501Tab />}
      {activeTab === 's2501' && <S2501Tab />}
    </div>
  );
}

/* ───── S.06.02 Assets ───── */
function S0602Tab() {
  const [resp, setResp] = useState<S0602Response | null>(null);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [cicFilter, setCicFilter] = useState('');
  const [countryFilter, setCountryFilter] = useState('');

  useEffect(() => {
    setLoading(true);
    fetchS0602({ page, page_size: 50, cic_filter: cicFilter, country_filter: countryFilter })
      .then(setResp)
      .finally(() => setLoading(false));
  }, [page, cicFilter, countryFilter]);

  // Use actual EIOPA column names from the gold table
  const columns: Column<S0602Row>[] = [
    { key: 'c0040_asset_id', header: 'Asset ID' },
    { key: 'c0190_item_title', header: 'Name' },
    { key: 'c0270_cic', header: 'CIC' },
    { key: 'c0260_currency', header: 'Currency' },
    { key: 'c0170_total_sii_amount', header: 'SII Value', render: (r) => formatEur(r.c0170_total_sii_amount), className: 'text-right' },
    { key: 'c0290_external_rating', header: 'Rating' },
    { key: 'c0250_issuer_country', header: 'Country' },
  ];

  const cicOptions = useMemo(() => {
    if (!resp) return [];
    return [...new Set(resp.data.map((r) => r.c0270_cic))].filter(Boolean).sort();
  }, [resp]);

  const countryOptions = useMemo(() => {
    if (!resp) return [];
    return [...new Set(resp.data.map((r) => r.c0250_issuer_country))].filter(Boolean).sort();
  }, [resp]);

  const totalPages = resp ? Math.ceil(resp.total / resp.page_size) : 1;

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200">
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
        <div className="flex items-center gap-3">
          <select
            id="cic-filter"
            className="text-sm border border-gray-300 rounded-md px-2 py-1.5 bg-white text-gray-700"
            value={cicFilter}
            onChange={(e) => { setCicFilter(e.target.value); setPage(1); }}
          >
            <option value="">All CIC codes</option>
            {cicOptions.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
          <select
            id="country-filter"
            className="text-sm border border-gray-300 rounded-md px-2 py-1.5 bg-white text-gray-700"
            value={countryFilter}
            onChange={(e) => { setCountryFilter(e.target.value); setPage(1); }}
          >
            <option value="">All countries</option>
            {countryOptions.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </div>
        <button
          onClick={() => downloadCsv('/api/reports/s0602/csv', 's0602_assets.csv')}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-blue-600 border border-blue-200 rounded-md hover:bg-blue-50 transition-colors"
        >
          <Download className="w-4 h-4" /> Download CSV
        </button>
      </div>

      {loading ? (
        <div className="flex justify-center py-12">
          <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
        </div>
      ) : (
        <>
          <DataTable columns={columns} data={resp?.data || []} rowKey={(_, i) => i} compact />
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200 text-sm text-gray-600">
            <span>
              Showing page {resp?.page || 1} of {totalPages} ({resp?.total || 0} total records)
            </span>
            <div className="flex items-center gap-2">
              <button
                disabled={page <= 1}
                onClick={() => setPage((p) => p - 1)}
                className="p-1 rounded hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed"
              >
                <ChevronLeft className="w-5 h-5" />
              </button>
              <span className="font-medium">{page}</span>
              <button
                disabled={page >= totalPages}
                onClick={() => setPage((p) => p + 1)}
                className="p-1 rounded hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed"
              >
                <ChevronRight className="w-5 h-5" />
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

/* ───── S.05.01 P&C ───── */

// Map template_row_id to section names for grouping
const ROW_SECTIONS: Record<string, string> = {
  R0110: 'Premiums Written',
  R0140: 'Premiums Written',
  R0200: 'Premiums Written',
  R0210: 'Premiums Earned',
  R0240: 'Premiums Earned',
  R0300: 'Premiums Earned',
  R0310: 'Claims Incurred',
  R0340: 'Claims Incurred',
  R0400: 'Claims Incurred',
  R0410: 'Claims Paid',
  R0440: 'Claims Paid',
  R0500: 'Claims Paid',
  R0510: 'Changes in Technical Provisions',
  R0540: 'Changes in Technical Provisions',
  R0550: 'Expenses',
  R0610: 'Expenses',
  R0620: 'Expenses',
  R0630: 'Expenses',
  R0640: 'Expenses',
  R0680: 'Expenses',
  R1200: 'Other / Total Expenses',
  R1300: 'Other / Total Expenses',
};

function S0501Tab() {
  const [data, setData] = useState<S0501Row[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchS0501()
      .then(setData)
      .finally(() => setLoading(false));
  }, []);

  const { sections, lobLabels } = useMemo(() => {
    const lobSet = new Set<string>();
    const sectionMap = new Map<string, { rowId: string; label: string; values: Map<string, number> }[]>();

    // Group by template_row_id first, then build section map
    const rowMap = new Map<string, Map<string, number>>();
    const rowLabels = new Map<string, string>();

    for (const row of data) {
      lobSet.add(row.lob_label);
      if (!rowMap.has(row.template_row_id)) rowMap.set(row.template_row_id, new Map());
      rowMap.get(row.template_row_id)!.set(row.lob_label, parseFloat(row.amount_eur));
      rowLabels.set(row.template_row_id, row.template_row_label);
    }

    // Build sections
    for (const [rowId, values] of rowMap.entries()) {
      const section = ROW_SECTIONS[rowId] || 'Other';
      if (!sectionMap.has(section)) sectionMap.set(section, []);
      sectionMap.get(section)!.push({
        rowId,
        label: rowLabels.get(rowId) || rowId,
        values,
      });
    }

    const lobLabels = [...lobSet].sort((a, b) => {
      if (a === 'Total') return 1;
      if (b === 'Total') return -1;
      return a.localeCompare(b);
    });

    const sections = [...sectionMap.entries()].map(([sectionName, rows]) => ({
      sectionName,
      rows,
    }));

    return { sections, lobLabels };
  }, [data]);

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200">
      <div className="flex items-center justify-end px-4 py-3 border-b border-gray-200">
        <button
          onClick={() => downloadCsv('/api/reports/s0501/csv', 's0501_premiums.csv')}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-blue-600 border border-blue-200 rounded-md hover:bg-blue-50 transition-colors"
        >
          <Download className="w-4 h-4" /> Download CSV
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
              <SectionBlock key={sec.sectionName} section={sec} lobLabels={lobLabels} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SectionBlock({
  section,
  lobLabels,
}: {
  section: { sectionName: string; rows: { rowId: string; label: string; values: Map<string, number> }[] };
  lobLabels: string[];
}) {
  return (
    <>
      <tr className="bg-gray-100 border-t-2 border-gray-300">
        <td colSpan={2 + lobLabels.length} className="px-3 py-2 text-xs font-bold text-gray-700 uppercase tracking-wide">
          {section.sectionName}
        </td>
      </tr>
      {section.rows.map((row, i) => (
        <tr key={`${row.rowId}-${i}`} className={`border-b border-gray-100 ${i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}>
          <td className="px-3 py-1.5 text-gray-500 font-mono text-xs">{row.rowId}</td>
          <td className="px-3 py-1.5 text-gray-800">{row.label}</td>
          {lobLabels.map((lob) => (
            <td key={lob} className={`px-3 py-1.5 text-right text-gray-800 font-mono ${lob === 'Total' ? 'bg-blue-50/50 font-semibold' : ''}`}>
              {row.values.has(lob) ? formatEur(row.values.get(lob)!) : '—'}
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

/* ───── S.25.01 SCR ───── */
function S2501Tab() {
  const [data, setData] = useState<S2501Row[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchS2501()
      .then(setData)
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex justify-center py-12">
        <Loader2 className="w-6 h-6 animate-spin text-blue-600" />
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200">
      <div className="flex items-center justify-end px-4 py-3 border-b border-gray-200">
        <button
          onClick={() => downloadCsv('/api/reports/s2501/csv', 's2501_scr.csv')}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-blue-600 border border-blue-200 rounded-md hover:bg-blue-50 transition-colors"
        >
          <Download className="w-4 h-4" /> Download CSV
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50">
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase w-24">Row ID</th>
              <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Description</th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase w-40">Net SCR (C0100)</th>
              <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase w-40">Gross SCR (C0110)</th>
            </tr>
          </thead>
          <tbody>
            {data.map((row, i) => {
              const isHighlight = row.template_row_id === 'R0200';
              const isBscr = row.template_row_id === 'R0100';
              return (
                <tr
                  key={`${row.template_row_id}-${i}`}
                  className={`border-b ${
                    isHighlight
                      ? 'bg-blue-50 border-blue-200 font-bold'
                      : isBscr
                        ? 'border-t-2 border-t-gray-300 bg-gray-50'
                        : i % 2 === 0
                          ? 'bg-white border-gray-100'
                          : 'bg-gray-50/50 border-gray-100'
                  }`}
                >
                  <td className="px-4 py-2.5 text-gray-500 font-mono text-xs">{row.template_row_id}</td>
                  <td className={`px-4 py-2.5 ${isHighlight ? 'text-blue-900' : 'text-gray-800'}`}>
                    {row.template_row_label}
                  </td>
                  <td className="px-4 py-2.5 text-right font-mono">
                    {formatEur(row.c0100_net_scr)}
                  </td>
                  <td className="px-4 py-2.5 text-right font-mono">
                    {formatEur(row.c0110_gross_scr)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
