const BASE = '';

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(`${BASE}${url}`);
  if (!res.ok) throw new Error(`API error: ${res.status} ${res.statusText}`);
  return res.json();
}

export function formatEur(value: number | string | null | undefined): string {
  if (value == null || value === '') return '—';
  const num = typeof value === 'string' ? parseFloat(value) : value;
  if (isNaN(num)) return '—';
  const abs = Math.abs(num);
  const sign = num < 0 ? '-' : '';
  if (abs >= 1_000_000_000) {
    return `${sign}EUR ${(abs / 1_000_000_000).toFixed(2)}B`;
  }
  if (abs >= 1_000_000) {
    return `${sign}EUR ${(abs / 1_000_000).toFixed(2)}M`;
  }
  if (abs >= 1_000) {
    return `${sign}EUR ${(abs / 1_000).toFixed(2)}K`;
  }
  return `${sign}EUR ${abs.toFixed(2)}`;
}

export function formatNumber(value: number | null | undefined): string {
  if (value == null) return '—';
  return value.toLocaleString('en-GB');
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null) return '—';
  return `${value.toFixed(1)}%`;
}

export function downloadFile(endpoint: string, filename: string) {
  const a = document.createElement('a');
  a.href = `${BASE}${endpoint}`;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

// Dashboard
export interface DashboardData {
  total_assets_sii: number;
  asset_count: number;
  gwp: number;
  nwp: number;
  net_claims: number;
  total_expenses: number;
  combined_ratio: number | null;
  scr: number;
  bscr: number;
  solvency_ratio: number | null;
  pipeline_last_run: string | null;
  dq_checks_total: number;
  dq_checks_passed: number;
}

export function fetchDashboard(): Promise<DashboardData> {
  return fetchJson('/api/dashboard');
}

// ─── S.06.02 ────────────────────────────────────────────────────────

export interface S0602Response {
  data: S0602Row[];
  total: number;
  page: number;
  page_size: number;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface S0602Row { [key: string]: any }

export function fetchS0602(params: {
  page?: number;
  page_size?: number;
  cic_filter?: string;
  country_filter?: string;
}): Promise<S0602Response> {
  const sp = new URLSearchParams();
  if (params.page) sp.set('page', String(params.page));
  if (params.page_size) sp.set('page_size', String(params.page_size));
  if (params.cic_filter) sp.set('cic_filter', params.cic_filter);
  if (params.country_filter) sp.set('country_filter', params.country_filter);
  return fetchJson(`/api/reports/s0602?${sp.toString()}`);
}

// ─── S.05.01 ────────────────────────────────────────────────────────

export interface S0501Row {
  template_row_id: string;
  template_row_label: string;
  lob_code: string;
  lob_label: string;
  amount_eur: string;
}

export async function fetchS0501(): Promise<S0501Row[]> {
  const resp = await fetchJson<{ data: S0501Row[] }>('/api/reports/s0501');
  return resp.data;
}

// ─── S.25.01 ────────────────────────────────────────────────────────

export interface S2501Row {
  template_row_id: string;
  template_row_label: string;
  c0110_gross_scr: string | null;
  c0100_net_scr: string | null;
}

export async function fetchS2501(): Promise<S2501Row[]> {
  const resp = await fetchJson<{ data: S2501Row[] }>('/api/reports/s2501');
  return resp.data;
}

// ─── Audit ──────────────────────────────────────────────────────────

export interface LineageRecord {
  step_sequence: string;
  step_name: string;
  source_tables: string;
  target_table: string;
  row_count_in: string;
  row_count_out: string;
  duration_seconds: string;
  status: string;
  executed_by: string;
  executed_at: string;
  [key: string]: string;
}

export async function fetchLineage(): Promise<LineageRecord[]> {
  const resp = await fetchJson<{ data: LineageRecord[] }>('/api/audit/lineage');
  return resp.data;
}

export interface QualityRecord {
  step_name: string;
  table_name: string;
  check_name: string;
  check_category: string;
  expected_value: string;
  actual_value: string;
  passed: string;
  severity: string;
  [key: string]: string;
}

export async function fetchQuality(): Promise<QualityRecord[]> {
  const resp = await fetchJson<{ data: QualityRecord[] }>('/api/audit/quality');
  return resp.data;
}

export interface TableRecord {
  table_name: string;
  row_count: number;
  [key: string]: string | number;
}

export async function fetchTables(): Promise<TableRecord[]> {
  const resp = await fetchJson<{ data: TableRecord[] }>('/api/audit/tables');
  return resp.data;
}

export function downloadCsv(endpoint: string, filename: string) {
  downloadFile(endpoint, filename);
}

export function downloadRegulatoryPackage() {
  downloadFile('/api/audit/regulatory-package', 'regulatory_package.zip');
}

// ─── Approvals ──────────────────────────────────────────────────────

export interface ApprovalRecord {
  approval_id: string;
  reporting_period: string;
  status: string;
  submitted_by: string;
  submitted_at: string;
  reviewed_by: string | null;
  reviewed_at: string | null;
  comments: string | null;
}

export async function fetchCurrentApproval(): Promise<ApprovalRecord | null> {
  const resp = await fetchJson<{ data: ApprovalRecord | null }>('/api/approvals/current');
  return resp.data ?? null;
}

export async function submitApproval(): Promise<ApprovalRecord> {
  const res = await fetch(`${BASE}/api/approvals/submit`, { method: 'POST' });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

export async function reviewApproval(
  status: 'approved' | 'rejected',
  comments: string
): Promise<ApprovalRecord> {
  const res = await fetch(`${BASE}/api/approvals/review`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ status, comments }),
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
