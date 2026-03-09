import { useEffect, useState } from 'react';
import {
  Loader2,
  Send,
  CheckCircle2,
  XCircle,
  Clock,
  FileCheck,
} from 'lucide-react';
import StatusBadge from '../components/StatusBadge';
import {
  fetchCurrentApproval,
  submitApproval,
  reviewApproval,
  type ApprovalRecord,
} from '../lib/api';

export default function Approvals() {
  const [approval, setApproval] = useState<ApprovalRecord | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [reviewing, setReviewing] = useState(false);
  const [comments, setComments] = useState('');

  function load() {
    setLoading(true);
    setError(null);
    fetchCurrentApproval()
      .then(setApproval)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }

  useEffect(() => { load(); }, []);

  async function handleSubmit() {
    setSubmitting(true);
    try {
      const a = await submitApproval();
      setApproval(a);
    } catch (e: unknown) {
      setError((e as Error).message);
    } finally {
      setSubmitting(false);
    }
  }

  async function handleReview(status: 'approved' | 'rejected') {
    setReviewing(true);
    try {
      const a = await reviewApproval(status, comments);
      setApproval(a);
      setComments('');
    } catch (e: unknown) {
      setError((e as Error).message);
    } finally {
      setReviewing(false);
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
      </div>
    );
  }

  const statusVariant = !approval
    ? 'neutral'
    : approval.status === 'pending'
      ? 'warning'
      : approval.status === 'approved'
        ? 'success'
        : approval.status === 'rejected'
          ? 'error'
          : 'neutral';

  const statusLabel = !approval
    ? 'Not Submitted'
    : approval.status === 'pending'
      ? 'Awaiting Review'
      : approval.status === 'approved'
        ? 'Approved'
        : approval.status === 'rejected'
          ? 'Rejected'
          : approval.status;

  const statusIcon = !approval
    ? FileCheck
    : approval.status === 'pending'
      ? Clock
      : approval.status === 'approved'
        ? CheckCircle2
        : XCircle;

  const StatusIcon = statusIcon;

  return (
    <div className="p-6 max-w-4xl mx-auto space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Approvals</h2>
        <p className="text-sm text-gray-500 mt-1">QRT submission and review workflow</p>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {/* Current Status Card */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-start gap-5">
          <div
            className={`p-3 rounded-full ${
              statusVariant === 'success'
                ? 'bg-green-100 text-green-600'
                : statusVariant === 'error'
                  ? 'bg-red-100 text-red-600'
                  : statusVariant === 'warning'
                    ? 'bg-amber-100 text-amber-600'
                    : 'bg-gray-100 text-gray-500'
            }`}
          >
            <StatusIcon className="w-8 h-8" />
          </div>
          <div className="flex-1">
            <h3 className="text-lg font-semibold text-gray-900">Current Approval Status</h3>
            <div className="mt-2">
              <StatusBadge label={statusLabel} variant={statusVariant} />
            </div>

            {approval && (
              <div className="mt-3 space-y-1 text-sm text-gray-600">
                <p>
                  <span className="font-medium">Submitted by:</span> {approval.submitted_by}
                </p>
                <p>
                  <span className="font-medium">Submitted at:</span> {approval.submitted_at}
                </p>
                {approval.reporting_period && (
                  <p>
                    <span className="font-medium">Period:</span> {approval.reporting_period}
                  </p>
                )}
                {approval.reviewed_by && (
                  <p>
                    <span className="font-medium">Reviewed by:</span> {approval.reviewed_by} on{' '}
                    {approval.reviewed_at}
                  </p>
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

      {/* Actions */}
      {(!approval || approval.status === 'rejected') && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Submit for Approval</h3>
          <p className="text-sm text-gray-600 mb-4">
            Submit the current QRT reporting package for review. This will include all S.06.02, S.05.01,
            and S.25.01 reports along with data quality results.
          </p>
          <button
            onClick={handleSubmit}
            disabled={submitting}
            className="inline-flex items-center gap-2 px-5 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors font-medium"
          >
            {submitting ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Send className="w-4 h-4" />
            )}
            Submit for Approval
          </button>
        </div>
      )}

      {approval?.status === 'pending' && (
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Review Submission</h3>
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Comments</label>
              <textarea
                rows={3}
                value={comments}
                onChange={(e) => setComments(e.target.value)}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="Add review comments..."
              />
            </div>
            <div className="flex items-center gap-3">
              <button
                onClick={() => handleReview('approved')}
                disabled={reviewing}
                className="inline-flex items-center gap-2 px-5 py-2.5 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors font-medium"
              >
                {reviewing ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <CheckCircle2 className="w-4 h-4" />
                )}
                Approve
              </button>
              <button
                onClick={() => handleReview('rejected')}
                disabled={reviewing}
                className="inline-flex items-center gap-2 px-5 py-2.5 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors font-medium"
              >
                {reviewing ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <XCircle className="w-4 h-4" />
                )}
                Reject
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
