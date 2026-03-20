import { useEffect, useState } from 'react';
import { Loader2 } from 'lucide-react';
import { fetchEmbeds } from '../lib/api';

export default function Dashboard() {
  const [url, setUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchEmbeds()
      .then((e) => setUrl(e.dashboard_url))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-[calc(100vh-64px)]">
        <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
      </div>
    );
  }

  if (error || !url) {
    return (
      <div className="max-w-4xl mx-auto p-6">
        <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-red-700">
          Failed to load dashboard: {error || 'No URL available'}
        </div>
      </div>
    );
  }

  return (
    <iframe
      src={url}
      className="w-full border-0"
      style={{ height: 'calc(100vh - 64px)' }}
      title="QRT Comparison Dashboard"
      allow="fullscreen"
    />
  );
}
