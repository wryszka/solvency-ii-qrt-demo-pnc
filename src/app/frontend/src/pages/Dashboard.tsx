import { useEffect, useState } from 'react';
import { Loader2, ExternalLink } from 'lucide-react';
import { fetchEmbeds } from '../lib/api';

export default function Dashboard() {
  const [url, setUrl] = useState<string | null>(null);
  const [directUrl, setDirectUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [iframeError, setIframeError] = useState(false);

  useEffect(() => {
    fetchEmbeds()
      .then((e) => {
        setUrl(e.dashboard_url);
        setDirectUrl(e.dashboard_url.replace('/embed/dashboardsv3/', '/dashboardsv3/'));
      })
      .catch(() => setIframeError(true))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-[calc(100vh-56px)]">
        <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
      </div>
    );
  }

  if (iframeError || !url) {
    return (
      <div className="max-w-4xl mx-auto p-6">
        <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-3 text-amber-700">
          Could not load dashboard embed. {directUrl && (
            <a href={directUrl} target="_blank" rel="noopener noreferrer"
               className="underline font-medium">Open in new tab</a>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="relative" style={{ height: 'calc(100vh - 56px)' }}>
      {directUrl && (
        <a href={directUrl} target="_blank" rel="noopener noreferrer"
           className="absolute top-2 right-2 z-10 inline-flex items-center gap-1 px-2.5 py-1.5 bg-white/90 border border-gray-200 rounded-md text-xs text-gray-600 hover:text-blue-600 hover:border-blue-300 shadow-sm transition-colors"
        >
          <ExternalLink className="w-3 h-3" /> New tab
        </a>
      )}
      <iframe
        src={url}
        className="w-full h-full border-0"
        title="QRT Comparison Dashboard"
        allow="fullscreen"
      />
    </div>
  );
}
