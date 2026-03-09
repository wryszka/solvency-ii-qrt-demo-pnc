interface MetricCardProps {
  title: string;
  value: string;
  subtitle?: string;
  accent?: 'blue' | 'green' | 'red' | 'amber' | 'gray';
}

const accentColors: Record<string, string> = {
  blue: 'border-l-blue-600',
  green: 'border-l-green-600',
  red: 'border-l-red-600',
  amber: 'border-l-amber-500',
  gray: 'border-l-gray-400',
};

export default function MetricCard({ title, value, subtitle, accent = 'blue' }: MetricCardProps) {
  return (
    <div
      className={`bg-white rounded-lg shadow-sm border border-gray-200 border-l-4 ${accentColors[accent]} p-5`}
    >
      <p className="text-sm font-medium text-gray-500 uppercase tracking-wide">{title}</p>
      <p className="mt-2 text-2xl font-bold text-gray-900">{value}</p>
      {subtitle && <p className="mt-1 text-sm text-gray-500">{subtitle}</p>}
    </div>
  );
}
