import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import {
  LayoutDashboard,
  FileText,
  ShieldCheck,
  ClipboardCheck,
  Building2,
} from 'lucide-react';
import Dashboard from './pages/Dashboard';
import Reports from './pages/Reports';
import Audit from './pages/Audit';
import Approvals from './pages/Approvals';

const navItems = [
  { to: '/', label: 'Dashboard', icon: LayoutDashboard },
  { to: '/reports', label: 'QRT Reports', icon: FileText },
  { to: '/audit', label: 'Audit & Lineage', icon: ShieldCheck },
  { to: '/approvals', label: 'Approvals', icon: ClipboardCheck },
];

export default function App() {
  return (
    <BrowserRouter>
      <div className="flex h-screen bg-gray-100 font-[system-ui]">
        {/* Sidebar */}
        <aside className="w-60 flex-shrink-0 bg-[#1e293b] text-white flex flex-col">
          <div className="px-5 py-6 border-b border-white/10">
            <h1 className="text-lg font-bold tracking-tight">Solvency II QRT</h1>
            <p className="text-xs text-gray-400 mt-0.5">Reporting & Approval</p>
          </div>

          <nav className="flex-1 px-3 py-4 space-y-1">
            {navItems.map(({ to, label, icon: Icon }) => (
              <NavLink
                key={to}
                to={to}
                end={to === '/'}
                className={({ isActive }) =>
                  `flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-colors ${
                    isActive
                      ? 'bg-blue-600 text-white'
                      : 'text-gray-300 hover:bg-white/10 hover:text-white'
                  }`
                }
              >
                <Icon className="w-4.5 h-4.5" />
                {label}
              </NavLink>
            ))}
          </nav>

          <div className="px-5 py-4 border-t border-white/10">
            <div className="flex items-center gap-2 text-xs text-gray-400">
              <Building2 className="w-4 h-4" />
              <div>
                <p className="font-medium text-gray-300">Europa Re Insurance SE</p>
                <p>Q4 2025 Reporting</p>
              </div>
            </div>
          </div>
        </aside>

        {/* Content */}
        <main className="flex-1 overflow-y-auto">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/reports" element={<Reports />} />
            <Route path="/audit" element={<Audit />} />
            <Route path="/approvals" element={<Approvals />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
