import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom';
import { Building2, FileText, BarChart3, MessageCircle, Activity, ShieldCheck } from 'lucide-react';
import Monitor from './pages/Monitor';
import ReportsList from './pages/ReportsList';
import ReportDetail from './pages/ReportDetail';
import DataQuality from './pages/DataQuality';
import Dashboard from './pages/Dashboard';
import Genie from './pages/Genie';

function NavLink({ to, icon: Icon, label }: { to: string; icon: React.ComponentType<{ className?: string }>; label: string }) {
  const { pathname } = useLocation();
  const active = to === '/' ? pathname === '/' : pathname.startsWith(to);
  return (
    <Link
      to={to}
      className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
        active ? 'bg-white/10 text-white' : 'text-gray-400 hover:text-white hover:bg-white/5'
      }`}
    >
      <Icon className="w-4 h-4" />
      {label}
    </Link>
  );
}

function Nav() {
  return (
    <header className="bg-[#1e293b] text-white">
      <div className="max-w-7xl mx-auto px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-6">
          <Link to="/" className="flex items-center gap-3 hover:opacity-90 transition-opacity">
            <FileText className="w-6 h-6 text-blue-400" />
            <div>
              <h1 className="text-lg font-bold tracking-tight">Solvency II QRT</h1>
              <p className="text-xs text-gray-400">Reporting & Approval</p>
            </div>
          </Link>
          <nav className="flex items-center gap-1 ml-4">
            <NavLink to="/monitor" icon={Activity} label="Monitor" />
            <NavLink to="/" icon={FileText} label="Reports" />
            <NavLink to="/data-quality" icon={ShieldCheck} label="Data Quality" />
            <NavLink to="/dashboard" icon={BarChart3} label="Dashboard" />
            <NavLink to="/genie" icon={MessageCircle} label="Ask Genie" />
          </nav>
        </div>
        <div className="flex items-center gap-2 text-sm text-gray-400">
          <Building2 className="w-4 h-4" />
          <span className="font-medium text-gray-300">Bricksurance SE</span>
        </div>
      </div>
    </header>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-100 font-[system-ui]">
        <Nav />
        <main>
          <Routes>
            <Route path="/" element={<ReportsList />} />
            <Route path="/monitor" element={<Monitor />} />
            <Route path="/report/:qrtId" element={<ReportDetail />} />
            <Route path="/data-quality" element={<DataQuality />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/genie" element={<Genie />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
