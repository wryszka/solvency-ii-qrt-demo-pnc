import { BrowserRouter, Routes, Route, Link } from 'react-router-dom';
import { Building2, FileText } from 'lucide-react';
import ReportsList from './pages/ReportsList';
import ReportDetail from './pages/ReportDetail';

function Nav() {
  return (
    <header className="bg-[#1e293b] text-white">
      <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
        <Link to="/" className="flex items-center gap-3 hover:opacity-90 transition-opacity">
          <FileText className="w-6 h-6 text-blue-400" />
          <div>
            <h1 className="text-lg font-bold tracking-tight">Solvency II QRT</h1>
            <p className="text-xs text-gray-400">Reporting & Approval</p>
          </div>
        </Link>
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
            <Route path="/report/:qrtId" element={<ReportDetail />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  );
}
