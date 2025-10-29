"use client";
import { useState } from 'react';

export default function RecomputeButton() {
  const [running, setRunning] = useState(false);
  const [logs, setLogs] = useState('');
  const [exitCode, setExitCode] = useState<number | null>(null);
  const [flush, setFlush] = useState<any>(null);
  const [playersUrlState, setPlayersUrlState] = useState<string | null>(null);
  const [playersHtmlPreview, setPlayersHtmlPreview] = useState<string | null>(null);

  async function run() {
    setRunning(true);
    setLogs('');
    setExitCode(null);
    try {
      const res = await fetch('/api/admin/recompute-ui', { method: 'POST' });
      const json = await res.json();
      if (json.ok) {
        setLogs(json.logs || '');
        setExitCode(json.exitCode ?? 0);
          setFlush(json.flush ?? null);
          setPlayersUrlState(json.playersUrl ?? null);
          setPlayersHtmlPreview(json.playersHtml ? String(json.playersHtml).slice(0, 2048) : null);
      } else {
        setLogs(json.error || 'Unknown error');
        setExitCode(-1);
      }
    } catch (e: any) {
      setLogs(String(e?.message ?? e));
      setExitCode(-1);
    } finally {
      setRunning(false);
    }
  }

  return (
    <div>
      <button onClick={run} disabled={running} className="px-4 py-2 bg-blue-600 text-white rounded">
        {running ? 'Running…' : 'Run recompute'}
      </button>
      <div className="mt-4">
        <strong>Exit code:</strong> {exitCode === null ? '—' : String(exitCode)}
      </div>
      <div className="mt-2">
        <strong>Flush result:</strong>
        <div className="mt-1 text-xs bg-gray-50 p-2 rounded">{flush ? JSON.stringify(flush, null, 2) : '—'}</div>
      </div>
      <div className="mt-4">
        <strong>Players page:</strong>
        <div className="mt-1">
          <a href={playersUrlState ?? '#'} target="_blank" rel="noreferrer" className="text-blue-600 underline mr-4">{playersUrlState ?? '—'}</a>
          {playersUrlState ? (
            <button onClick={() => window.open(playersUrlState!, '_blank')} className="px-2 py-1 bg-gray-200 rounded text-sm">Open players page</button>
          ) : null}
        </div>
        {playersHtmlPreview ? (
          <pre className="mt-2 p-2 bg-black text-white text-xs rounded max-h-64 overflow-auto">{playersHtmlPreview}</pre>
        ) : null}
      </div>
      <pre className="mt-2 p-2 bg-gray-100 rounded max-h-64 overflow-auto text-xs">{logs}</pre>
    </div>
  );
}
