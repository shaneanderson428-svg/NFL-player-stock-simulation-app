"use client";
import { useState } from 'react';

export default function Page() {
  const [espnId, setEspnId] = useState('');
  const [fileDataUrl, setFileDataUrl] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);

  const onFile = (f?: File) => {
    if (!f) return setFileDataUrl(null);
    const reader = new FileReader();
    reader.onload = () => setFileDataUrl(String(reader.result ?? null));
    reader.readAsDataURL(f);
  };

  const submit = async () => {
    setStatus('Uploading...');
    try {
      const res = await fetch('/api/admin/avatars/upload', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ espnId, dataUrl: fileDataUrl }) });
      const j = await res.json();
      if (j.ok) {
        setStatus(`Uploaded: ${j.url}`);
      } else {
        setStatus(`Error: ${j.error}`);
      }
    } catch (e: any) {
      setStatus(`Upload error: ${String(e?.message ?? e)}`);
    }
  };

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-4">Dev: Upload Avatar</h1>
      <p className="mb-2 text-sm text-gray-600">Select an image and provide the ESPN id to save it under <code>/public/avatars/{'{espnId}'}.</code></p>
      <div className="mb-3">
        <label className="block mb-1">ESPN id</label>
        <div className="flex gap-2">
          <input value={espnId} onChange={(e) => setEspnId(e.target.value)} className="border p-2 rounded w-full" />
          <button type="button" onClick={() => setEspnId('3917')} className="px-3 py-2 bg-gray-200 rounded">Suggest McCaffrey (3917)</button>
        </div>
      </div>
      <div className="mb-3">
        <label className="block mb-1">Image</label>
        <input type="file" accept="image/*" onChange={(e) => onFile(e.target.files?.[0])} />
      </div>
      <div className="mb-3">
        <button onClick={submit} disabled={!espnId || !fileDataUrl} className="px-4 py-2 bg-blue-600 text-white rounded">Upload</button>
      </div>
      {status ? <div className="mt-2">{status}</div> : null}
      {fileDataUrl ? <img src={fileDataUrl} alt="preview" className="mt-4 w-32 h-32 object-cover rounded" /> : null}
    </div>
  );
}
