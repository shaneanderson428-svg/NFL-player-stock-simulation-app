import RecomputeButton from './RecomputeButton.client';

export default function Page() {
  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mb-4">Admin: Recompute Advanced Metrics (dev only)</h1>
      <p className="mb-4 text-sm text-gray-600">This page is intended for local development. It will run the compute script and return logs.</p>
      <RecomputeButton />
    </div>
  );
}
