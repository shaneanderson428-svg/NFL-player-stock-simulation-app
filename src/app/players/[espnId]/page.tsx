import React from 'react';
import PlayerClient from './PlayerClient';

export default function Page({ params }: any) {
  // Keep this server component minimal: only pass the espnId from params to the client.
  const espnId = String((params && (params.espnId ?? '')) ?? '');
  return (
    <div>
      <PlayerClient espnId={espnId} />
    </div>
  );
}
