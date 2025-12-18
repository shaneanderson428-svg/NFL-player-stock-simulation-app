import React from 'react'
import PlayerStockChart from '../../../src/components/PlayerStockChart'

type Props = {
  params: Promise<{ playerId: string }>
}

export default async function Page(props: Props) {
  const { playerId } = await props.params
  return (
    <div style={{ padding: 20 }}>
      <header style={{ marginBottom: 12 }}>
        <h1 style={{ margin: 0, fontSize: 22 }}>Player {playerId}</h1>
        <div style={{ color: '#666', fontSize: 13 }}>Name: (placeholder)</div>
      </header>

      <section>
        <div style={{ maxWidth: 960 }}>
          {/* PlayerStockChart is a client component that fetches /api/history/{playerId} */}
          <PlayerStockChart playerId={playerId} />
        </div>
      </section>
    </div>
  )
}

export const dynamic = 'force-dynamic'
