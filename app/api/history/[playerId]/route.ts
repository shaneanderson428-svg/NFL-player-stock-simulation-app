import { NextResponse } from 'next/server'
import { promises as fs } from 'fs'
import path from 'path'

export async function GET(_req: Request, ctx: { params: Promise<{ playerId: string }> }) {
  const { playerId } = (await ctx.params) || {}
  if (!playerId) {
    return NextResponse.json({ error: 'missing_playerId', message: 'playerId param is required' }, { status: 400 })
  }

  const filePath = path.join(process.cwd(), 'data', 'history', `${playerId}_price_history.json`)

  try {
    const raw = await fs.readFile(filePath, 'utf8')
    // Return raw JSON exactly as stored
    return new NextResponse(raw, {
      status: 200,
      headers: { 'content-type': 'application/json' },
    })
  } catch (err: any) {
    if (err && err.code === 'ENOENT') {
      return NextResponse.json(
        { error: 'not_found', message: `No price history found for playerId ${playerId}` },
        { status: 404 }
      )
    }
    return NextResponse.json({ error: 'server_error', message: String(err) }, { status: 500 })
  }
}

export const dynamic = 'force-dynamic'
