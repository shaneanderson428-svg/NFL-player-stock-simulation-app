const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

async function writeGzCsv(filename, csv) {
  const gz = zlib.gzipSync(Buffer.from(csv, 'utf8'));
  fs.writeFileSync(filename, gz);
  console.log('Wrote', filename);
}

function makeSampleCsv(rows) {
  const header = [
    'play_id','pass_attempt','complete_pass','complete_pass_prob','air_yards','yards_after_catch','yards_gained','epa','passer_player_id','rusher_player_id','receiver_player_id','target_player_id','rush_attempt','rush_yards','wp_before','wp','wp_post'
  ].join(',') + '\n';
  const lines = rows.map(r => header ? '' : '').join('\n');
  // build content from rows
  const body = rows.map(r => {
    return [
      r.play_id || '',
      r.pass_attempt || 0,
      r.complete_pass || 0,
      r.complete_pass_prob != null ? r.complete_pass_prob : '',
      r.air_yards != null ? r.air_yards : '',
      r.yards_after_catch != null ? r.yards_after_catch : '',
      r.yards_gained != null ? r.yards_gained : '',
      r.epa != null ? r.epa : '',
      r.passer_player_id || '',
      r.rusher_player_id || '',
      r.receiver_player_id || '',
      r.target_player_id || '',
      r.rush_attempt || 0,
      r.rush_yards != null ? r.rush_yards : '',
      r.wp_before != null ? r.wp_before : '',
      r.wp != null ? r.wp : '',
      r.wp_post != null ? r.wp_post : ''
    ].join(',');
  }).join('\n');

  return header + body + '\n';
}

async function main() {
  const outDir = path.join(process.cwd(), 'data', 'pbp');
  fs.mkdirSync(outDir, { recursive: true });

  const rows1 = [
    { play_id: 1, pass_attempt:1, complete_pass:1, complete_pass_prob:0.6, air_yards:12, yards_after_catch:8, yards_gained:20, epa:0.5, passer_player_id:3045146, receiver_player_id:4038944, target_player_id:4038944, rush_attempt:0, wp_before:0.5, wp:0.52, wp_post:0.53 },
    { play_id: 2, pass_attempt:1, complete_pass:0, complete_pass_prob:0.4, air_yards:25, yards_after_catch:0, yards_gained:0, epa:-0.6, passer_player_id:3045146, receiver_player_id:4431728, target_player_id:4431728, rush_attempt:0, wp_before:0.53, wp:0.48, wp_post:0.47 },
    { play_id: 3, pass_attempt:0, complete_pass:0, rush_attempt:1, rush_yards:5, rusher_player_id:3045146, yards_gained:5, epa:0.1, wp_before:0.47, wp:0.49, wp_post:0.495 }
  ];

  const rows2 = [
    { play_id: 1, pass_attempt:1, complete_pass:1, complete_pass_prob:0.7, air_yards:8, yards_after_catch:3, yards_gained:11, epa:0.4, passer_player_id:4038944, receiver_player_id:3045146, target_player_id:3045146, rush_attempt:0, wp_before:0.5, wp:0.53, wp_post:0.54 },
    { play_id: 2, pass_attempt:0, complete_pass:0, rush_attempt:1, rush_yards:2, rusher_player_id:4431728, yards_gained:2, epa:0.02, wp_before:0.54, wp:0.545, wp_post:0.546 },
  ];

  const csv1 = makeSampleCsv(rows1);
  const csv2 = makeSampleCsv(rows2);

  await writeGzCsv(path.join(outDir, 'demo_game1.csv.gz'), csv1);
  await writeGzCsv(path.join(outDir, 'demo_game2.csv.gz'), csv2);
}

main().catch(err => { console.error(err); process.exit(1); });
