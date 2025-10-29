Deployment notes — Vercel

Overview
--------
This project deploys best on Vercel (Next.js native). The build step will attempt to generate CSV data (player stock & cleaned profiles) by running the Python scripts present in `scripts/`.

Environment variables
---------------------
Add these in Vercel project settings → Environment Variables:

- ADVANCED_SECRET
- RAPID_API_KEY
- ESPN_API_KEY

Build behaviour
--------------
Vercel will run `npm run build` by default. `package.json` has been updated so the `build` script runs `node ./scripts/vercel_build.js` before `next build`.

vercel_build.js will:
- check for `python3` in the build environment
- if present, run:
  - `python3 scripts/compute_player_stock.py --input data/player_game_stats.csv --output data/player_stock_summary.csv`
  - `python3 scripts/clean_player_profiles.py`

If `python3` is not available on the chosen Vercel environment, the script logs a warning and the build continues — but charts that rely on generated CSVs may not have data. To ensure data generation during build, make sure to use a Vercel environment that includes Python (or vendor the generated CSVs into the repo or an object store).

Deploy steps
------------
1. Push your repo to GitHub (if not already).
2. In Vercel, import the repository and connect it to your GitHub repo.
3. Set the required Environment Variables in Project Settings.
4. Deploy. Vercel will run `npm run build` which runs the data generation step and then builds the Next app.

Notes
-----
- If you prefer deterministic builds, consider running the Python data-generation in CI and committing the CSV outputs or uploading them to S3 and fetching during build.
- For local testing, create and activate `.venv`, install `requirements.txt`, then run the compute scripts before `npm run dev`.
