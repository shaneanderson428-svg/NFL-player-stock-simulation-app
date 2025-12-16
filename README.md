This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Fetch NFL play-by-play data (nflfastR)

This repo includes a small R script to download nflfastR play-by-play data and write per-season files into `data/pbp/`.

You can run it locally if you have R installed:

```bash
# fetch last two seasons by default
Rscript scripts/fetch_pbp.R

# or fetch specific seasons
Rscript scripts/fetch_pbp.R 2018 2019 2020 2021 2022 2023
```

If you don't have R installed, you can run the same script in Docker (the project includes a Dockerfile for this):

```bash
# make the helper executable first (one-time)
chmod +x scripts/run_fetch_pbp.sh

# run via Docker; pass season years as args like the Rscript command
./scripts/run_fetch_pbp.sh 2018 2019 2020 2021 2022 2023
```

Outputs will be placed in `data/pbp/` as Parquet files (if arrow is available) or gzipped CSV otherwise.

## Python / Scripts setup

Some utility scripts in `scripts/` (for example `scripts/clean_player_profiles.py`) require Python dependencies.

Recommended setup:

```bash
# create a venv named .venv (one-time)
python3 -m venv .venv

# activate the venv
source .venv/bin/activate

# install Python dependencies from requirements.txt
pip install -r requirements.txt
```

Once dependencies are installed the cleaner can be run with:

```bash
python3 scripts/clean_player_profiles.py
```


## Manual weekly update (MVP)

This project ships as a file-based, manually-run weekly pipeline. No background jobs or live polling are required. Follow these steps to perform the weekly update (manual, once per week):

1. Ensure you have a valid RapidAPI key available in your shell environment:

```bash
export RAPIDAPI_KEY=your_key_here
```

2. Make the runner executable (one-time):

```bash
chmod +x scripts/run_weekly_update.sh
```

3. Run the pipeline for the season and week you want to update (example: 2025 week 7):

```bash
./scripts/run_weekly_update.sh 2025 7
```

What it does:
- Fetches boxscores and player stats for the requested week
- Computes advanced metrics
- Computes player stock/site-ready CSV outputs (or falls back to price update script)
- Writes a small JSON summary file to `data/weekly_update_summary_<SEASON>_w<WEEK>.json`

Notes and constraints:
- Manual-only process: do not schedule or enable background polling unless you intentionally add CI automation.
- No new external APIs from the frontend: the site only reads generated CSV/JSON artifacts under `external/` and `data/`.
- If you hit rate limits while fetching data, check your `RAPIDAPI_KEY` quota. The scripts include some retry/backoff but heavy rate-limiting will require a higher quota or slower manual runs.


## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
