# Job Intel — GCC Job Intelligence Platform

Automated job dashboard for Saudi Arabia. Scrapes 351 government entities + 3 job boards daily at 9am GST.

## Setup
1. Upload all files to a GitHub repo
2. Settings → Pages → Branch: main → Save
3. Actions → Daily Job Scrape → Enable → Run workflow
4. Site auto-updates every morning at 9am GST

## Files
- `index.html` — Dashboard (fetches jobs.json on load)
- `jobs.json` — Job data (auto-updated by scraper)
- `scraper/scrape.py` — Python scraper
- `scraper/entities.json` — 351 Saudi government entities
- `scraper/requirements.txt` — Python dependencies
- `.github/workflows/daily-scrape.yml` — Daily cron job
