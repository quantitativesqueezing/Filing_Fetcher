# Filing_Fetcher
A lightweight Python utility that polls the SEC EDGAR Latest Filings Atom feed, converts each entry to JSON, and reuses the local `sec_feed.parser` helpers to enumerate the document set for every new accession number it sees.

## Getting started

1. **Install dependencies**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Provide a User-Agent** – SEC.gov blocks undeclared automated tools. Either export it once:

   ```bash
   export SEC_USER_AGENT="Filing_Fetcher/0.1 (your-name@example.com)"
   ```

   or pass `--user-agent` on the command line. Pick something unique and reachable. Placeholder examples such as `contact@example.com` are rejected by the watcher to keep you compliant.

3. **Run the watcher** – it polls the Atom feed every 7 seconds (the SEC-recommended cadence) and throttles document downloads so the request rate stays well below the 10 rps ceiling:

   ```bash
   python scripts/watch_sec_filings.py --user-agent "Filing_Fetcher/0.1 (name@example.com)"
   ```

   Useful flags:

   - `--compact` emits one-line JSON blocks that are easy to pipe into `jq`.
   - `--count 100` asks the feed for more rows per poll if you need deeper backfill.
   - `--doc-delay 1.0` widens the pause between filing downloads if you want to be extra conservative.
   - `--discord --discord-bot-token $DISCORD_BOT_TOKEN` turns on webhook dispatch (optionally pointing to test channels with `--test`), automatically routes filings into ticker-specific forum threads named like `$AAPL`, and deduplicates posts via `--discord-log`.

## Output

Each processed filing produces a JSON object on stdout:

- `feed_entry` is the RSS/Atom entry converted to JSON (company, CIK, role, ticker, summary, accession, etc.).
- `feed_entry.links` contains a pre-rendered HTML snippet with shortcuts to the SEC filing and a ticker-specific Twitter search, suitable for Discord embeds or web dashboards.
- `documents` is the structure returned by `sec_feed.parser.fetch_index_documents` (accession number, index URL, parsed document table, and the complete submission link).
- `ndjson` contains newline-delimited JSON strings for each document row so you can stream them elsewhere without reparsing.
- `summary` is a concise synopsis (≤2 sentences) extracted from the filing's primary document; `signal` is a placeholder structure (`label`, `reason`) ready for bullish/bearish heuristics you can add later.

Redirect stdout if you want to persist the feed, e.g. `python scripts/watch_sec_filings.py ... > filings.jsonl`.

## Other utilities

- `scripts/fetch_sec_docs.py` still accepts one or more explicit `*-index.htm` URLs and prints the parsed table plus NDJSON to stderr. This is handy for debugging a specific accession number without running the watcher loop.

## Project layout

```
Filing_Fetcher/
├── scripts/
│   ├── fetch_sec_docs.py      # Manual helper for specific index URLs.
│   └── watch_sec_filings.py   # Atom feed poller + parser bridge.
└── src/
    └── sec_feed/
        ├── __init__.py
        └── parser.py          # Document table parser reused by both scripts.
```

## Notes

- `watch_sec_filings.py` keeps a cache of accession numbers it has already processed, reverses the queue so older unseen filings are handled first, and honors the feed's `ETag`/`Last-Modified` headers to avoid unnecessary downloads. If the SEC ever returns HTTP 403 (usually because of an invalid/placeholder User-Agent or excessive traffic), the poller now logs a clear message and automatically pauses for a longer cooldown so you do not hammer the endpoint while you fix the issue.
- Set the `SEC_USER_AGENT` environment variable once and both scripts will pick it up automatically. You can still override it per run via `--user-agent`.
- Ticker symbols are sourced from the SEC's official `company_tickers.json` dataset and attached to every emission; if a filing is for an entity without a listed ticker the JSON will include `"ticker": null` explicitly.
- When multiple Atom entries exist for the same accession (e.g., "Filed by" and "Subject" pairs for Schedule 13D/G forms), the watcher now prioritises the "Subject" record so the `ticker` always reflects the company the filing is catalogued under rather than the filer.
- The optional `--discord` workflow now accepts `--discord-log` (default `webhook_post_log.json`) and skips any filing whose SEC document URL was already posted to a given webhook/thread, preventing duplicate spam across restarts.
- When a webhook mapping supplies a channel/thread ID (see `.env`), the dispatcher treats it as a Discord Forum channel: it uses the provided bot token to enumerate threads, finds any `$TICKER` threads that match the filing’s symbol, and posts the embed there too (without duplicating messages that already contain the filing URL).
- If you are piping the JSON into another service, consider adding your own backoff/retry layer for the downstream webhook call so the poller can stay focused on feeding you fresh SEC data.
