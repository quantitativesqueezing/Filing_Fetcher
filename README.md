# FilingFetcher

FilingFetcher is a small Python application that leans on the [`john-friedman/secbrowser`](https://github.com/john-friedman/secbrowser) toolkit to watch the SEC's EDGAR system for fresh submissions from issuers listed on NASDAQ, NYSE, ARCA and AMEX. Each new filing is downloaded, parsed, and distilled into quick insights:

- A rule-based sentiment classification (bullish, bearish, neutral) with supporting rationale.
- An insider-trading materiality check for Form 4 filings that flags unplanned open-market trades.
- An "explain-like-I'm-five" summary for 8-K press releases that keeps the core disclosures intact.

## Getting started

1. **Install dependencies** – the `requirements.txt` file pins everything the app needs, including the secbrowser plugin:
ls -l start_dev.sh
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Set an SEC-compliant User-Agent** – the SEC expects a contactable User-Agent string. You can provide it in one of two ways:

   ```bash
   export SEC_USER_AGENT="FilingFetcher/0.1 (your-name@example.com)"
   ```

   or pass `--user-agent` when running the watcher.

3. **Run the monitor** – the CLI uses `Portfolio.monitor_submissions` from secbrowser/datamule so it will keep streaming until you interrupt it:

   ```bash
   python -m filingfetcher --poll 60 --validate 600
   ```

   Useful optional flags:

   - `--reporter json` to emit newline-delimited JSON instead of human-readable text.
   - `--exchanges NASDAQ,NYSE` to narrow the exchange filter.
   - `--quiet` to silence secbrowser's progress output.

## Output

By default the console reporter prints a compact block for each filing, showing:

- Timestamp, company, tickers, exchanges, filing type, and sentiment score.
- The SEC text link for the raw submission.
- Rationale for the bullish/bearish/neutral classification.
- Insider-trade verdicts for Form 4s.
- An ELI5 summary for 8-Ks plus noteworthy sentences that triggered keyword matches.

Switching to the JSON reporter provides the same data in machine-friendly form that can be piped into other systems.

## Project layout

```
filingfetcher/
└── filingfetcher/
    ├── analysis.py      # Sentiment, insider trade checks, and 8-K summarisation.
    ├── cli.py           # Argument parsing and bootstrap.
    ├── fetcher.py       # Downloads and parses filings via secbrowser tooling.
    ├── metadata.py      # Loads exchange/ticker metadata from secbrowser datasets.
    ├── monitor.py       # Glue around Portfolio.monitor_submissions.
    ├── reporters.py     # Console and JSON output adapters.
    ├── utils.py         # Text-cleaning helpers.
    └── ...
```

## Notes

- The heuristics are intentionally transparent and deterministic; refine the keyword lists or Form 4 rules as needed for your workflow.
- Monitoring relies on both the RSS feed and EFTS validation path supplied by `Portfolio.monitor_submissions`, which balances speed with completeness.
- Large filings (e.g., 10-Ks) can be heavyweight; the analyzer currently looks at the primary document only to stay responsive.

