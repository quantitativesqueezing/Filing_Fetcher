"""
Command line entry point for the FilingFetcher application.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from contextlib import suppress
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from .analysis import FilingAnalyzer
from .fetcher import FilingContentFetcher
from .metadata import MetadataRepository
from .monitor import FilingMonitor
from .reporters import ConsoleReporter, DiscordReporter, JsonReporter, load_discord_webhooks
from .discord_threads import DiscordThreadPoller

DEFAULT_EXCHANGES = ("NASDAQ", "NYSE", "ARCA", "AMEX")
DEFAULT_USER_AGENT = os.environ.get("SEC_USER_AGENT", "FilingFetcher/0.1 (contact@example.com)")

LOGGER = logging.getLogger(__name__)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor newly posted SEC filings and summarize them.")
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent header to use for SEC requests (default: %(default)s)",
    )
    parser.add_argument(
        "--exchanges",
        default=",".join(DEFAULT_EXCHANGES),
        help="Comma separated list of exchanges to monitor (default: %(default)s)",
    )
    parser.add_argument(
        "--portfolio-path",
        default=".filingfetcher/portfolio",
        help="Directory used by secbrowser/datamule to cache submissions (default: %(default)s)",
    )
    parser.add_argument(
        "--poll",
        type=int,
        default=60,
        help="Polling interval for the RSS feed in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--validate",
        type=int,
        default=600,
        help="EFTS validation interval in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--reporter",
        choices=("console", "json", "discord"),
        default="console",
        help="Reporting format (default: %(default)s)",
    )
    parser.add_argument(
        "--discord-mode",
        choices=("prod", "test"),
        default="prod",
        help="Which Discord webhook mapping to use (default: %(default)s)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Use DISCORD_WEBHOOK_TEST_URL webhooks and Discord thread polling.",
    )
    parser.add_argument(
        "--discord-template",
        default=os.environ.get("DISCORD_WEBHOOK_TEMPLATE"),
        help="Optional path to a JSON Discord webhook payload template.",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("FILINGFETCHER_LOGLEVEL", "INFO"),
        help="Python logging level (default: %(default)s)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress secbrowser progress output.",
    )
    return parser


def configure_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    configure_logging(args.log_level)

    if "example.com" in args.user_agent or "contact@example.com" in args.user_agent:
        LOGGER.warning("Please set --user-agent or SEC_USER_AGENT with your contact information per SEC guidelines.")

    exchanges = [exchange.strip() for exchange in args.exchanges.split(",") if exchange.strip()]
    metadata_repo = MetadataRepository()
    fetcher = FilingContentFetcher(user_agent=args.user_agent)
    analyzer = FilingAnalyzer()
    discord_mode = "test" if args.test else args.discord_mode

    webhook_mapping = load_discord_webhooks("test" if discord_mode == "test" else "prod")

    if args.reporter == "console":
        reporter = ConsoleReporter()
    elif args.reporter == "json":
        reporter = JsonReporter()
    else:
        template_path = Path(args.discord_template) if args.discord_template else None
        reporter = DiscordReporter(webhook_mapping=webhook_mapping, template_path=template_path)

    monitor = FilingMonitor(
        metadata_repository=metadata_repo,
        fetcher=fetcher,
        analyzer=analyzer,
        reporter=reporter,
        target_exchanges=exchanges,
        portfolio_path=args.portfolio_path,
        polling_interval_seconds=args.poll,
        validation_interval_seconds=args.validate,
        quiet=args.quiet,
    )

    bot_token = os.environ.get("DISCORD_BOT_TOKEN") or os.environ.get("DISCORD_API_TOKEN")
    poller = DiscordThreadPoller(webhook_mapping, poll_interval_seconds=60, bot_token=bot_token, stream=sys.stdout)

    stop_requested = False

    def _handle_signal(signum, frame):  # pragma: no cover - signal handling
        nonlocal stop_requested
        if stop_requested:
            return
        stop_requested = True
        LOGGER.info("Signal %s received; shutting down...", signum)
        fetcher.close()
        sys.exit(0)

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(Exception):
            signal.signal(sig, _handle_signal)

    poller.poll_once()
    poller.start()

    try:
        monitor.start()
    finally:
        poller.stop()
        fetcher.close()

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
