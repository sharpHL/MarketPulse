#!/usr/bin/env python3
"""Fetch and persist A-share market snapshot mapped to board hierarchy.

The script downloads real-time market data, enriches it with board metadata
from ``board_tree.json``, and stores the resulting dataframe as a Parquet
file partitioned by trading date and capture time (HHMM).
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from chinese_calendar import is_workday
from zoneinfo import ZoneInfo
from urllib3.util.retry import Retry
import time as time_module

LOGGER = logging.getLogger(__name__)

API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
API_FIELDS = "f2,f3,f5,f6,f12,f13,f14,f20"
API_FS = "m:0+t:6,m:0+t:13,m:0+t:80,m:0+t:81,m:0+t:82,m:1+t:2,m:1+t:23"
# SNAPSHOT_TIMES = ("09:30", "10:00", "10:30", "11:00", "11:30", "13:30", "14:00", "14:40", "15:00")
SNAPSHOT_TIMES = ("09:30", "11:00", "14:00", "15:30")

MARKET_SUFFIXES = {"0": "SZ", "1": "SH", "116": "BJ", "105": "HK", "106": "US"}
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/"
}
CN_TZ = ZoneInfo("Asia/Shanghai")



def build_session(retries: int = 3, backoff_factor: float = 0.3) -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


@dataclass
class BoardMeta:
    """Metadata helper describing how a stock maps to the board hierarchy."""

    full_code: str
    board: Optional[str]
    sub_board: Optional[str]
    tree_name: str

    @property
    def base_code(self) -> str:
        return self.full_code.split(".")[0]


def _iter_board_leaf_nodes(board_tree: Iterable[dict]) -> Iterable[BoardMeta]:
    """Yield :class:`BoardMeta` objects for each stock leaf in the tree."""

    def traverse(node: dict, level: int, top: Optional[str], sub: Optional[str]):
        children = node.get("children")
        name = node.get("name")
        code = node.get("code")

        if children:
            next_top = top
            next_sub = sub
            if level == 0:
                next_top = name
                next_sub = None
            elif level == 1:
                next_top = top or name
                next_sub = name
            for child in children:
                if child is None:
                    continue
                yield from traverse(child, level + 1, next_top, next_sub)
            return

        if not code:
            return

        yield BoardMeta(
            full_code=code,
            board=top or name,
            sub_board=sub or top or name,
            tree_name=name,
        )

    for root in board_tree:
        yield from traverse(root, level=0, top=None, sub=None)


def load_board_lookup(board_tree_path: Path) -> pd.DataFrame:
    """Load ``board_tree.json`` and return a dataframe keyed by ``base_code``."""
    board_tree = json.loads(board_tree_path.read_text(encoding="utf-8"))
    records = [
        {
            "base_code": meta.base_code,
            "full_code": meta.full_code,
            "board": meta.board,
            "sub_board": meta.sub_board,
            "tree_name": meta.tree_name,
        }
        for meta in _iter_board_leaf_nodes(board_tree)
    ]
    board_df = pd.DataFrame.from_records(records)
    duplicated = board_df.duplicated(subset=["base_code"], keep=False)
    if duplicated.any():
        dup_codes = board_df.loc[duplicated, "base_code"].unique()
        LOGGER.warning("Duplicate base codes discovered in board tree: %s", ", ".join(dup_codes))
    board_df = board_df.drop_duplicates(subset="base_code", keep="first")
    return board_df.set_index("base_code")


def fetch_market_snapshot(
    page_size: int = 500,
    throttle_seconds: float = 0.3,
    max_failures: int = 3,
    min_page_size: int = 100,
    max_total_failures: int = 10,
    session: Optional[requests.Session] = None,
) -> pd.DataFrame:
    """Fetch the real-time market snapshot for all A-shares with pagination."""
    if page_size <= 0:
        raise ValueError("page_size must be positive")
    if max_failures < 0:
        raise ValueError("max_failures must be non-negative")
    if min_page_size <= 0:
        min_page_size = 1
    min_page_size = min(min_page_size, page_size)

    current_page_size = page_size
    total_failures = 0
    params = {
        "pn": "1",
        "pz": str(current_page_size),
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": API_FS,
        "fields": API_FIELDS,
    }

    owns_session = session is None
    sess = session or build_session(retries=max_failures if max_failures > 0 else 1)
    frames: List[pd.DataFrame] = []
    page = 1

    try:
        while True:
            params["pn"] = str(page)
            params["pz"] = str(current_page_size)
            attempt = 0

            while True:
                LOGGER.debug(
                    "Requesting market snapshot page %s (page size=%s, attempt=%s)",
                    page,
                    current_page_size,
                    attempt + 1,
                )
                try:
                    response = sess.get(API_URL, params=params, headers=HEADERS, timeout=15)
                    response.raise_for_status()
                except requests.RequestException as exc:
                    attempt += 1
                    total_failures += 1
                    wait_seconds = max(throttle_seconds, 0.5) * (1 + attempt / 2)
                    if total_failures > max_total_failures:
                        LOGGER.error(
                            "Exceeded global retry budget (%s failures); aborting",
                            total_failures,
                        )
                        raise
                    if attempt > max_failures:
                        if page == 1 and current_page_size > min_page_size:
                            new_size = max(current_page_size // 2, min_page_size)
                            if new_size < current_page_size:
                                LOGGER.warning(
                                    "Reducing page size from %s to %s and retrying page %s",
                                    current_page_size,
                                    new_size,
                                    page,
                                )
                                current_page_size = new_size
                                if owns_session:
                                    sess.close()
                                    sess = build_session(retries=max_failures if max_failures > 0 else 1)
                                time_module.sleep(wait_seconds)
                                attempt = 0
                                continue
                        LOGGER.error("Page %s request failed after %s attempts", page, attempt)
                        raise
                    LOGGER.warning(
                        "Page %s request failed (%s/%s, total=%s/%s): %s; retrying in %.1fs",
                        page,
                        attempt,
                        max_failures,
                        total_failures,
                        max_total_failures,
                        exc,
                        wait_seconds,
                    )
                    time_module.sleep(wait_seconds)
                    continue
                break

            payload = response.json() or {}
            data = payload.get("data") or {}
            records = data.get("diff") or []
            if not records:
                LOGGER.debug("No records returned for page %s; stopping", page)
                break

            frame = pd.DataFrame(records)
            frames.append(frame)

            page += 1
            if throttle_seconds > 0:
                time_module.sleep(throttle_seconds)
    finally:
        if owns_session:
            sess.close()

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        return df
    df["f12"] = df["f12"].astype(str).str.zfill(6)
    numeric_cols = ["f2", "f3", "f5", "f6", "f20"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return df


def merge_with_board(df: pd.DataFrame, board_lookup: pd.DataFrame) -> pd.DataFrame:
    """Join price data with the board hierarchy metadata."""
    merged = df.merge(board_lookup, how="left", left_on="f12", right_index=True, suffixes=("", "_board"))
    missing = merged["full_code"].isna()
    if missing.any():
        missing_codes = merged.loc[missing, "f12"].unique()
        LOGGER.warning(
            "%s codes missing from board hierarchy (sample=%s)",
            len(missing_codes),
            list(missing_codes[:10]),
        )
    market_codes = merged["f13"].astype(str)
    suffix = market_codes.map(MARKET_SUFFIXES).fillna(market_codes)
    merged["stock_id"] = merged["full_code"].fillna(merged["f12"] + "." + suffix)
    merged["stock_name_cn"] = merged["f14"].fillna(merged["tree_name"])
    merged["change_pct"] = merged["f3"]
    merged["price"] = merged["f2"]
    merged["volume"] = merged["f5"]
    merged["turnover"] = merged["f6"]
    merged["market_cap"] = merged["f20"].mul(1e4)
    merged = merged[[
        "stock_id",
        "stock_name_cn",
        "change_pct",
        "price",
        "volume",
        "turnover",
        "market_cap",
        "board",
        "sub_board",
    ]].copy()
    if merged.empty:
        LOGGER.warning("Merged dataframe is empty; skipping save.")
    return merged


def ensure_trading_day(today: date, force: bool) -> None:
    if force:
        return
    if not is_workday(today):
        LOGGER.info("%s is not a Chinese trading day; skipping run (use --force to override).", today)
        raise SystemExit(0)


def resolve_snapshot_label(ts: datetime) -> str:
    """Return the scheduled label (HHMM) closest to the capture time."""
    tolerance_minutes = 5
    day = ts.date()
    candidates = []
    for hhmm in SNAPSHOT_TIMES:
        hour, minute = map(int, hhmm.split(":"))
        slot = datetime.combine(day, time(hour=hour, minute=minute), tzinfo=ts.tzinfo)
        candidates.append((hhmm.replace(":", ""), slot))

    window = ts + pd.Timedelta(minutes=tolerance_minutes)
    eligible = [item for item in candidates if item[1] <= window]
    if eligible:
        label, slot_time = eligible[-1]
        delta = abs((ts - slot_time).total_seconds())
        if delta > 60:
            LOGGER.info(
                "Using snapshot label %s for timestamp %s (difference %.1f seconds)",
                label,
                ts,
                delta,
            )
        return label

    LOGGER.info("Timestamp %s does not align with predefined slots; using actual time", ts)
    return ts.strftime("%H%M")


def save_snapshot(df: pd.DataFrame, output_dir: Path, ts: datetime) -> Path:
    date_part = ts.strftime("%Y%m%d")
    time_part = resolve_snapshot_label(ts)
    target_dir = output_dir / date_part
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{time_part}.parquet"
    df.to_parquet(path, index=False)
    return path


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--board-tree",
        default="board_tree.json",
        type=Path,
        help="Path to board_tree.json (default: board_tree.json in repo root).",
    )
    parser.add_argument(
        "--output-dir",
        default=Path("data"),
        type=Path,
        help="Directory where parquet snapshots will be stored (default: ./data).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Fetch even if today is not recognised as a trading day.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Configure logging verbosity (default: INFO).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=500,
        help="Number of rows per API page (default: 500).",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.3,
        help="Delay in seconds between API page requests (default: 0.3).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts per page when the API call fails (default: 3).",
    )
    parser.add_argument(
        "--min-page-size",
        type=int,
        default=100,
        help="Smallest page size to fall back to when repeated failures happen (default: 100).",
    )
    parser.add_argument(
        "--max-total-retries",
        type=int,
        default=10,
        help="Abort after this many total request failures across pages (default: 10).",
    )
    parser.add_argument(
        "--graceful",
        action="store_true",
        help="If set, skip saving data when the API keeps failing instead of raising an error.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    today = datetime.now(tz=CN_TZ).date()
    ensure_trading_day(today, args.force)

    LOGGER.info("Loading board hierarchy from %s", args.board_tree)
    board_lookup = load_board_lookup(args.board_tree)

    LOGGER.info("Fetching market snapshot from Eastmoney API")
    try:
        raw_df = fetch_market_snapshot(
            page_size=args.page_size,
            throttle_seconds=args.throttle,
            max_failures=args.max_retries,
            min_page_size=args.min_page_size,
            max_total_failures=args.max_total_retries,
        )
    except Exception as exc:
        if args.graceful:
            LOGGER.error("Market snapshot failed after retries: %s", exc)
            return
        raise

    LOGGER.info("Merging %s snapshot rows with board metadata", len(raw_df))
    merged_df = merge_with_board(raw_df, board_lookup)

    timestamp = datetime.now(tz=CN_TZ)
    output_path = save_snapshot(merged_df, args.output_dir, timestamp)

    LOGGER.info("Saved %s rows to %s", len(merged_df), output_path)


if __name__ == "__main__":
    main()
