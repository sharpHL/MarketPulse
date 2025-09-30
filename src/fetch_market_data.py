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
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests
from chinese_calendar import is_workday
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)

API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
API_FIELDS = "f2,f3,f12,f13,f14,f20"
API_FS = "m:0+t:6,m:0+t:13,m:0+t:80,m:0+t:81,m:0+t:82,m:1+t:2,m:1+t:23"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/"
}
CN_TZ = ZoneInfo("Asia/Shanghai")


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

    def traverse(node: dict, level: int, top: Optional[str], sub: Optional[str]) -> None:
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
                traverse(child, level + 1, next_top, next_sub)
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
        traverse(root, level=0, top=None, sub=None)


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
    return board_df.set_index("base_code", drop=False)


def fetch_market_snapshot(session: Optional[requests.Session] = None) -> pd.DataFrame:
    """Fetch the real-time market snapshot for all A-shares."""
    params = {
        "pn": "1",
        "pz": "5000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": API_FS,
        "fields": API_FIELDS,
    }

    sess = session or requests.Session()
    response = sess.get(API_URL, params=params, headers=HEADERS, timeout=15)
    response.raise_for_status()
    payload = response.json()

    try:
        records = payload["data"]["diff"]
    except (KeyError, TypeError) as exc:
        raise ValueError("Unexpected response payload") from exc

    df = pd.DataFrame(records)
    if df.empty:
        raise ValueError("Empty dataset returned from market API")

    df["f12"] = df["f12"].astype(str).str.zfill(6)
    numeric_cols = ["f2", "f3", "f20"]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    return df


def merge_with_board(df: pd.DataFrame, board_lookup: pd.DataFrame) -> pd.DataFrame:
    """Join price data with the board hierarchy metadata."""
    merged = df.merge(board_lookup, how="left", left_on="f12", right_on="base_code", suffixes=("", "_board"))
    missing = merged["full_code"].isna()
    if missing.any():
        missing_codes = merged.loc[missing, "f12"].unique()
        LOGGER.warning("%s codes missing from board hierarchy (sample=%s)", len(missing_codes), list(missing_codes[:10]))
    merged = merged[~missing].copy()
    merged["stock_id"] = merged["full_code"]
    merged["stock_name_cn"] = merged["f14"].fillna(merged["tree_name"])
    merged["change_pct"] = merged["f3"]
    merged["price"] = merged["f2"]
    merged["market_cap"] = merged["f20"].mul(1e4)
    merged = merged[[
        "stock_id",
        "stock_name_cn",
        "change_pct",
        "price",
        "market_cap",
        "board",
        "sub_board",
    ]].copy()
    return merged


def ensure_trading_day(today: date, force: bool) -> None:
    if force:
        return
    if not is_workday(today):
        LOGGER.info("%s is not a Chinese trading day; skipping run (use --force to override).", today)
        raise SystemExit(0)


def save_snapshot(df: pd.DataFrame, output_dir: Path, ts: datetime) -> Path:
    date_part = ts.strftime("%Y%m%d")
    time_part = ts.strftime("%H%M")
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
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    today = datetime.now(tz=CN_TZ).date()
    ensure_trading_day(today, args.force)

    LOGGER.info("Loading board hierarchy from %s", args.board_tree)
    board_lookup = load_board_lookup(args.board_tree)

    LOGGER.info("Fetching market snapshot from Eastmoney API")
    raw_df = fetch_market_snapshot()

    LOGGER.info("Merging %s snapshot rows with board metadata", len(raw_df))
    merged_df = merge_with_board(raw_df, board_lookup)

    timestamp = datetime.now(tz=CN_TZ)
    output_path = save_snapshot(merged_df, args.output_dir, timestamp)

    LOGGER.info("Saved %s rows to %s", len(merged_df), output_path)


if __name__ == "__main__":
    main()
